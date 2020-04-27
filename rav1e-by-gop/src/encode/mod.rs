pub mod stats;

pub use self::stats::*;

use super::VideoDetails;
use crate::muxer::create_muxer;
use crate::{build_encoder_config, SegmentData};
use anyhow::Result;
use crossbeam_channel::{Receiver, Sender};
use rav1e::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use systemstat::data::ByteSize;
use threadpool::ThreadPool;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct EncodeOptions {
    pub speed: usize,
    pub qp: usize,
}

pub fn encode_segment<T: Pixel>(
    opts: &EncodeOptions,
    video_info: VideoDetails,
    data: SegmentData<T>,
    thread_pool: &mut ThreadPool,
    progress_sender: ProgressSender,
    segment_output_file: PathBuf,
) -> Result<()> {
    let progress = ProgressInfo::new(
        Rational {
            num: video_info.time_base.den,
            den: video_info.time_base.num,
        },
        data.frames.len(),
        {
            let mut kf = BTreeSet::new();
            kf.insert(data.start_frameno);
            kf
        },
        data.segment_no + 1,
        data.next_analysis_frame,
    );
    let _ = progress_sender.send(ProgressStatus::Encoding(Box::new(progress.clone())));

    let frames = data.frames.into_iter().map(Arc::new).collect::<Vec<_>>();
    let cfg = build_encoder_config(opts.speed, opts.qp, video_info);

    thread_pool.execute(move || {
        let source = Source {
            frames,
            sent_count: 0,
        };
        do_encode(cfg, source, segment_output_file, progress, progress_sender)
            .expect("Failed encoding segment");
    });
    Ok(())
}

fn do_encode<T: Pixel>(
    cfg: Config,
    mut source: Source<T>,
    segment_output_file: PathBuf,
    mut progress: ProgressInfo,
    progress_sender: ProgressSender,
) -> Result<ProgressInfo> {
    let mut ctx: Context<T> = cfg.new_context()?;
    let _ = progress_sender.send(ProgressStatus::Encoding(Box::new(progress.clone())));

    let mut output = create_muxer(&segment_output_file).expect("Failed to create segment output");

    loop {
        match process_frame(&mut ctx, &mut source)? {
            ProcessFrameResult::Packet(packet) => {
                output.write_frame(
                    packet.input_frameno as u64,
                    packet.data.as_ref(),
                    packet.frame_type,
                );
                progress.add_packet(*packet);
                let _ = progress_sender.send(ProgressStatus::Encoding(Box::new(progress.clone())));
            }
            ProcessFrameResult::NoPacket => {
                // Next iteration
            }
            ProcessFrameResult::EndOfSegment => {
                output.flush().unwrap();
                break;
            }
        };
    }

    Ok(progress)
}

pub struct Source<T: Pixel> {
    pub sent_count: usize,
    pub frames: Vec<Arc<Frame<T>>>,
}

impl<T: Pixel> Source<T> {
    pub fn read_frame(&mut self, ctx: &mut Context<T>) {
        if self.sent_count == self.frames.len() {
            ctx.flush();
            return;
        }

        let _ = ctx.send_frame(Some(self.frames[self.sent_count].clone()));
        self.sent_count += 1;
    }
}

pub enum ProcessFrameResult<T: Pixel> {
    Packet(Box<Packet<T>>),
    NoPacket,
    EndOfSegment,
}

pub fn process_frame<T: Pixel>(
    ctx: &mut Context<T>,
    source: &mut Source<T>,
) -> Result<ProcessFrameResult<T>> {
    let pkt_wrapped = ctx.receive_packet();
    match pkt_wrapped {
        Ok(pkt) => {
            return Ok(ProcessFrameResult::Packet(Box::new(pkt)));
        }
        Err(EncoderStatus::NeedMoreData) => {
            source.read_frame(ctx);
        }
        Err(EncoderStatus::EnoughData) => {
            unreachable!();
        }
        Err(EncoderStatus::LimitReached) => {
            return Ok(ProcessFrameResult::EndOfSegment);
        }
        e @ Err(EncoderStatus::Failure) => {
            e?;
        }
        Err(EncoderStatus::NotReady) => {
            unreachable!();
        }
        Err(EncoderStatus::Encoded) => {}
    }
    Ok(ProcessFrameResult::NoPacket)
}

pub type ProgressSender = Sender<ProgressStatus>;
pub type ProgressReceiver = Receiver<ProgressStatus>;
pub type ProgressChannel = (ProgressSender, ProgressReceiver);

pub enum ProgressStatus {
    Idle,
    Loading,
    Compressing(usize),
    Sending(ByteSize),
    Encoding(Box<ProgressInfo>),
}
