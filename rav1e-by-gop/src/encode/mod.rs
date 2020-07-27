pub mod stats;

pub use self::stats::*;

use super::VideoDetails;
use crate::muxer::create_muxer;
use crate::{build_encoder_config, decompress_frame, SegmentData};
use anyhow::Result;
use crossbeam_channel::{Receiver, Sender};
use rav1e::prelude::*;
use serde::de::DeserializeOwned;
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
    pub max_bitrate: Option<i32>,
}

pub fn encode_segment(
    opts: &EncodeOptions,
    video_info: VideoDetails,
    data: SegmentData,
    thread_pool: &mut ThreadPool,
    progress_sender: ProgressSender,
    segment_output_file: PathBuf,
) -> Result<()> {
    let progress = ProgressInfo::new(
        Rational {
            num: video_info.time_base.den,
            den: video_info.time_base.num,
        },
        data.compressed_frames.len(),
        {
            let mut kf = BTreeSet::new();
            kf.insert(data.start_frameno);
            kf
        },
        data.segment_no + 1,
        data.next_analysis_frame,
    );
    let _ = progress_sender.send(ProgressStatus::Encoding(Box::new(progress.clone())));

    let cfg = build_encoder_config(
        opts.speed,
        opts.qp,
        opts.max_bitrate,
        video_info,
        data.compressed_frames.len(),
    );

    thread_pool.execute(move || {
        let source = Source {
            compressed_frames: data.compressed_frames,
            sent_count: 0,
        };
        if video_info.bit_depth > 8 {
            do_encode::<u16>(cfg, source, segment_output_file, progress, progress_sender)
                .expect("Failed encoding segment");
        } else {
            do_encode::<u8>(cfg, source, segment_output_file, progress, progress_sender)
                .expect("Failed encoding segment");
        }
    });
    Ok(())
}

fn do_encode<T: Pixel + DeserializeOwned>(
    cfg: Config,
    mut source: Source,
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

pub struct Source {
    pub sent_count: usize,
    pub compressed_frames: Vec<Vec<u8>>,
}

impl Source {
    pub fn read_frame<T: Pixel + DeserializeOwned>(&mut self, ctx: &mut Context<T>) {
        if self.sent_count == self.compressed_frames.len() {
            ctx.flush();
            return;
        }

        let _ = ctx.send_frame(Some(Arc::new(decompress_frame(
            &self.compressed_frames[self.sent_count],
        ))));
        // Deallocate the compressed frame from memory, we no longer need it
        self.compressed_frames[self.sent_count] = Vec::new();
        self.sent_count += 1;
    }
}

pub enum ProcessFrameResult<T: Pixel> {
    Packet(Box<Packet<T>>),
    NoPacket,
    EndOfSegment,
}

pub fn process_frame<T: Pixel + DeserializeOwned>(
    ctx: &mut Context<T>,
    source: &mut Source,
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
