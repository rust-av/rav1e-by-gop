pub mod stats;

pub use self::stats::*;

use super::VideoDetails;
use crate::muxer::{create_muxer, Muxer};
use crate::SegmentData;
use anyhow::Result;
use crossbeam_channel::{Receiver, Sender};
use rav1e::prelude::*;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use threadpool::ThreadPool;

#[derive(Debug, Clone, Copy)]
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
    let _ = progress_sender.send(Some(progress.clone()));

    let frames = data.frames.into_iter().map(Arc::new).collect::<Vec<_>>();

    let mut enc_config = EncoderConfig::with_speed_preset(opts.speed);
    enc_config.width = video_info.width;
    enc_config.height = video_info.height;
    enc_config.bit_depth = video_info.bit_depth;
    enc_config.chroma_sampling = video_info.chroma_sampling;
    enc_config.chroma_sample_position = video_info.chroma_sample_position;
    enc_config.time_base = video_info.time_base;
    enc_config.quantizer = opts.qp;
    enc_config.tiles = 1;
    enc_config.min_key_frame_interval = 0;
    enc_config.max_key_frame_interval = u64::max_value();
    enc_config.speed_settings.no_scene_detection = true;
    let cfg = Config::new()
        .with_encoder_config(enc_config)
        .with_threads(1);

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
    let _ = progress_sender.send(Some(progress.clone()));

    let mut output = create_muxer(&segment_output_file).expect("Failed to create segment output");

    loop {
        match process_frame(&mut ctx, &mut source, &mut *output)? {
            ProcessFrameResult::Packet(packet) => {
                progress.add_packet(*packet);
                let _ = progress_sender.send(Some(progress.clone()));
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

struct Source<T: Pixel> {
    sent_count: usize,
    frames: Vec<Arc<Frame<T>>>,
}

impl<T: Pixel> Source<T> {
    fn read_frame(&mut self, ctx: &mut Context<T>) {
        if self.sent_count == self.frames.len() {
            ctx.flush();
            return;
        }

        let _ = ctx.send_frame(Some(self.frames[self.sent_count].clone()));
        self.sent_count += 1;
    }
}

enum ProcessFrameResult<T: Pixel> {
    Packet(Box<Packet<T>>),
    NoPacket,
    EndOfSegment,
}

fn process_frame<T: Pixel>(
    ctx: &mut Context<T>,
    source: &mut Source<T>,
    output: &mut dyn Muxer,
) -> Result<ProcessFrameResult<T>> {
    let pkt_wrapped = ctx.receive_packet();
    match pkt_wrapped {
        Ok(pkt) => {
            output.write_frame(pkt.input_frameno as u64, pkt.data.as_ref(), pkt.frame_type);
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

pub type ProgressSender = Sender<Option<ProgressInfo>>;
pub type ProgressReceiver = Receiver<Option<ProgressInfo>>;
pub type ProgressChannel = (ProgressSender, ProgressReceiver);
