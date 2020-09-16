pub mod stats;

pub use self::stats::*;

use super::VideoDetails;
use crate::{build_encoder_config, create_memory_muxer, decompress_frame, Muxer, SegmentData};
use anyhow::Result;
use crossbeam_channel::{Receiver, Sender};
use rav1e::prelude::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use systemstat::data::ByteSize;
use threadpool::ThreadPool;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct EncodeOptions {
    pub speed: usize,
    pub qp: usize,
}

pub fn encode_segment(
    opts: EncodeOptions,
    video_info: VideoDetails,
    data: SegmentData,
    thread_pool: &mut ThreadPool,
    rayon_pool: Arc<rayon::ThreadPool>,
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
        None,
    );
    let _ = progress_sender.send(ProgressStatus::Encoding(Box::new(progress.clone())));

    thread_pool.execute(move || {
        let source = Source {
            compressed_frames: data.compressed_frames.into_iter().map(Rc::new).collect(),
            sent_count: 0,
        };
        if video_info.bit_depth > 8 {
            do_encode::<u16>(
                rayon_pool,
                opts,
                video_info,
                source,
                segment_output_file,
                progress,
                progress_sender,
            )
            .expect("Failed encoding segment");
        } else {
            do_encode::<u8>(
                rayon_pool,
                opts,
                video_info,
                source,
                segment_output_file,
                progress,
                progress_sender,
            )
            .expect("Failed encoding segment");
        }
    });
    Ok(())
}

fn do_encode<T: Pixel + DeserializeOwned>(
    pool: Arc<rayon::ThreadPool>,
    opts: EncodeOptions,
    video_info: VideoDetails,
    mut source: Source,
    segment_output_file: PathBuf,
    mut progress: ProgressInfo,
    progress_sender: ProgressSender,
) -> Result<ProgressInfo> {
    let cfg = build_encoder_config(opts.speed, opts.qp, video_info, pool);

    let mut ctx: Context<T> = cfg.new_context()?;
    let _ = progress_sender.send(ProgressStatus::Encoding(Box::new(progress.clone())));

    let mut output = create_memory_muxer();
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
            ProcessFrameResult::NoPacket(_) => {
                // Next iteration
            }
            ProcessFrameResult::EndOfSegment => {
                let mut output_file =
                    File::create(&segment_output_file).expect("Failed to create segment output");
                output_file.write_all(&output.buffer).unwrap();
                break;
            }
        };
    }

    Ok(progress)
}

#[derive(Clone)]
pub struct Source {
    pub sent_count: usize,
    pub compressed_frames: Vec<Rc<Vec<u8>>>,
}

impl Source {
    fn read_frame<T: Pixel + DeserializeOwned>(&mut self, ctx: &mut Context<T>) {
        if self.sent_count == self.compressed_frames.len() {
            ctx.flush();
            return;
        }

        let _ = ctx.send_frame(Some(Arc::new(decompress_frame(
            &self.compressed_frames[self.sent_count],
        ))));
        // Deallocate the compressed frame from memory, we no longer need it
        self.compressed_frames[self.sent_count] = Rc::new(Vec::new());
        self.sent_count += 1;
    }

    pub fn frame_count(&self) -> usize {
        self.compressed_frames.len()
    }
}

pub enum ProcessFrameResult<T: Pixel> {
    Packet(Box<Packet<T>>),
    NoPacket(bool),
    EndOfSegment,
}

pub fn process_frame<T: Pixel + DeserializeOwned>(
    ctx: &mut Context<T>,
    source: &mut Source,
) -> Result<ProcessFrameResult<T>> {
    let pkt_wrapped = ctx.receive_packet();
    match pkt_wrapped {
        Ok(pkt) => Ok(ProcessFrameResult::Packet(Box::new(pkt))),
        Err(EncoderStatus::NeedMoreData) => {
            source.read_frame(ctx);
            Ok(ProcessFrameResult::NoPacket(false))
        }
        Err(EncoderStatus::EnoughData) => {
            unreachable!();
        }
        Err(EncoderStatus::LimitReached) => Ok(ProcessFrameResult::EndOfSegment),
        e @ Err(EncoderStatus::Failure) => {
            e?;
            unreachable!();
        }
        Err(EncoderStatus::NotReady) => {
            unreachable!();
        }
        Err(EncoderStatus::Encoded) => Ok(ProcessFrameResult::NoPacket(true)),
    }
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
