pub mod stats;

pub use self::stats::*;

use super::VideoDetails;
use crate::muxer::create_muxer;
use crate::{build_config, decompress_frame, Output, SegmentData, SegmentFrameData};
use anyhow::Result;
use crossbeam_channel::{Receiver, Sender};
use rav1e::prelude::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::Arc;
use systemstat::data::ByteSize;
use threadpool::ThreadPool;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct EncodeOptions {
    pub speed: usize,
    pub qp: usize,
    pub max_bitrate: Option<i32>,
    pub tiles: usize,
}

pub fn encode_segment(
    opts: EncodeOptions,
    video_info: VideoDetails,
    data: SegmentData,
    thread_pool: &mut ThreadPool,
    rayon_pool: Arc<rayon::ThreadPool>,
    progress_sender: ProgressSender,
    segment_output_file: Output,
) -> Result<()> {
    let progress = ProgressInfo::new(
        Rational {
            num: video_info.time_base.den,
            den: video_info.time_base.num,
        },
        match data.frame_data {
            SegmentFrameData::Y4MFile { frame_count, .. } => frame_count,
            SegmentFrameData::CompressedFrames(ref frames) => frames.len(),
        },
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
            frame_data: match data.frame_data {
                SegmentFrameData::Y4MFile { path, frame_count } => SourceFrameData::Y4MFile {
                    frame_count,
                    video_info,
                    input: {
                        let file = File::open(&path).unwrap();
                        BufReader::new(file)
                    },
                    path,
                },
                SegmentFrameData::CompressedFrames(frames) => {
                    SourceFrameData::CompressedFrames(frames)
                }
            },
            sent_count: 0,
        };
        if video_info.bit_depth > 8 {
            do_encode::<u16>(
                rayon_pool,
                opts,
                video_info,
                source,
                &segment_output_file,
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
                &segment_output_file,
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
    segment_output_file: &Output,
    mut progress: ProgressInfo,
    progress_sender: ProgressSender,
) -> Result<ProgressInfo> {
    let cfg = build_config(
        opts.speed,
        opts.qp,
        opts.max_bitrate,
        opts.tiles,
        video_info,
        pool,
    );

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
            ProcessFrameResult::NoPacket(_) => {
                // Next iteration
            }
            ProcessFrameResult::EndOfSegment => {
                output.flush().unwrap();
                break;
            }
        };
    }
    if let SourceFrameData::Y4MFile { path, .. } = source.frame_data {
        fs::remove_file(&path).unwrap();
    }

    Ok(progress)
}

pub enum SourceFrameData {
    CompressedFrames(Vec<Vec<u8>>),
    Y4MFile {
        path: PathBuf,
        input: BufReader<File>,
        frame_count: usize,
        video_info: VideoDetails,
    },
}

pub struct Source {
    pub sent_count: usize,
    pub frame_data: SourceFrameData,
}

impl Source {
    fn read_frame<T: Pixel + DeserializeOwned>(&mut self, ctx: &mut Context<T>) {
        if self.sent_count == self.frame_count() {
            ctx.flush();
            return;
        }

        match self.frame_data {
            SourceFrameData::CompressedFrames(ref mut frames) => {
                let _ = ctx.send_frame(Some(Arc::new(decompress_frame(&frames[self.sent_count]))));
                // Deallocate the compressed frame from memory, we no longer need it
                frames[self.sent_count] = Vec::new();
            }
            SourceFrameData::Y4MFile { ref mut input, .. } => {
                let _ = ctx.send_frame(Some(Arc::new(bincode::deserialize_from(input).unwrap())));
            }
        };
        self.sent_count += 1;
    }

    pub fn frame_count(&self) -> usize {
        match self.frame_data {
            SourceFrameData::CompressedFrames(ref frames) => frames.len(),
            SourceFrameData::Y4MFile { frame_count, .. } => frame_count,
        }
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
