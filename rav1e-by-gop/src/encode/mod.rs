pub mod stats;

pub use self::stats::*;

use super::VideoDetails;
use crate::muxer::create_muxer;
use crate::{build_encoder_config, build_first_pass_encoder_config, decompress_frame, SegmentData};
use anyhow::Result;
use crossbeam_channel::{Receiver, Sender};
use rav1e::prelude::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::io::{Cursor, Read};
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
    opts: EncodeOptions,
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

    thread_pool.execute(move || {
        let source = Source {
            compressed_frames: data.compressed_frames.into_iter().map(Arc::new).collect(),
            sent_count: 0,
        };
        if video_info.bit_depth > 8 {
            do_encode::<u16>(
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
    opts: EncodeOptions,
    video_info: VideoDetails,
    mut source: Source,
    segment_output_file: PathBuf,
    mut progress: ProgressInfo,
    progress_sender: ProgressSender,
) -> Result<ProgressInfo> {
    let do_two_pass = opts.max_bitrate.is_some();

    let mut first_pass_data = Vec::new();
    if do_two_pass {
        let cfg = build_first_pass_encoder_config(
            opts.speed,
            opts.qp,
            opts.max_bitrate,
            video_info,
            source.frame_count(),
        );
        let mut source = source.clone();
        let mut ctx: Context<T> = cfg.new_context()?;
        let mut progress_counter = 0;
        progress_sender
            .send(ProgressStatus::FirstPass(FirstPassProgress {
                frames_done: progress_counter,
                frames_total: source.frame_count(),
                segment_idx: progress.segment_idx,
            }))
            .unwrap();
        loop {
            let result = process_frame(&mut ctx, &mut source).unwrap();
            match ctx.rc_receive_pass_data() {
                RcData::Frame(outbuf) => {
                    let len = outbuf.len() as u64;
                    first_pass_data.extend_from_slice(&len.to_be_bytes());
                    first_pass_data.extend_from_slice(&outbuf);
                }
                RcData::Summary(outbuf) => {
                    // The last packet of rate control data we get is the summary data.
                    // Let's put it at the start of the file.
                    let mut tmp_data = Vec::new();
                    let len = outbuf.len() as u64;
                    tmp_data.extend_from_slice(&len.to_be_bytes());
                    tmp_data.extend_from_slice(&outbuf);
                    tmp_data.extend_from_slice(&first_pass_data);
                    first_pass_data = tmp_data;
                }
            }
            match result {
                ProcessFrameResult::Packet(_) => {
                    progress_counter += 1;
                    progress_sender
                        .send(ProgressStatus::FirstPass(FirstPassProgress {
                            frames_done: progress_counter,
                            frames_total: source.frame_count(),
                            segment_idx: progress.segment_idx,
                        }))
                        .unwrap();
                }
                ProcessFrameResult::EndOfSegment => {
                    break;
                }
                _ => {}
            }
        }
    }

    let mut pass_data = Cursor::new(first_pass_data);
    let cfg = build_encoder_config(
        opts.speed,
        opts.qp,
        opts.max_bitrate,
        video_info,
        source.frame_count(),
        if do_two_pass {
            Some(&mut pass_data)
        } else {
            None
        },
    );

    let mut ctx: Context<T> = cfg.new_context()?;
    let _ = progress_sender.send(ProgressStatus::Encoding(Box::new(progress.clone())));

    let mut output = create_muxer(&segment_output_file).expect("Failed to create segment output");
    loop {
        if do_two_pass {
            while ctx.rc_second_pass_data_required() > 0 {
                let mut buflen = [0u8; 8];
                pass_data.read_exact(&mut buflen)?;

                let mut data = vec![0u8; u64::from_be_bytes(buflen) as usize];
                pass_data.read_exact(&mut data)?;

                ctx.rc_send_pass_data(&data)?;
            }
        }
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

#[derive(Clone)]
pub struct Source {
    pub sent_count: usize,
    pub compressed_frames: Vec<Arc<Vec<u8>>>,
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
        self.compressed_frames[self.sent_count] = Arc::new(Vec::new());
        self.sent_count += 1;
    }

    pub fn frame_count(&self) -> usize {
        self.compressed_frames.len()
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
    FirstPass(FirstPassProgress),
    Encoding(Box<ProgressInfo>),
}

#[derive(Debug, Clone, Copy)]
pub struct FirstPassProgress {
    pub frames_done: usize,
    pub frames_total: usize,
    pub segment_idx: usize,
}
