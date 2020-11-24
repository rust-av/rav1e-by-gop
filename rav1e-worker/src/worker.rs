use crate::ENCODER_QUEUE;
use chrono::Utc;
use log::{error, info};
use rav1e::prelude::*;
use rav1e_by_gop::*;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::BTreeSet;
use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::delay_for;
use uuid::Uuid;
use v_frame::pixel::Pixel;

pub async fn start_workers(worker_threads: usize) {
    info!("Starting {} workers", worker_threads);
    let rayon_pool = Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(worker_threads)
            .build()
            .unwrap(),
    );

    // This thread watches the slot request queue
    // and allocates slots when they are available.
    tokio::spawn(async move {
        loop {
            delay_for(Duration::from_secs(3)).await;

            let reader = ENCODER_QUEUE.read().await;
            let mut in_progress_items = 0;
            for item in reader.values() {
                match item.read().await.state {
                    EncodeState::Enqueued => (),
                    _ => {
                        in_progress_items += 1;
                    }
                }
            }
            if in_progress_items >= worker_threads {
                continue;
            }

            for (&request_id, item) in reader.iter() {
                let mut item_writer = item.write().await;
                match item_writer.state {
                    EncodeState::Enqueued if in_progress_items < worker_threads => {
                        info!("A slot is ready for request {}", request_id);
                        item_writer.state = EncodeState::AwaitingData {
                            time_ready: Utc::now(),
                        };
                        in_progress_items += 1;
                    }
                    EncodeState::Ready { ref raw_frames } => {
                        info!("Beginning encode for request {}", request_id);
                        if item_writer.video_info.bit_depth <= 8 {
                            encode_segment::<u8>(
                                request_id,
                                item_writer.video_info,
                                item_writer.options,
                                raw_frames.clone(),
                                rayon_pool.clone(),
                            )
                            .await;
                        } else {
                            encode_segment::<u16>(
                                request_id,
                                item_writer.video_info,
                                item_writer.options,
                                raw_frames.clone(),
                                rayon_pool.clone(),
                            )
                            .await;
                        }
                    }
                    _ => (),
                }
            }
        }
    });
}

pub async fn encode_segment<T: Pixel + Default + Serialize + DeserializeOwned>(
    request_id: Uuid,
    video_info: VideoDetails,
    options: EncodeOptions,
    input: Arc<SegmentFrameData>,
    pool: Arc<rayon::ThreadPool>,
) {
    let queue_reader = ENCODER_QUEUE.read().await;
    let mut item_writer = queue_reader.get(&request_id).unwrap().write().await;
    item_writer.state = EncodeState::InProgress {
        progress: ProgressInfo::new(
            Rational::from_reciprocal(item_writer.video_info.time_base),
            match input.as_ref() {
                SegmentFrameData::CompressedFrames(frames) => frames.len(),
                SegmentFrameData::Y4MFile { frame_count, .. } => *frame_count,
            },
            BTreeSet::new(),
            0,
            0,
            None,
        ),
    };

    let mut source = Source {
        frame_data: match Arc::try_unwrap(input) {
            Ok(SegmentFrameData::CompressedFrames(input)) => {
                SourceFrameData::CompressedFrames(input)
            }
            Ok(SegmentFrameData::Y4MFile {
                frame_count,
                ref path,
            }) => SourceFrameData::Y4MFile {
                frame_count,
                input: {
                    let file = File::open(&path).unwrap();
                    BufReader::new(file)
                },
                path: path.to_path_buf(),
                video_info,
            },
            Err(_) => unreachable!("input should only have one reference at this point"),
        },
        sent_count: 0,
    };
    let cfg = build_encoder_config(
        options.speed,
        options.qp,
        options.max_bitrate,
        video_info,
        pool,
    );

    let mut ctx: Context<T> = match cfg.new_context() {
        Ok(ctx) => ctx,
        Err(e) => {
            error!(
                "Failed to create encode context for request {}: {}",
                request_id, e
            );
            return;
        }
    };
    let mut output = IvfMuxer::<Vec<u8>>::in_memory();
    loop {
        match process_frame(&mut ctx, &mut source) {
            Ok(ProcessFrameResult::Packet(packet)) => {
                output.write_frame(
                    packet.input_frameno as u64,
                    packet.data.as_ref(),
                    packet.frame_type,
                );
                let queue_reader = ENCODER_QUEUE.read().await;
                let mut item_writer = queue_reader.get(&request_id).unwrap().write().await;
                if let EncodeState::InProgress { ref mut progress } = item_writer.state {
                    output.write_frame(
                        packet.input_frameno as u64,
                        packet.data.as_ref(),
                        packet.frame_type,
                    );
                    progress.add_packet(*packet);
                }
            }
            Ok(ProcessFrameResult::NoPacket(_)) => {
                // Next iteration
            }
            Ok(ProcessFrameResult::EndOfSegment) => {
                break;
            }
            Err(e) => {
                error!("Encoding error for request {}: {}", request_id, e);
                if let SourceFrameData::Y4MFile { path, .. } = source.frame_data {
                    let _ = fs::remove_file(path);
                };
                return;
            }
        };
    }

    let queue_reader = ENCODER_QUEUE.read().await;
    let mut item_writer = queue_reader.get(&request_id).unwrap().write().await;
    item_writer.state = if let EncodeState::InProgress { ref progress } = item_writer.state {
        EncodeState::EncodingDone {
            progress: progress.clone(),
            encoded_data: output.output,
            time_finished: Utc::now(),
        }
    } else {
        unreachable!()
    };
    if let SourceFrameData::Y4MFile { path, .. } = source.frame_data {
        let _ = fs::remove_file(path);
    };
    info!("Segment {} finished", request_id);
}
