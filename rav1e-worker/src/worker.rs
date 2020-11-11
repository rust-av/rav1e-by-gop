use crate::channels::SlotRequestReceiver;
use crate::ConnectedSocket;
use byteorder::{LittleEndian, WriteBytesExt};
use crossbeam_utils::thread::Scope;
use log::{debug, error, info, warn};
use parking_lot::Mutex;
use rav1e::prelude::*;
use rav1e_by_gop::*;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::VecDeque;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use std::{cmp, fs};
use threadpool::ThreadPool;
use tungstenite::Message;
use uuid::Uuid;
use v_frame::pixel::Pixel;

#[derive(Debug)]
pub struct SlotQueueItem {
    pub connection: ConnectedSocket,
    pub message: SlotRequestMessage,
}

pub fn start_workers(
    worker_threads: usize,
    scope: &Scope,
    slot_request_receiver: SlotRequestReceiver,
    temp_dir: Option<PathBuf>,
) {
    info!("Starting {} workers", worker_threads);
    let thread_pool = ThreadPool::new(worker_threads);
    let slot_request_queue: Arc<Mutex<VecDeque<SlotQueueItem>>> =
        Arc::new(Mutex::new(VecDeque::new()));
    let rayon_pool = Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(worker_threads)
            .build()
            .unwrap(),
    );

    // This thread watches the slot request receiver
    // for new slot requests
    let slot_request_queue_handle = slot_request_queue.clone();
    scope.spawn(move |_| {
        while let Ok(message) = slot_request_receiver.recv() {
            slot_request_queue_handle.lock().push_back(message);
        }
    });

    // This thread watches the slot request queue
    // and allocates slots when they are available.
    let slot_request_queue_handle = slot_request_queue;
    scope.spawn(move |_| loop {
        sleep(Duration::from_secs(1));

        let active_workers = thread_pool.active_count();
        let total_workers = thread_pool.max_count();
        if active_workers >= total_workers {
            continue;
        }

        let mut queue = slot_request_queue_handle.lock();
        let total_items = queue.len();
        let ready_items = queue
            .drain(0..cmp::min(total_items, total_workers - active_workers))
            .collect::<Vec<_>>();
        debug!(
            "Slot queue: {} total items, {} slots available",
            total_items,
            ready_items.len()
        );
        for queue_item in ready_items {
            info!(
                "Notifying client {} that a slot is ready",
                queue_item.connection.id
            );

            let slot_request = queue_item.message;
            let mut connection = queue_item.connection;
            let _ = connection.socket.write_message(Message::Binary(
                if slot_request.video_info.bit_depth <= 8 {
                    rmp_serde::to_vec(&EncoderMessage::SlotAllocated::<u8>(connection.id)).unwrap()
                } else {
                    rmp_serde::to_vec(&EncoderMessage::SlotAllocated::<u16>(connection.id)).unwrap()
                },
            ));

            let temp_dir = temp_dir.clone();
            let rayon_handle = rayon_pool.clone();
            thread_pool.execute(move || {
                if slot_request.video_info.bit_depth <= 8 {
                    wait_for_remote_data::<u8>(connection, slot_request, rayon_handle, temp_dir);
                } else {
                    wait_for_remote_data::<u16>(connection, slot_request, rayon_handle, temp_dir);
                }
            });
        }
    });
}

pub fn encode_segment<T: Pixel + Default + Serialize + DeserializeOwned>(
    mut connection: ConnectedSocket,
    encode_request: SlotRequestMessage,
    input: SegmentFrameData,
    pool: Arc<rayon::ThreadPool>,
) {
    let connection_id = connection.id;
    let mut source = Source {
        frame_data: match input {
            SegmentFrameData::CompressedFrames(input) => {
                SourceFrameData::CompressedFrames(input.compressed_frames)
            }
            SegmentFrameData::Y4MFile {
                frame_count,
                ref path,
                video_info,
                ..
            } => SourceFrameData::Y4MFile {
                frame_count,
                input: {
                    let file = File::open(&path).unwrap();
                    BufReader::new(file)
                },
                path: path.to_path_buf(),
                video_info,
            },
        },
        sent_count: 0,
    };
    let cfg = build_encoder_config(
        encode_request.options.speed,
        encode_request.options.qp,
        encode_request.options.max_bitrate,
        encode_request.video_info,
        pool,
    );

    let mut ctx: Context<T> = match cfg.new_context() {
        Ok(ctx) => ctx,
        Err(e) => {
            error!(
                "Failed to create encode context for connection {}: {}",
                connection_id, e
            );
            let _ = connection.socket.write_message(Message::Binary(
                rmp_serde::to_vec(&EncoderMessage::<T>::EncodeFailed(e.to_string())).unwrap(),
            ));
            let _ = connection.socket.close(None);
            return;
        }
    };
    loop {
        match process_frame(&mut ctx, &mut source) {
            Ok(ProcessFrameResult::Packet(packet)) => {
                debug!(
                    "Sending frame {} to {}",
                    packet.input_frameno, connection_id
                );
                if let Err(e) = connection.socket.write_message(Message::Binary(
                    rmp_serde::to_vec(&EncoderMessage::<T>::SendEncodedPacket(packet)).unwrap(),
                )) {
                    error!(
                        "Failed to send packet for connection {}: {}",
                        connection_id, e
                    );
                    let _ = connection.socket.write_message(Message::Binary(
                        rmp_serde::to_vec(&EncoderMessage::<T>::EncodeFailed(e.to_string()))
                            .unwrap(),
                    ));
                    let _ = connection.socket.close(None);
                    if let SourceFrameData::Y4MFile { path, .. } = source.frame_data {
                        let _ = fs::remove_file(path);
                    };
                    return;
                };
            }
            Ok(ProcessFrameResult::NoPacket(_)) => {
                // Next iteration
            }
            Ok(ProcessFrameResult::EndOfSegment) => {
                break;
            }
            Err(e) => {
                error!("Encoding error for connection {}: {}", connection_id, e);
                let _ = connection.socket.write_message(Message::Binary(
                    rmp_serde::to_vec(&EncoderMessage::<T>::EncodeFailed(e.to_string())).unwrap(),
                ));
                let _ = connection.socket.close(None);
                if let SourceFrameData::Y4MFile { path, .. } = source.frame_data {
                    let _ = fs::remove_file(path);
                };
                return;
            }
        };
    }

    if let SourceFrameData::Y4MFile { path, .. } = source.frame_data {
        let _ = fs::remove_file(path);
    };
    info!("Sending segment finished message to {}", connection_id);
    if let Err(e) = connection.socket.write_message(Message::Binary(
        rmp_serde::to_vec(&EncoderMessage::<T>::SegmentFinished).unwrap(),
    )) {
        error!(
            "Failed to write segment finished message for connection {}: {}",
            connection_id, e
        );
    };
    if let Err(e) = connection.socket.close(None) {
        error!("Failed to close connection {}: {}", connection_id, e);
    };
}

fn wait_for_remote_data<T: Pixel + Default + Serialize + DeserializeOwned>(
    mut connection: ConnectedSocket,
    slot_request: SlotRequestMessage,
    pool: Arc<rayon::ThreadPool>,
    temp_dir: Option<PathBuf>,
) {
    loop {
        match connection.socket.read_message() {
            Ok(Message::Binary(data)) => {
                // This must be an encoder message, start the encode
                if let Ok(input) = rmp_serde::from_read::<_, RawFrameData>(data.as_slice()) {
                    let frame_count = input.compressed_frames.len();
                    let connection_id = input.connection_id;
                    info!("Received {} raw frames from {}", frame_count, connection_id);
                    let input = if let Some(temp_dir) = temp_dir {
                        let mut temp_path = temp_dir;
                        temp_path.push(connection_id.to_hyphenated().to_string());
                        temp_path.set_extension("py4m");
                        let file = File::create(&temp_path).unwrap();
                        let mut writer = BufWriter::new(file);
                        for frame in input.compressed_frames {
                            let frame_data =
                                bincode::serialize(&decompress_frame::<T>(&frame)).unwrap();
                            writer
                                .write_u32::<LittleEndian>(frame_data.len() as u32)
                                .unwrap();
                            writer.write_all(&frame_data).unwrap();
                        }
                        writer.flush().unwrap();
                        SegmentFrameData::Y4MFile {
                            path: temp_path,
                            frame_count,
                            connection_id,
                            video_info: slot_request.video_info,
                        }
                    } else {
                        SegmentFrameData::CompressedFrames(input)
                    };
                    if slot_request.video_info.bit_depth <= 8 {
                        encode_segment::<u8>(connection, slot_request, input, pool);
                    } else {
                        encode_segment::<u16>(connection, slot_request, input, pool);
                    }
                    return;
                } else {
                    error!("Failed to read frame data for connection {}", connection.id);
                }
            }
            Ok(Message::Text(_)) | Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => {
                // No action needed
            }
            Ok(Message::Close(_)) => {
                info!("Connection {} closed by client", connection.id);
                return;
            }
            Err(e) => {
                warn!("Connection {} closed unexpectedly: {}", connection.id, e);
                return;
            }
        }
    }
}

pub enum SegmentFrameData {
    CompressedFrames(RawFrameData),
    Y4MFile {
        connection_id: Uuid,
        path: PathBuf,
        frame_count: usize,
        video_info: VideoDetails,
    },
}
