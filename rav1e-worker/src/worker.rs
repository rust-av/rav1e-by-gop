use crate::channels::SlotRequestReceiver;
use crate::ConnectedSocket;
use crossbeam_utils::thread::Scope;
use log::{debug, error, info, warn};
use rav1e::prelude::*;
use rav1e_by_gop::*;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::cmp;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;
use threadpool::ThreadPool;
use tungstenite::Message;
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
            slot_request_queue_handle.lock().unwrap().push_back(message);
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

        let mut queue = slot_request_queue_handle.lock().unwrap();
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

            let rayon_handle = rayon_pool.clone();
            thread_pool
                .execute(move || wait_for_remote_data(connection, slot_request, rayon_handle));
        }
    });
}

pub fn encode_segment<T: Pixel + Default + Serialize + DeserializeOwned>(
    mut connection: ConnectedSocket,
    encode_request: SlotRequestMessage,
    input: RawFrameData,
    pool: Arc<rayon::ThreadPool>,
) {
    let connection_id = input.connection_id;
    if !input.compressed_frames.is_empty() {
        let mut source = Source {
            frame_data: SourceFrameData::CompressedFrames(input.compressed_frames),
            sent_count: 0,
        };
        let cfg = build_encoder_config(
            encode_request.options.speed,
            encode_request.options.qp,
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
                        rmp_serde::to_vec(&EncoderMessage::<T>::EncodeFailed(e.to_string()))
                            .unwrap(),
                    ));
                    let _ = connection.socket.close(None);
                    return;
                }
            };
        }
    }

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

fn wait_for_remote_data(
    mut connection: ConnectedSocket,
    slot_request: SlotRequestMessage,
    pool: Arc<rayon::ThreadPool>,
) {
    loop {
        match connection.socket.read_message() {
            Ok(Message::Binary(data)) => {
                // This must be an encoder message, start the encode
                if let Ok(input) = rmp_serde::from_read::<_, RawFrameData>(data.as_slice()) {
                    info!(
                        "Received {} raw frames from {}",
                        input.compressed_frames.len(),
                        connection.id
                    );
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
