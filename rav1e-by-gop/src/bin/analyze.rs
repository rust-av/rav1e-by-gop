use crate::compress_frame;
use crate::decode::{get_video_details, process_raw_frame, read_raw_frame, DecodeError};
use crate::progress::get_segment_output_filename;
use crate::remote::{wait_for_slot_allocation, RemoteWorkerInfo};
use crate::CliOptions;
use av_scenechange::{DetectionOptions, SceneChangeDetector};
use crossbeam_channel::{unbounded, Receiver, Sender};
use crossbeam_utils::thread::Scope;
use http::request::Request;
use itertools::Itertools;
use log::{debug, error};
use rav1e::prelude::*;
use rav1e_by_gop::*;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;
use systemstat::ByteSize;
use tungstenite::{connect, Message};
use v_frame::frame::Frame;
use v_frame::pixel::Pixel;
use y4m::Decoder;

pub(crate) type AnalyzerSender = Sender<Option<SegmentData>>;
pub(crate) type AnalyzerReceiver = Receiver<Option<SegmentData>>;
pub(crate) type AnalyzerChannel = (AnalyzerSender, AnalyzerReceiver);

pub(crate) type RemoteAnalyzerSender = Sender<ActiveConnection>;
pub(crate) type RemoteAnalyzerReceiver = Receiver<ActiveConnection>;
pub(crate) type RemoteAnalyzerChannel = (RemoteAnalyzerSender, RemoteAnalyzerReceiver);

pub(crate) type InputFinishedSender = Sender<()>;
pub(crate) type InputFinishedReceiver = Receiver<()>;
pub(crate) type InputFinishedChannel = (InputFinishedSender, InputFinishedReceiver);

pub(crate) type SlotReadySender = Sender<Slot>;
pub(crate) type SlotReadyReceiver = Receiver<Slot>;
pub(crate) type SlotReadyChannel = (SlotReadySender, SlotReadyReceiver);

pub(crate) fn run_first_pass<
    T: Pixel + Serialize + DeserializeOwned + Default,
    R: 'static + Read + Send,
>(
    mut dec: Decoder<R>,
    opts: CliOptions,
    sender: AnalyzerSender,
    remote_sender: RemoteAnalyzerSender,
    progress_senders: Vec<ProgressSender>,
    remote_progress_senders: Vec<ProgressSender>,
    input_finished_sender: InputFinishedSender,
    input_finished_receiver: InputFinishedReceiver,
    slot_pool: Arc<Mutex<Vec<bool>>>,
    remote_pool: Arc<Mutex<Vec<RemoteWorkerInfo>>>,
    rayon_pool: Arc<rayon::ThreadPool>,
    next_frameno: usize,
    known_keyframes: BTreeSet<usize>,
    skipped_segments: BTreeSet<usize>,
    video_info: VideoDetails,
    scope: &Scope,
) {
    let sc_opts = DetectionOptions {
        fast_analysis: opts.speed >= 10,
        ignore_flashes: false,
        lookahead_distance: 5,
        min_scenecut_distance: Some(opts.min_keyint as usize),
        max_scenecut_distance: Some(opts.max_keyint as usize),
    };
    let cfg = get_video_details(&dec);
    let slot_ready_channel: SlotReadyChannel = unbounded();

    // Wait for an open slot before loading more frames,
    // to reduce memory usage.
    let slot_ready_sender = slot_ready_channel.0.clone();
    let pool_handle = slot_pool.clone();
    let speed = opts.speed;
    let qp = opts.qp;
    scope.spawn(move |s| {
        slot_checker_loop::<T>(
            pool_handle,
            remote_pool,
            slot_ready_sender,
            remote_progress_senders,
            input_finished_receiver,
            s,
            video_info,
            speed,
            qp,
        );
    });

    let pool_handle = slot_pool.clone();
    let slot_ready_listener = slot_ready_channel.1.clone();
    let lookahead_distance = sc_opts.lookahead_distance;
    let frame_limit = opts
        .max_frames
        .map(|f| f as usize)
        .unwrap_or(usize::max_value());
    scope
        .spawn(move |scope| {
            let mut detector =
                SceneChangeDetector::new(dec.get_bit_depth(), cfg.chroma_sampling, &sc_opts);
            let mut analysis_frameno = next_frameno;
            let mut lookahead_frameno = 0;
            let mut segment_no = 0;
            let mut start_frameno;
            // The first keyframe will always be 0, so get the second keyframe.
            let mut next_known_keyframe = known_keyframes.iter().nth(1).copied();
            let mut keyframes: BTreeSet<usize> = known_keyframes.clone();
            keyframes.insert(0);
            let mut lookahead_queue: BTreeMap<usize, Frame<T>> = BTreeMap::new();

            let enc_cfg = build_encoder_config(opts.speed, opts.qp, video_info, rayon_pool);
            let ctx: Context<T> = enc_cfg.new_context::<T>().unwrap();

            while let Ok(message) = slot_ready_listener.recv() {
                debug!("Received slot ready message");
                match message {
                    Slot::Local(slot) => &progress_senders[slot],
                    Slot::Remote(ref conn) => &conn.progress_sender,
                }
                .send(ProgressStatus::Loading)
                .unwrap();

                let mut processed_frames: Vec<Vec<u8>>;
                loop {
                    if let Some(next_keyframe) = next_known_keyframe {
                        start_frameno = lookahead_frameno;
                        next_known_keyframe = known_keyframes
                            .iter()
                            .copied()
                            .find(|kf| *kf > next_keyframe);

                        // Quickly seek ahead if this is a skipped segment
                        if skipped_segments.contains(&(segment_no + 1)) {
                            while lookahead_frameno < next_keyframe {
                                match read_raw_frame(&mut dec) {
                                    Ok(_) => {
                                        lookahead_frameno += 1;
                                    }
                                    Err(DecodeError::EOF) => {
                                        break;
                                    }
                                    Err(e) => {
                                        error!("Decode error: {}", e);
                                        return;
                                    }
                                }
                            }
                            segment_no += 1;
                            continue;
                        } else {
                            processed_frames =
                                Vec::with_capacity(next_keyframe - lookahead_frameno);
                            while lookahead_frameno < next_keyframe {
                                match read_raw_frame(&mut dec) {
                                    Ok(frame) => {
                                        processed_frames.push(compress_frame::<T>(
                                            &process_raw_frame(frame, &ctx, &cfg),
                                        ));
                                        lookahead_frameno += 1;
                                    }
                                    Err(DecodeError::EOF) => {
                                        break;
                                    }
                                    Err(e) => {
                                        error!("Decode error: {}", e);
                                        return;
                                    }
                                };
                            }
                        }
                    } else {
                        start_frameno = keyframes.iter().copied().last().unwrap();
                        loop {
                            // Load frames until the lookahead queue is filled
                            while analysis_frameno + lookahead_distance > lookahead_frameno
                                && lookahead_frameno < frame_limit
                            {
                                match read_raw_frame(&mut dec) {
                                    Ok(frame) => {
                                        lookahead_queue.insert(
                                            lookahead_frameno,
                                            process_raw_frame(frame, &ctx, &cfg),
                                        );
                                        lookahead_frameno += 1;
                                    }
                                    Err(DecodeError::EOF) => {
                                        break;
                                    }
                                    Err(e) => {
                                        error!("Decode error: {}", e);
                                        return;
                                    }
                                };
                            }

                            // Analyze the current frame for a scenechange
                            if analysis_frameno != keyframes.iter().last().copied().unwrap() {
                                // The frame_queue should start at whatever the previous frame was
                                let frame_set = lookahead_queue
                                    .iter()
                                    .skip_while(|(frameno, _)| **frameno < analysis_frameno - 1)
                                    .take(lookahead_distance + 1)
                                    .map(|(_, frame)| frame)
                                    .collect::<Vec<_>>();
                                if frame_set.len() >= 2 {
                                    detector.analyze_next_frame(
                                        &frame_set,
                                        analysis_frameno,
                                        &mut keyframes,
                                    );
                                } else {
                                    // End of encode
                                    keyframes.insert(*lookahead_queue.iter().last().unwrap().0 + 1);
                                    break;
                                }

                                analysis_frameno += 1;
                                if keyframes.iter().last().copied().unwrap() == analysis_frameno - 1
                                {
                                    // Keyframe found
                                    break;
                                }
                            } else if analysis_frameno < lookahead_frameno {
                                analysis_frameno += 1;
                            } else {
                                debug!("End of encode");
                                sender.send(None).unwrap();
                                input_finished_sender.send(()).unwrap();
                                match message {
                                    Slot::Local(slot) => {
                                        pool_handle.lock().unwrap()[slot] = false;
                                        progress_senders[slot].send(ProgressStatus::Idle).unwrap();
                                    }
                                    Slot::Remote(connection) => {
                                        connection
                                            .progress_sender
                                            .send(ProgressStatus::Idle)
                                            .unwrap();
                                        connection
                                            .worker_update_sender
                                            .send(WorkerStatusUpdate {
                                                status: Some(SlotStatus::None),
                                                slot_delta: Some((
                                                    connection.slot_in_worker,
                                                    false,
                                                )),
                                            })
                                            .unwrap();
                                    }
                                };
                                return;
                            }
                        }

                        // The frames comprising the segment are known, so set them in `processed_frames`
                        let interval: (usize, usize) = keyframes
                            .iter()
                            .rev()
                            .take(2)
                            .rev()
                            .copied()
                            .collect_tuple()
                            .unwrap();
                        let interval_len = interval.1 - interval.0;
                        processed_frames = Vec::with_capacity(interval_len);
                        for frameno in (interval.0)..(interval.1) {
                            processed_frames
                                .push(compress_frame(&lookahead_queue.remove(&frameno).unwrap()));
                        }
                    }
                    break;
                }

                match message {
                    Slot::Local(slot) => {
                        debug!("Encoding with local slot");
                        sender
                            .send(Some(SegmentData {
                                segment_no,
                                slot,
                                next_analysis_frame: analysis_frameno - 1,
                                start_frameno,
                                compressed_frames: processed_frames,
                            }))
                            .unwrap();
                    }
                    Slot::Remote(mut connection) => {
                        debug!("Encoding with remote slot");
                        let output = opts.output.clone();
                        let remote_sender = remote_sender.clone();
                        scope.spawn(move |_| {
                            let raw_data = RawFrameData {
                                connection_id: connection.connection_id.unwrap(),
                                compressed_frames: processed_frames.clone(),
                            };

                            connection
                                .progress_sender
                                .send(ProgressStatus::Sending(ByteSize(
                                    raw_data
                                        .compressed_frames
                                        .iter()
                                        .map(|frame| frame.len() as u64)
                                        .sum(),
                                )))
                                .unwrap();
                            while let Err(e) = connection.socket.write_message(Message::Binary(
                                rmp_serde::to_vec(&raw_data).unwrap(),
                            )) {
                                error!(
                                    "Failed to send frames to connection {}: {}",
                                    connection.connection_id.unwrap(),
                                    e
                                );
                                sleep(Duration::from_secs(1));
                            }
                            connection
                                .worker_update_sender
                                .send(WorkerStatusUpdate {
                                    status: Some(SlotStatus::None),
                                    slot_delta: None,
                                })
                                .unwrap();
                            connection.encode_info = Some(EncodeInfo {
                                output_file: get_segment_output_filename(&output, segment_no + 1),
                                frame_count: processed_frames.len(),
                                next_analysis_frame: analysis_frameno - 1,
                                segment_idx: segment_no + 1,
                                start_frameno,
                            });
                            remote_sender.send(*connection).unwrap();
                        });
                    }
                }
                segment_no += 1;
            }
        })
        .join()
        .unwrap();

    // Close any extra ready sockets
    while let Ok(message) = slot_ready_channel.1.try_recv() {
        match message {
            Slot::Local(slot) => {
                slot_pool.lock().unwrap()[slot] = false;
            }
            Slot::Remote(connection) => {
                connection
                    .worker_update_sender
                    .send(WorkerStatusUpdate {
                        status: Some(SlotStatus::None),
                        slot_delta: Some((connection.slot_in_worker, false)),
                    })
                    .unwrap();
            }
        };
    }
}

fn slot_checker_loop<T: Pixel + DeserializeOwned + Default>(
    pool: Arc<Mutex<Vec<bool>>>,
    remote_pool: Arc<Mutex<Vec<RemoteWorkerInfo>>>,
    slot_ready_sender: SlotReadySender,
    remote_progress_senders: Vec<ProgressSender>,
    input_finished_receiver: InputFinishedReceiver,
    scope: &Scope,
    video_info: VideoDetails,
    speed: usize,
    qp: usize,
) {
    loop {
        if input_finished_receiver.is_full() {
            debug!("Exiting slot checker loop");
            return;
        }

        sleep(Duration::from_millis(500));

        {
            let mut pool_lock = pool.lock().unwrap();
            if let Some(slot) = pool_lock.iter().position(|slot| !*slot) {
                if slot_ready_sender.send(Slot::Local(slot)).is_err() {
                    debug!("Exiting slot checker loop");
                    return;
                };
                pool_lock[slot] = true;
                continue;
            }
        }

        let mut worker_start_idx = 0;
        for worker in remote_pool.lock().unwrap().iter_mut() {
            if worker.workers.iter().all(|worker| *worker) {
                worker_start_idx += worker.workers.len();
                continue;
            }
            if let SlotStatus::None = worker.slot_status {
                debug!("Empty connection--requesting new slot");
                let request = Request::builder()
                    .uri(worker.uri.as_str())
                    .header("X-RAV1E-AUTH", &worker.password)
                    .body(())
                    .unwrap();
                let mut connection = match connect(request) {
                    Ok(c) => c.0,
                    Err(e) => {
                        error!("Failed to connect to {}: {}", worker.uri, e);
                        worker_start_idx += worker.workers.len();
                        continue;
                    }
                };
                if let Err(e) = connection.write_message(Message::Binary(
                    rmp_serde::to_vec(&SlotRequestMessage {
                        options: EncodeOptions { speed, qp },
                        video_info,
                        client_version: semver::Version::parse(env!("CARGO_PKG_VERSION")).unwrap(),
                    })
                    .unwrap(),
                )) {
                    let _ = connection.close(None);
                    error!("Failed to send slot request to {}: {}", worker.uri, e);
                    worker_start_idx += worker.workers.len();
                    continue;
                };
                worker.slot_status = SlotStatus::Requested;
                let slot = worker.workers.iter().position(|worker| !worker).unwrap();
                let connection = ActiveConnection {
                    socket: connection,
                    video_info,
                    connection_id: None,
                    encode_info: None,
                    worker_update_sender: worker.update_channel.0.clone(),
                    slot_in_worker: slot,
                    progress_sender: remote_progress_senders[worker_start_idx + slot].clone(),
                };
                let slot_ready_sender = slot_ready_sender.clone();
                scope.spawn(move |_| wait_for_slot_allocation::<T>(connection, slot_ready_sender));
            }
            worker_start_idx += worker.workers.len();
        }
    }
}
