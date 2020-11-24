use crate::decode::{get_video_details, process_raw_frame, read_raw_frame, DecodeError};
use crate::progress::{get_segment_input_filename, get_segment_output_filename};
use crate::remote::{wait_for_slot_allocation, RemoteWorkerInfo};
use crate::{compress_frame, CliOptions, CLIENT};
use av_scenechange::{new_detector, DetectionOptions};
use byteorder::{LittleEndian, WriteBytesExt};
use crossbeam_channel::{unbounded, Receiver, Sender};
use crossbeam_utils::thread::Scope;
use itertools::Itertools;
use log::{debug, error};
use parking_lot::Mutex;
use rav1e::prelude::*;
use rav1e_by_gop::*;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use systemstat::ByteSize;
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
    num_segments: Arc<AtomicUsize>,
) {
    let sc_opts = DetectionOptions {
        fast_analysis: opts.speed >= 10,
        ignore_flashes: true,
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
    let max_bitrate = opts.max_bitrate;
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
            max_bitrate,
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
            let mut detector = new_detector(&mut dec, sc_opts);
            let mut analysis_frameno = next_frameno;
            let mut lookahead_frameno = 0;
            let mut segment_no = 0;
            let mut start_frameno;
            // The first keyframe will always be 0, so get the second keyframe.
            let mut next_known_keyframe = known_keyframes.iter().nth(1).copied();
            let mut keyframes: BTreeSet<usize> = known_keyframes.clone();
            keyframes.insert(0);
            let mut lookahead_queue: BTreeMap<usize, Arc<Frame<T>>> = BTreeMap::new();

            let enc_cfg = build_encoder_config(opts.speed, opts.qp, opts.max_bitrate, video_info, rayon_pool);
            let ctx: Context<T> = enc_cfg.new_context::<T>().unwrap();

            while let Ok(message) = slot_ready_listener.recv() {
                debug!("Received slot ready message");
                match message {
                    Slot::Local(slot) => &progress_senders[slot],
                    Slot::Remote(ref conn) => &conn.progress_sender,
                }
                .send(ProgressStatus::Loading)
                .unwrap();

                let mut processed_frames: Vec<Vec<u8>> = Vec::new();
                let mut frame_count = 0;
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
                            num_segments.fetch_add(1, Ordering::SeqCst);
                            continue;
                        } else {
                            processed_frames =
                                Vec::with_capacity(next_keyframe - lookahead_frameno);
                            frame_count = 0;

                            if opts.temp_input && !message.is_remote() {
                                let file = File::create(get_segment_input_filename(
                                    match &opts.output {
                                        Output::File(output) => &output,
                                        Output::Null => unimplemented!("Temp file input not supported with /dev/null output"),
                                        _ => unreachable!()
                                    },
                                    segment_no + 1,
                                ))
                                .unwrap();
                                let mut writer = BufWriter::new(file);
                                while lookahead_frameno < next_keyframe {
                                    match read_raw_frame(&mut dec) {
                                        Ok(frame) => {
                                            frame_count += 1;
                                            let frame_data = bincode::serialize(
                                                &process_raw_frame(&frame, &ctx, &cfg),
                                            )
                                            .unwrap();
                                            writer
                                                .write_u32::<LittleEndian>(frame_data.len() as u32)
                                                .unwrap();
                                            writer.write_all(&frame_data).unwrap();
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
                            } else {
                                while lookahead_frameno < next_keyframe {
                                    match read_raw_frame(&mut dec) {
                                        Ok(frame) => {
                                            processed_frames.push(compress_frame::<T>(
                                                &process_raw_frame(&frame, &ctx, &cfg),
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
                                    }
                                }
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
                                            Arc::new(process_raw_frame(&frame, &ctx, &cfg)),
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
                                    .cloned()
                                    .collect::<Vec<_>>();
                                if frame_set.len() >= 2 {
                                    if detector.analyze_next_frame(
                                        &frame_set,
                                        analysis_frameno as u64,
                                        *keyframes.iter().last().unwrap() as u64
                                    ) {
                                        keyframes.insert(analysis_frameno);
                                    }
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
                                        pool_handle.lock()[slot] = false;
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
                                                status: Some(SlotStatus::Empty),
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

                        // The frames comprising the segment are known
                        let interval: (usize, usize) = keyframes
                            .iter()
                            .rev()
                            .take(2)
                            .rev()
                            .copied()
                            .collect_tuple()
                            .unwrap();
                        let interval_len = interval.1 - interval.0;
                        if opts.temp_input && !message.is_remote() {
                            let file = File::create(get_segment_input_filename(
                                match &opts.output {
                                    Output::File(output) => &output,
                                    Output::Null => unimplemented!("Temp file input not supported with /dev/null output"),
                                    _ => unimplemented!()
                                },
                                segment_no + 1,
                            ))
                            .unwrap();
                            let mut writer = BufWriter::new(file);
                            frame_count = 0;
                            for frameno in (interval.0)..(interval.1) {
                                frame_count += 1;
                                let frame_data =
                                    bincode::serialize(&Arc::try_unwrap(lookahead_queue.remove(&frameno).unwrap()).unwrap())
                                        .unwrap();
                                writer
                                    .write_u32::<LittleEndian>(frame_data.len() as u32)
                                    .unwrap();
                                writer.write_all(&frame_data).unwrap();
                            }
                        } else {
                            processed_frames = Vec::with_capacity(interval_len);
                            for frameno in (interval.0)..(interval.1) {
                                processed_frames.push(compress_frame(
                                    &lookahead_queue.remove(&frameno).unwrap(),
                                ));
                            }
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
                                frame_data: if opts.temp_input {
                                    SegmentFrameData::Y4MFile {
                                        frame_count,
                                        path: get_segment_input_filename(
                                            match &opts.output {
                                                Output::File(output) => &output,
                                                Output::Null => unimplemented!("Temp file input not supported with /dev/null output"),
                                                _ => unimplemented!()
                                            },
                                            segment_no + 1,
                                        ),
                                    }
                                } else {
                                    SegmentFrameData::CompressedFrames(processed_frames)
                                },
                            }))
                            .unwrap();
                    }
                    Slot::Remote(mut connection) => {
                        debug!("Encoding with remote slot");
                        let output = opts.output.clone();
                        let remote_sender = remote_sender.clone();
                        scope.spawn(move |_| {
                            let frame_count = processed_frames.len();
                            connection
                                .progress_sender
                                .send(ProgressStatus::Sending(ByteSize(
                                    processed_frames
                                        .iter()
                                        .map(|frame| frame.len() as u64)
                                        .sum(),
                                )))
                                .unwrap();
                            while CLIENT
                                .post(&format!("{}{}/{}", &connection.worker_uri, "segment", connection.request_id))
                                .header("X-RAV1E-AUTH", &connection.worker_password)
                                .json(&PostSegmentMessage {
                                    keyframe_number: start_frameno,
                                    segment_idx: segment_no + 1,
                                    next_analysis_frame: analysis_frameno - 1
                                })
                                .send()
                                .and_then(|res| res.error_for_status()).is_err() {
                                    sleep(Duration::from_secs(5));
                            }
                            while CLIENT
                                .post(&format!("{}{}/{}", &connection.worker_uri, "segment_data", connection.request_id))
                                .header("X-RAV1E-AUTH", &connection.worker_password)
                                .body(bincode::serialize(&processed_frames).unwrap())
                                .send()
                                .and_then(|res| res.error_for_status()).is_err() {
                                    sleep(Duration::from_secs(5));
                            }
                            connection
                                .worker_update_sender
                                .send(WorkerStatusUpdate {
                                    status: Some(SlotStatus::Empty),
                                    slot_delta: None,
                                })
                                .unwrap();
                            connection.encode_info = Some(EncodeInfo {
                                output_file: match output {
                                    Output::File(output) => Output::File(
                                        get_segment_output_filename(&output, segment_no + 1),
                                    ),
                                    x => x
                                },
                                frame_count,
                                next_analysis_frame: analysis_frameno - 1,
                                segment_idx: segment_no + 1,
                                start_frameno,
                            });
                            remote_sender.send(*connection).unwrap();
                        });
                    }
                }
                segment_no += 1;
                num_segments.fetch_add(1, Ordering::SeqCst);
            }
        })
        .join()
        .unwrap();

    // Close any extra ready sockets
    while let Ok(message) = slot_ready_channel.1.try_recv() {
        match message {
            Slot::Local(slot) => {
                slot_pool.lock()[slot] = false;
            }
            Slot::Remote(connection) => {
                connection
                    .worker_update_sender
                    .send(WorkerStatusUpdate {
                        status: Some(SlotStatus::Empty),
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
    max_bitrate: Option<i32>,
) {
    loop {
        if input_finished_receiver.is_full() {
            debug!("Exiting slot checker loop");
            return;
        }

        sleep(Duration::from_millis(500));

        {
            let mut pool_lock = pool.lock();
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
        for worker in remote_pool.lock().iter_mut() {
            if worker.workers.iter().all(|worker| *worker) {
                worker_start_idx += worker.workers.len();
                continue;
            }
            if let SlotStatus::Empty = worker.slot_status {
                debug!("Empty connection--requesting new slot");
                match CLIENT
                    .post(&format!("{}{}", &worker.uri, "enqueue"))
                    .header("X-RAV1E-AUTH", &worker.password)
                    .json(&SlotRequestMessage {
                        options: EncodeOptions {
                            speed,
                            qp,
                            max_bitrate,
                        },
                        video_info,
                        client_version: semver::Version::parse(env!("CARGO_PKG_VERSION")).unwrap(),
                    })
                    .send()
                    .and_then(|res| res.error_for_status())
                    .and_then(|res| res.json::<PostEnqueueResponse>())
                {
                    Ok(response) => {
                        worker.slot_status = SlotStatus::Requested;
                        let slot = worker.workers.iter().position(|worker| !worker).unwrap();
                        let connection = ActiveConnection {
                            worker_uri: worker.uri.clone(),
                            worker_password: worker.password.clone(),
                            video_info,
                            request_id: response.request_id,
                            encode_info: None,
                            worker_update_sender: worker.update_channel.0.clone(),
                            slot_in_worker: slot,
                            progress_sender: remote_progress_senders[worker_start_idx + slot]
                                .clone(),
                        };
                        let slot_ready_sender = slot_ready_sender.clone();
                        scope.spawn(move |_| {
                            wait_for_slot_allocation::<T>(connection, slot_ready_sender)
                        });
                    }
                    Err(e) => {
                        error!("Failed to contact remote server: {}", e);
                    }
                }
            }
            worker_start_idx += worker.workers.len();
        }
    }
}
