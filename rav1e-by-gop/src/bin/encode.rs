use super::VideoDetails;
use crate::analyze::{
    run_first_pass, AnalyzerChannel, AnalyzerReceiver, InputFinishedChannel, InputFinishedReceiver,
    RemoteAnalyzerChannel, RemoteAnalyzerReceiver,
};
use crate::decode::*;
use crate::muxer::create_muxer;
use crate::progress::*;
use crate::remote::{discover_remote_worker, remote_encode_segment, RemoteWorkerInfo};
use crate::CliOptions;
use crate::{decide_thread_count, Output};
use anyhow::{bail, Result};
use console::style;
use crossbeam_channel::{bounded, unbounded, TryRecvError};
use crossbeam_utils::thread::{scope, Scope};
use log::{debug, error, info};
use parking_lot::Mutex;
use rav1e::prelude::*;
use rav1e_by_gop::*;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::BTreeSet;
use std::fs::remove_file;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::thread::sleep;
use std::time::Duration;
use threadpool::ThreadPool;
use y4m::Decoder;

pub fn perform_encode(
    keyframes: BTreeSet<usize>,
    next_analysis_frame: usize,
    opts: &CliOptions,
    progress: Option<ProgressInfo>,
) -> Result<()> {
    let reader = opts.input.as_reader()?;
    let dec = y4m::decode(reader).expect("input is not a y4m file");
    let video_info = get_video_details(&dec);
    scope(|s| {
        if video_info.bit_depth == 8 {
            perform_encode_inner::<u8, _>(
                s,
                keyframes,
                next_analysis_frame,
                opts,
                progress,
                dec,
                video_info,
            )
        } else {
            perform_encode_inner::<u16, _>(
                s,
                keyframes,
                next_analysis_frame,
                opts,
                progress,
                dec,
                video_info,
            )
        }
    })
    .unwrap()
}

pub fn perform_encode_inner<
    T: Pixel + Serialize + DeserializeOwned + Default,
    R: 'static + Read + Send,
>(
    s: &Scope,
    keyframes: BTreeSet<usize>,
    next_analysis_frame: usize,
    opts: &CliOptions,
    progress: Option<ProgressInfo>,
    dec: Decoder<R>,
    video_info: VideoDetails,
) -> Result<()> {
    info!(
        "Using {} decoder: {}p @ {} fps, {}, {}",
        style("y4m").cyan(),
        style(format!("{}x{}", video_info.width, video_info.height)).cyan(),
        style(format!(
            "{}/{}",
            video_info.time_base.den, video_info.time_base.num
        ))
        .cyan(),
        style(video_info.chroma_sampling).cyan(),
        style(format!("{}-bit", video_info.bit_depth)).cyan()
    );

    let remote_workers = opts
        .workers
        .iter()
        .map(|worker| discover_remote_worker(worker))
        .filter_map(|worker| {
            worker
                .map_err(|e| {
                    error!("Failed to negotiate with remote worker: {}", e);
                    e
                })
                .ok()
        })
        .collect::<Vec<_>>();
    let worker_thread_count = remote_workers
        .iter()
        .map(|worker| worker.workers.len())
        .sum::<usize>();

    let num_threads = decide_thread_count(opts, &video_info, !remote_workers.is_empty());

    if !remote_workers.is_empty() {
        info!(
            "Discovered {} remote workers with up to {} slots",
            remote_workers.len(),
            worker_thread_count
        )
    } else if num_threads == 0 {
        bail!("Cannot disable local threads without having remote workers available!");
    }

    let mut thread_pool = if num_threads > 0 {
        Some(ThreadPool::new(num_threads))
    } else {
        None
    };
    let rayon_pool = Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()
            .unwrap(),
    );

    let overall_progress = if let Some(progress) = progress {
        progress
    } else {
        let progress = ProgressInfo::new(
            Rational {
                num: video_info.time_base.den,
                den: video_info.time_base.num,
            },
            0,
            keyframes,
            0,
            next_analysis_frame,
            opts.max_frames,
        );

        // Do an initial write of the progress file,
        // so we don't need to redo keyframe search.
        update_progress_file(&opts.output, &progress);

        progress
    };

    let num_local_slots = if num_threads == 0 {
        0
    } else {
        let local_workers = opts.local_workers.unwrap_or_else(|| {
            // workers tend to use about 56% of the available cpus
            // tiles tend to use about the 40% of the available cpus
            // letting rayon schedule the workload seems to deal with starvation
            // reserve 1 thread for 4 parallel tiles

            num_threads - opts.tiles * 2 / 8
        });
        info!(
            "Using {} encoder threads ({} local workers)",
            style(num_threads).cyan(),
            style(local_workers).cyan()
        );
        local_workers
    };

    let analyzer_channel: AnalyzerChannel = unbounded();
    let remote_analyzer_channel: RemoteAnalyzerChannel = unbounded();
    let progress_channels: Vec<ProgressChannel> = (0..(num_local_slots + worker_thread_count))
        .map(|_| unbounded())
        .collect();
    let input_finished_channel: InputFinishedChannel = bounded(1);
    let slots: Arc<Mutex<Vec<bool>>> = Arc::new(Mutex::new(vec![false; num_local_slots]));
    let remote_slots = Arc::new(Mutex::new(remote_workers));

    let output_file = opts.output.clone();
    let verbose = opts.verbose;
    let start_frameno = overall_progress.next_analysis_frame;
    let known_keyframes = overall_progress.keyframes.clone();
    let skipped_segments = overall_progress.completed_segments.clone();
    let display_progress = opts.display_progress;

    let receivers = progress_channels
        .iter()
        .map(|(_, rx)| rx.clone())
        .collect::<Vec<_>>();
    let slots_ref = slots.clone();
    let remote_slots_ref = remote_slots.clone();
    let input_finished_receiver = input_finished_channel.1.clone();
    let max_frames = opts.max_frames;
    let num_segments = Arc::new(AtomicUsize::new(0));
    s.spawn(move |_| {
        watch_progress_receivers(
            receivers,
            slots_ref,
            remote_slots_ref,
            output_file,
            verbose,
            overall_progress,
            input_finished_receiver,
            display_progress,
            max_frames,
        );
    });

    let opts_ref = opts.clone();
    let analyzer_sender = analyzer_channel.0.clone();
    let remote_analyzer_sender = remote_analyzer_channel.0.clone();
    let remote_slots_ref = remote_slots.clone();
    let input_finished_sender = input_finished_channel.0.clone();
    let input_finished_receiver = input_finished_channel.1.clone();
    let progress_senders = progress_channels
        .iter()
        .map(|(tx, _)| tx.clone())
        .collect::<Vec<_>>();
    let remote_progress_senders = progress_senders
        .iter()
        .skip(num_local_slots)
        .cloned()
        .collect::<Vec<_>>();
    let rayon_handle = rayon_pool.clone();
    let num_segments_handle = num_segments.clone();
    s.spawn(move |s| {
        run_first_pass::<T, R>(
            dec,
            opts_ref,
            analyzer_sender,
            remote_analyzer_sender,
            progress_senders,
            remote_progress_senders,
            input_finished_sender,
            input_finished_receiver,
            slots,
            remote_slots_ref,
            rayon_handle,
            start_frameno,
            known_keyframes,
            skipped_segments,
            video_info,
            s,
            num_segments_handle,
        );
    });

    for worker in remote_slots.lock().iter() {
        let receiver = worker.update_channel.1.clone();
        let remote_slots_ref = remote_slots.clone();
        let input_finished_receiver = input_finished_channel.1.clone();
        s.spawn(move |_| watch_worker_updates(remote_slots_ref, receiver, input_finished_receiver));
    }

    // Write only the ivf header
    create_muxer(&match &opts.output {
        Output::File(output_file) => Output::File(get_segment_output_filename(&output_file, 0)),
        x => x.clone(),
    })
    .map(|mut output| {
        output.write_header(
            video_info.width,
            video_info.height,
            video_info.time_base.den as usize,
            video_info.time_base.num as usize,
        );
    })
    .expect("Failed to create segment output");

    let input_finished_receiver = input_finished_channel.1;
    let remote_analyzer_receiver = remote_analyzer_channel.1;
    let remote_slots_ref = remote_slots;
    let remote_listener = s.spawn(move |s| {
        listen_for_remote_workers(
            s,
            remote_analyzer_receiver,
            input_finished_receiver,
            remote_slots_ref,
        )
    });

    if num_threads > 0 {
        let _ = listen_for_local_workers::<T>(
            EncodeOptions::from(opts),
            &opts.output,
            thread_pool.as_mut().unwrap(),
            rayon_pool,
            video_info,
            &progress_channels,
            analyzer_channel.1,
        );
        thread_pool.unwrap().join();
    }
    let _ = remote_listener.join();

    if let Output::File(output) = &opts.output {
        mux_output_files(&output, num_segments.load(Ordering::SeqCst))?;
    };

    Ok(())
}

fn watch_worker_updates(
    remote_slots: Arc<Mutex<Vec<RemoteWorkerInfo>>>,
    update_receiver: WorkerUpdateReceiver,
    input_finished_receiver: InputFinishedReceiver,
) {
    let mut done = false;
    loop {
        sleep(Duration::from_millis(100));

        if !done && input_finished_receiver.is_full() {
            debug!("Worker update thread knows input is done");
            done = true;
            let remote_slots_ref = remote_slots.lock();
            if remote_slots_ref
                .iter()
                .find(|worker| worker.update_channel.1.same_channel(&update_receiver))
                .unwrap()
                .workers
                .iter()
                .filter(|worker| **worker)
                .count()
                == 0
            {
                debug!("Exiting worker update thread");
                return;
            }
        }

        if let Ok(message) = update_receiver.try_recv() {
            debug!("Updating worker status: {:?}", message);
            let mut remote_slots_ref = remote_slots.lock();
            let worker = remote_slots_ref
                .iter_mut()
                .find(|worker| worker.update_channel.1.same_channel(&update_receiver))
                .unwrap();
            if let Some(status) = message.status {
                worker.slot_status = status;
            }
            if let Some((slot, new_state)) = message.slot_delta {
                worker.workers[slot] = new_state;
                if done
                    && !new_state
                    && worker.workers.iter().filter(|worker| **worker).count() == 0
                {
                    debug!("Exiting worker update thread after receiving message");
                    return;
                }
            }
            debug!(
                "Worker now at {:?}, {}/{} workers",
                worker.slot_status,
                worker.workers.iter().filter(|worker| **worker).count(),
                worker.workers.len()
            );
        }
    }
}

fn listen_for_local_workers<T: Pixel>(
    encode_opts: EncodeOptions,
    output_file: &Output,
    thread_pool: &mut ThreadPool,
    rayon_pool: Arc<rayon::ThreadPool>,
    video_info: VideoDetails,
    progress_channels: &[ProgressChannel],
    analyzer_receiver: AnalyzerReceiver,
) -> Result<()> {
    loop {
        match analyzer_receiver.try_recv() {
            Ok(Some(data)) => {
                let slot = data.slot;
                let segment_idx = data.segment_no + 1;
                encode_segment(
                    encode_opts,
                    video_info,
                    data,
                    thread_pool,
                    rayon_pool.clone(),
                    progress_channels[slot].0.clone(),
                    match output_file {
                        Output::File(output_file) => {
                            Output::File(get_segment_output_filename(output_file, segment_idx))
                        }
                        x => x.clone(),
                    },
                )?;
            }
            Ok(None) => {
                // No more input frames, finish.
                break;
            }
            Err(TryRecvError::Empty) => {
                sleep(Duration::from_millis(1000));
            }
            Err(e) => {
                debug!("{}", e);
                break;
            }
        }
    }
    Ok(())
}

fn listen_for_remote_workers(
    scope: &Scope,
    remote_analyzer_receiver: RemoteAnalyzerReceiver,
    input_finished_receiver: InputFinishedReceiver,
    remote_slots: Arc<Mutex<Vec<RemoteWorkerInfo>>>,
) {
    let mut threads = Vec::new();
    loop {
        if let Ok(message) = remote_analyzer_receiver.try_recv() {
            threads.push(scope.spawn(move |_| {
                if message.video_info.bit_depth <= 8 {
                    remote_encode_segment::<u8>(message)
                } else {
                    remote_encode_segment::<u16>(message)
                }
            }));
        }
        if input_finished_receiver.is_full()
            && remote_slots
                .lock()
                .iter()
                .map(|slot| slot.workers.iter())
                .flatten()
                .filter(|worker| **worker)
                .count()
                == 0
        {
            break;
        }
        sleep(Duration::from_millis(100));
    }
    for thread in threads {
        thread.join().unwrap();
    }
}

fn mux_output_files(out_filename: &Path, num_segments: usize) -> Result<()> {
    let mut out = BufWriter::new(File::create(out_filename)?);
    let segments =
        (0..=num_segments).map(|seg_idx| get_segment_output_filename(out_filename, seg_idx));
    let mut files = segments.clone();
    let header = files.next().unwrap();
    std::io::copy(&mut File::open(header)?, &mut out)?;

    let mut pts = 0;
    for seg_filename in files {
        let mut in_seg = BufReader::new(File::open(seg_filename)?);
        loop {
            match ivf::read_packet(&mut in_seg) {
                Ok(pkt) => {
                    ivf::write_ivf_frame(&mut out, pts, &pkt.data);
                    pts += 1;
                }
                Err(err) => match err.kind() {
                    std::io::ErrorKind::UnexpectedEof => break,
                    _ => return Err(err.into()),
                },
            }
        }
    }
    out.flush()?;

    // Allow the progress indicator thread
    // enough time to output the end-of-encode stats
    thread::sleep(Duration::from_secs(3));

    for segment in segments {
        let _ = remove_file(segment);
    }
    let _ = remove_file(get_progress_filename(out_filename));

    Ok(())
}
