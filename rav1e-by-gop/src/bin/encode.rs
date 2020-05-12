use super::VideoDetails;
use crate::analyze::{run_first_pass, AnalyzerChannel, InputFinishedChannel};
use crate::decide_thread_count;
use crate::decode::*;
use crate::muxer::create_muxer;
use crate::progress::*;
use crate::CliOptions;
use anyhow::Result;
use console::style;
use crossbeam_channel::{bounded, unbounded, TryRecvError};
use crossbeam_utils::thread::{scope, Scope};
use log::info;
use rav1e::prelude::*;
use rav1e_by_gop::*;
use std::collections::BTreeSet;
use std::fs::remove_file;
use std::fs::File;
use std::io::{BufWriter, Read};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;
use std::{cmp, thread};
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

pub fn perform_encode_inner<T: Pixel, R: 'static + Read + Send>(
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

    let num_threads = decide_thread_count(opts, &video_info);
    let mut thread_pool = ThreadPool::new(num_threads);
    info!("Using {} encoder threads", style(num_threads).cyan());

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
        );

        // Do an initial write of the progress file,
        // so we don't need to redo keyframe search.
        update_progress_file(&opts.output, &progress);

        progress
    };

    let analyzer_channel: AnalyzerChannel<T> = unbounded();
    let progress_channels: Vec<ProgressChannel> = (0..num_threads).map(|_| unbounded()).collect();
    let input_finished_channel: InputFinishedChannel = bounded(1);
    let slots: Arc<Mutex<Vec<bool>>> = Arc::new(Mutex::new(vec![false; num_threads]));

    let output_file = opts.output.to_owned();
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
    let input_finished_receiver = input_finished_channel.1.clone();
    s.spawn(move |_| {
        watch_progress_receivers(
            receivers,
            slots_ref,
            output_file,
            verbose,
            overall_progress,
            input_finished_receiver,
            display_progress,
        );
    });

    let opts_ref = opts.clone();
    let analyzer_sender = analyzer_channel.0.clone();
    s.spawn(move |_| {
        run_first_pass(
            dec,
            opts_ref,
            analyzer_sender,
            slots,
            start_frameno,
            known_keyframes,
            skipped_segments,
        )
        .expect("An error occurred during input analysis");
    });

    // Write only the ivf header
    create_muxer(&get_segment_output_filename(&opts.output, 0))
        .map(|mut output| {
            output.write_header(
                video_info.width,
                video_info.height,
                video_info.time_base.den as usize,
                video_info.time_base.num as usize,
            );
        })
        .expect("Failed to create segment output");

    let mut num_segments = 0;
    let encode_opts = EncodeOptions::from(opts);
    let output_file = opts.output.to_owned();
    loop {
        match analyzer_channel.1.try_recv() {
            Ok(Some(data)) => {
                let slot = data.slot_no;
                let segment_idx = data.segment_no + 1;
                num_segments = cmp::max(num_segments, segment_idx);
                encode_segment(
                    &encode_opts,
                    video_info,
                    data,
                    &mut thread_pool,
                    progress_channels[slot].0.clone(),
                    get_segment_output_filename(&output_file, segment_idx),
                )?;
            }
            Ok(None) => {
                // No more input frames, finish.
                input_finished_channel.0.send(())?;
                break;
            }
            Err(TryRecvError::Empty) => {
                sleep(Duration::from_millis(1000));
            }
            Err(e) => {
                return Err(e.into());
            }
        }
    }
    thread_pool.join();

    mux_output_files(&opts.output, num_segments)?;

    Ok(())
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
        let mut in_seg = File::open(seg_filename)?;
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

    // Allow the progress indicator thread
    // enough time to output the end-of-encode stats
    thread::sleep(Duration::from_secs(3));

    for segment in segments {
        let _ = remove_file(segment);
    }
    let _ = remove_file(get_progress_filename(out_filename));

    Ok(())
}
