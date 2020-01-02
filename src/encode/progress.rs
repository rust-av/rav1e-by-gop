use crate::encode::stats::ProgressInfo;
use crate::encode::update_progress_file;
use console::{style, StyledObject};
use crossbeam_channel::{Receiver, Sender};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

pub type ProgressSender = Sender<Option<ProgressInfo>>;
pub type ProgressReceiver = Receiver<Option<ProgressInfo>>;
pub type ProgressChannel = (ProgressSender, ProgressReceiver);

pub fn watch_progress_receivers(
    receivers: Vec<ProgressReceiver>,
    slots: Arc<Mutex<Vec<bool>>>,
    output_file: PathBuf,
    verbose: bool,
    mut overall_progress: ProgressInfo,
) {
    let segments_pb_holder = MultiProgress::new();
    let main_pb = segments_pb_holder.add(ProgressBar::new(overall_progress.total_frames as u64));
    main_pb.set_style(main_progress_style());
    main_pb.set_prefix(&overall_prefix().to_string());
    main_pb.set_position(0);
    let segment_pbs = (0..receivers.len())
        .map(|_| {
            let pb = segments_pb_holder.add(ProgressBar::new_spinner());
            pb.set_style(progress_idle_style());
            pb.set_prefix(&idle_prefix().to_string());
            pb.set_position(0);
            pb.set_length(0);
            pb
        })
        .collect::<Vec<ProgressBar>>();
    thread::spawn(move || {
        segments_pb_holder.join_and_clear().unwrap();
    });

    let segment_count = overall_progress.keyframes.len();
    let mut last_segments_completed = 0;
    loop {
        for (slot, rx) in receivers.iter().enumerate() {
            while let Ok(msg) = rx.try_recv() {
                let segment_idx = msg.as_ref().map(|msg| msg.segment_idx).unwrap_or(0);
                update_progress(
                    msg,
                    &mut overall_progress,
                    segment_idx,
                    segment_count,
                    slots.clone(),
                    slot,
                    &segment_pbs[slot],
                );
            }
        }
        if overall_progress.completed_segments.len() != last_segments_completed {
            last_segments_completed = overall_progress.completed_segments.len();
            update_overall_progress(&output_file, &overall_progress, &main_pb);

            if overall_progress.completed_segments.len() == overall_progress.keyframes.len() {
                // Done encoding
                main_pb.finish();
                for pb in &segment_pbs {
                    pb.finish();
                }
                thread::sleep(Duration::from_millis(100));
                break;
            }
        }
        thread::sleep(Duration::from_millis(100));
    }

    overall_progress.print_summary(verbose);
}

fn update_progress(
    progress: Option<ProgressInfo>,
    overall_progress: &mut ProgressInfo,
    segment_idx: usize,
    segment_count: usize,
    slots: Arc<Mutex<Vec<bool>>>,
    slot_idx: usize,
    pb: &ProgressBar,
) {
    if let Some(ref progress) = progress {
        if progress.total_frames == 0 {
            // New segment starting
            pb.set_style(progress_active_style());
            pb.set_prefix(&segment_prefix(segment_idx, segment_count).to_string());
            pb.reset_elapsed();
            pb.set_position(0);
            pb.set_length(0);
        } else if progress.frame_info.is_empty() {
            // Frames loaded for new segment
            pb.set_length(progress.total_frames as u64);
        } else if progress.total_frames == progress.frame_info.len() {
            // Segment complete
            overall_progress
                .frame_info
                .extend_from_slice(&progress.frame_info);
            overall_progress.encoded_size += progress.encoded_size;
            overall_progress.completed_segments.push(segment_idx);
            slots.lock().unwrap()[slot_idx] = false;

            pb.set_style(progress_idle_style());
            pb.set_prefix(&idle_prefix().to_string());
            pb.set_message("");
            pb.reset_elapsed();
            pb.set_length(0);
        } else {
            // Normal tick
            pb.set_position(progress.frame_info.len() as u64);
            pb.set_message(&progress.progress());
        }
    } else {
        // Skipped segment
        slots.lock().unwrap()[slot_idx] = false;
        pb.set_style(progress_idle_style());
        pb.set_prefix(&idle_prefix().to_string());
        pb.set_message("");
        pb.reset_elapsed();
        pb.set_length(0);
    }
}

fn update_overall_progress(output_file: &Path, overall_progress: &ProgressInfo, pb: &ProgressBar) {
    update_progress_file(output_file, &overall_progress);

    pb.set_position(overall_progress.frame_info.len() as u64);
    pb.set_message(&overall_progress.progress_verbose());
}

fn progress_idle_style() -> ProgressStyle {
    ProgressStyle::default_spinner().template("[{prefix}]")
}

fn main_progress_style() -> ProgressStyle {
    ProgressStyle::default_bar()
        .template(
            "[{prefix}] [{elapsed_precise}] {bar:36.cyan/white.dim} {pos:>7}/{len:7} {wide_msg}",
        )
        .progress_chars("##-")
}

fn progress_active_style() -> ProgressStyle {
    ProgressStyle::default_bar()
        .template(
            "[{prefix}] [{elapsed_precise}] {bar:40.blue/white.dim} {pos:>4}/{len:4} {wide_msg}",
        )
        .progress_chars("##-")
}

fn overall_prefix() -> StyledObject<&'static str> {
    style("Overall").blue().bold()
}

fn idle_prefix() -> StyledObject<&'static str> {
    style("Idle").cyan().dim()
}

fn segment_prefix(segment_idx: usize, segment_count: usize) -> StyledObject<String> {
    style(format!("{:>4}/{}", segment_idx, segment_count)).cyan()
}
