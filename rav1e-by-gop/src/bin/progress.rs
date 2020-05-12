use crate::analyze::InputFinishedReceiver;
use clap::ArgMatches;
use console::Term;
use console::{style, StyledObject};
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use log::error;
use rav1e_by_gop::*;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{cmp, thread};

pub fn update_progress_file(output: &Path, progress: &ProgressInfo) {
    let data = rmp_serde::to_vec(&stats::SerializableProgressInfo::from(progress))
        .expect("Failed to serialize data");
    let mut progress_file =
        File::create(get_progress_filename(&output)).expect("Failed to open progress file");
    progress_file
        .write_all(&data)
        .expect("Failed to write to progress file");
}

pub fn get_segment_output_filename(output: &Path, segment_idx: usize) -> PathBuf {
    output.with_extension(&format!("part{}.ivf", segment_idx))
}

pub fn load_progress_file(outfile: &Path, matches: &ArgMatches) -> Option<ProgressInfo> {
    let term_err = Term::stderr();
    if get_progress_filename(outfile).is_file() {
        let resume = if matches.is_present("FORCE_RESUME") {
            true
        } else if matches.is_present("FORCE_OVERWRITE") {
            false
        } else if term_err.is_term() && matches.value_of("INPUT") != Some("-") {
            let resolved;
            loop {
                let input = dialoguer::Input::<String>::new()
                    .with_prompt(&format!(
                        "Found progress file for this encode. [{}]esume or [{}]verwrite?",
                        style("R").cyan(),
                        style("O").cyan()
                    ))
                    .interact()
                    .unwrap();
                match input.to_lowercase().as_str() {
                    "r" | "resume" => {
                        resolved = true;
                        break;
                    }
                    "o" | "overwrite" => {
                        resolved = false;
                        break;
                    }
                    _ => {
                        error!("Input not recognized");
                    }
                };
            }
            resolved
        } else {
            // Assume we want to resume if this is not a TTY
            // and no CLI option is given
            true
        };
        if resume {
            let progress_file =
                File::open(get_progress_filename(outfile)).expect("Failed to open progress file");
            let progress_input: SerializableProgressInfo = rmp_serde::from_read(&progress_file)
                .expect("Progress file did not contain valid JSON");
            Some(ProgressInfo::from(&progress_input))
        } else {
            None
        }
    } else {
        None
    }
}

pub fn get_progress_filename(output: &Path) -> PathBuf {
    output.with_extension("progress.data")
}

pub fn watch_progress_receivers(
    receivers: Vec<ProgressReceiver>,
    slots: Arc<Mutex<Vec<bool>>>,
    output_file: PathBuf,
    verbose: bool,
    mut overall_progress: ProgressInfo,
    input_finished_receiver: InputFinishedReceiver,
    display_progress: bool,
) {
    let segments_pb_holder = MultiProgress::new();
    if !display_progress {
        segments_pb_holder.set_draw_target(ProgressDrawTarget::hidden());
    }
    let main_pb = segments_pb_holder.add(ProgressBar::new_spinner());
    main_pb.set_style(main_progress_style());
    main_pb.set_prefix(&overall_prefix().to_string());
    main_pb.set_message(&overall_progress.progress_overall());
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

    let mut input_finished = false;
    loop {
        for (slot, rx) in receivers.iter().enumerate() {
            while let Ok(msg) = rx.try_recv() {
                let segment_idx = msg.as_ref().map(|msg| msg.segment_idx).unwrap_or(0);
                if update_progress(
                    msg,
                    &mut overall_progress,
                    segment_idx,
                    slots.clone(),
                    slot,
                    &segment_pbs[slot],
                ) {
                    update_overall_progress(&output_file, &overall_progress, &main_pb);
                }
            }
        }

        if !input_finished && input_finished_receiver.try_recv().is_ok() {
            input_finished = true;
        }
        if input_finished && slots.lock().unwrap().iter().all(|&slot| !slot) {
            // Done encoding
            main_pb.finish();
            for pb in &segment_pbs {
                pb.finish_and_clear();
            }
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }

    thread::sleep(Duration::from_millis(500));
    overall_progress.print_summary(verbose);
}

fn update_progress(
    progress: Option<ProgressInfo>,
    overall_progress: &mut ProgressInfo,
    segment_idx: usize,
    slots: Arc<Mutex<Vec<bool>>>,
    slot_idx: usize,
    pb: &ProgressBar,
) -> bool {
    if let Some(ref progress) = progress {
        if progress.frame_info.is_empty() {
            // New segment starting
            pb.set_style(progress_active_style());
            pb.set_prefix(&segment_prefix(segment_idx).to_string());
            pb.reset_elapsed();
            pb.set_position(0);
            pb.set_length(0);
            pb.set_length(progress.total_frames as u64);
            overall_progress
                .keyframes
                .insert(progress.keyframes.iter().next().copied().unwrap());
            overall_progress.next_analysis_frame = cmp::max(
                overall_progress.next_analysis_frame,
                progress.next_analysis_frame,
            );
            true
        } else if progress.total_frames == progress.frame_info.len() {
            // Segment complete
            overall_progress
                .frame_info
                .extend_from_slice(&progress.frame_info);
            overall_progress.total_frames += progress.total_frames;
            overall_progress.encoded_size += progress.encoded_size;
            overall_progress.completed_segments.insert(segment_idx);
            overall_progress.encoding_stats.0 += &progress.encoding_stats.0;
            overall_progress.encoding_stats.1 += &progress.encoding_stats.1;
            slots.lock().unwrap()[slot_idx] = false;

            pb.set_style(progress_idle_style());
            pb.set_prefix(&idle_prefix().to_string());
            pb.set_message("");
            pb.reset_elapsed();
            pb.set_length(0);
            true
        } else {
            // Normal tick
            pb.set_position(progress.frame_info.len() as u64);
            pb.set_message(&progress.progress());
            false
        }
    } else {
        // Skipped segment
        slots.lock().unwrap()[slot_idx] = false;
        pb.set_style(progress_idle_style());
        pb.set_prefix(&idle_prefix().to_string());
        pb.set_message("");
        pb.reset_elapsed();
        pb.set_length(0);
        false
    }
}

fn update_overall_progress(output_file: &Path, overall_progress: &ProgressInfo, pb: &ProgressBar) {
    update_progress_file(output_file, &overall_progress);

    pb.set_message(&overall_progress.progress_overall());
}

fn progress_idle_style() -> ProgressStyle {
    ProgressStyle::default_spinner().template("[{prefix}]")
}

fn main_progress_style() -> ProgressStyle {
    ProgressStyle::default_spinner().template("[{prefix}] [{elapsed_precise}] {wide_msg}")
}

fn progress_active_style() -> ProgressStyle {
    ProgressStyle::default_bar()
        .template(
            "[{prefix}] [{elapsed_precise}] {bar:24.blue/white.dim} {pos:>4}/{len:4} {wide_msg}",
        )
        .progress_chars("##-")
}

fn overall_prefix() -> StyledObject<&'static str> {
    style("Overall").blue().bold()
}

fn idle_prefix() -> StyledObject<&'static str> {
    style("Idle").cyan().dim()
}

fn segment_prefix(segment_idx: usize) -> StyledObject<String> {
    style(format!("Seg. {:>4}", segment_idx)).cyan()
}
