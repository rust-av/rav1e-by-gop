mod progress;
pub mod stats;

use crate::decode::{Decoder, VideoDetails};
use crate::encode::progress::{watch_progress_receivers, ProgressChannel, ProgressSender};
use crate::encode::stats::ProgressInfo;
use crate::muxer::{create_muxer, Muxer};
use crate::CliOptions;
use console::style;
use crossbeam_channel::unbounded;
use err_derive::Error;
use rav1e::prelude::*;
use std::error::Error;
use std::fs::remove_file;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;
use std::{cmp, thread};
use threadpool::ThreadPool;

pub fn perform_encode(
    keyframes: &[usize],
    total_frames: usize,
    opts: &CliOptions,
    progress: Option<ProgressInfo>,
) -> Result<(), Box<dyn Error>> {
    let mut reader = opts.input.as_reader()?;
    let mut dec = y4m::decode(&mut reader).expect("input is not a y4m file");
    let video_info = dec.get_video_details();
    eprintln!(
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

    let mut num_threads = cmp::min(keyframes.len(), num_cpus::get());
    if let Some(max_threads) = opts.max_threads {
        num_threads = cmp::min(num_threads, max_threads);
    }
    let mut thread_pool = ThreadPool::new(num_threads);
    eprintln!("Using {} encoder threads", style(num_threads).cyan());

    let overall_progress = if let Some(progress) = progress {
        progress
    } else {
        let progress = ProgressInfo::new(
            Rational {
                num: video_info.time_base.den,
                den: video_info.time_base.num,
            },
            total_frames,
            keyframes.to_vec(),
            0,
        );

        // Do an initial write of the progress file,
        // so we don't need to redo keyframe search.
        update_progress_file(opts.output, &progress);

        progress
    };
    let channels: Vec<ProgressChannel> = (0..num_threads).map(|_| unbounded()).collect();
    let slots: Arc<Mutex<Vec<bool>>> = Arc::new(Mutex::new(vec![false; num_threads]));

    let keyframes = overall_progress.keyframes.clone();
    let skipped_segments = overall_progress.completed_segments.clone();
    let output_file = opts.output.to_owned();
    let receivers = channels
        .iter()
        .map(|(_, rx)| rx.clone())
        .collect::<Vec<_>>();
    let slots_ref = slots.clone();
    let verbose = opts.verbose;
    thread::spawn(move || {
        watch_progress_receivers(receivers, slots_ref, output_file, verbose, overall_progress);
    });

    let mut current_frameno = 0;
    let mut iter = keyframes.iter().enumerate().peekable();
    while let Some((idx, &keyframe)) = iter.next() {
        let mut open_slot;
        loop {
            // Loading frames costs a significant amount of memory,
            // so don't load frames until we're ready to encode them.
            open_slot = slots.lock().unwrap().iter().position(|slot| !*slot);
            if open_slot.is_some() {
                break;
            }

            sleep(Duration::from_millis(250));
        }
        let next_keyframe = iter.peek().map(|(_, next_fno)| **next_fno);

        let slot = open_slot.unwrap();
        slots.lock().unwrap()[slot] = true;
        let skip = skipped_segments.contains(&(idx + 1));

        if video_info.bit_depth == 8 {
            encode_segment::<u8, _>(
                &mut dec,
                opts,
                video_info,
                keyframe,
                next_keyframe,
                &mut current_frameno,
                &mut thread_pool,
                idx + 1,
                channels[slot].0.clone(),
                skip,
            )?;
        } else {
            encode_segment::<u16, _>(
                &mut dec,
                opts,
                video_info,
                keyframe,
                next_keyframe,
                &mut current_frameno,
                &mut thread_pool,
                idx + 1,
                channels[slot].0.clone(),
                skip,
            )?;
        }
    }
    thread_pool.join();

    mux_output_files(opts.output, keyframes.len())?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn encode_segment<T: Pixel, D: Decoder>(
    dec: &mut D,
    opts: &CliOptions,
    video_info: VideoDetails,
    keyframe: usize,
    next_keyframe: Option<usize>,
    current_frameno: &mut usize,
    thread_pool: &mut ThreadPool,
    segment_idx: usize,
    progress_sender: ProgressSender,
    skip: bool,
) -> Result<(), Box<dyn Error>> {
    let _ = progress_sender.send(Some(ProgressInfo::new(
        Rational {
            num: video_info.time_base.den,
            den: video_info.time_base.num,
        },
        0,
        vec![],
        segment_idx,
    )));

    let mut frames = Vec::with_capacity(next_keyframe.map(|next| next - keyframe).unwrap_or(0));
    while next_keyframe
        .map(|next| *current_frameno < next)
        .unwrap_or(true)
    {
        if let Ok(frame) = dec.read_frame::<T>(&video_info) {
            frames.push(Arc::new(frame));
            *current_frameno += 1;
        } else {
            break;
        }
    }

    if skip {
        let _ = progress_sender.send(None);
        return Ok(());
    }

    let mut cfg = Config {
        enc: EncoderConfig::with_speed_preset(opts.speed),
        threads: 1,
    };
    cfg.enc.width = video_info.width;
    cfg.enc.height = video_info.height;
    cfg.enc.bit_depth = video_info.bit_depth;
    cfg.enc.chroma_sampling = video_info.chroma_sampling;
    cfg.enc.chroma_sample_position = video_info.chroma_sample_position;
    cfg.enc.time_base = video_info.time_base;
    cfg.enc.min_key_frame_interval = opts.min_keyint;
    cfg.enc.max_key_frame_interval = opts.max_keyint;
    cfg.enc.quantizer = opts.qp;
    cfg.enc.tiles = 1;
    cfg.enc.speed_settings.no_scene_detection = true;

    let output_file = opts.output.to_owned();

    thread_pool.execute(move || {
        let source = Source {
            frames,
            sent_count: 0,
        };
        do_encode(
            cfg,
            video_info,
            source,
            output_file,
            segment_idx,
            progress_sender,
        )
        .expect("Failed encoding segment");
    });
    Ok(())
}

fn update_progress_file(output: &Path, progress: &ProgressInfo) {
    let progress_file =
        File::create(get_progress_filename(&output)).expect("Failed to open progress file");
    serde_json::to_writer(
        progress_file,
        &stats::SerializableProgressInfo::from(progress),
    )
    .expect("Failed to write to progress file");
}

fn get_segment_output_filename(output: &Path, segment_idx: usize) -> PathBuf {
    output.with_extension(&format!("part{}.ivf", segment_idx))
}

fn get_segment_list_filename(output: &Path) -> PathBuf {
    output.with_extension("segments.txt")
}

pub fn get_progress_filename(output: &Path) -> PathBuf {
    output.with_extension("progress.json")
}

fn do_encode<T: Pixel>(
    cfg: Config,
    video_info: VideoDetails,
    mut source: Source<T>,
    output_file: PathBuf,
    segment_idx: usize,
    progress_sender: ProgressSender,
) -> Result<ProgressInfo, Box<dyn Error>> {
    let mut ctx: Context<T> = cfg.new_context()?;
    let mut progress = ProgressInfo::new(
        Rational {
            num: video_info.time_base.den,
            den: video_info.time_base.num,
        },
        source.frames.len(),
        // Don't care about keyframes for the per-segment progress info
        Vec::new(),
        segment_idx,
    );
    let _ = progress_sender.send(Some(progress.clone()));

    let mut output = create_muxer(&get_segment_output_filename(&output_file, segment_idx))
        .expect("Failed to create segment output");
    output.write_header(
        video_info.width,
        video_info.height,
        cfg.enc.time_base.den as usize,
        cfg.enc.time_base.num as usize,
    );

    while let Some(packets) = process_frame(&mut ctx, &mut source, &mut *output)? {
        for packet in packets {
            progress.add_packet(packet);
        }
        let _ = progress_sender.send(Some(progress.clone()));
        output.flush().unwrap();
    }

    Ok(progress)
}

struct Source<T: Pixel> {
    sent_count: usize,
    frames: Vec<Arc<Frame<T>>>,
}

impl<T: Pixel> Source<T> {
    fn read_frame(&mut self, ctx: &mut Context<T>) {
        if self.sent_count == self.frames.len() {
            ctx.flush();
            return;
        }

        let _ = ctx.send_frame(Some(self.frames[self.sent_count].clone()));
        self.sent_count += 1;
    }
}

fn process_frame<T: Pixel>(
    ctx: &mut Context<T>,
    source: &mut Source<T>,
    output: &mut dyn Muxer,
) -> Result<Option<Vec<Packet<T>>>, Box<dyn Error>> {
    let mut packets = Vec::new();
    let pkt_wrapped = ctx.receive_packet();
    match pkt_wrapped {
        Ok(pkt) => {
            output.write_frame(pkt.input_frameno as u64, pkt.data.as_ref(), pkt.frame_type);
            packets.push(pkt);
        }
        Err(EncoderStatus::NeedMoreData) => {
            source.read_frame(ctx);
        }
        Err(EncoderStatus::EnoughData) => {
            unreachable!();
        }
        Err(EncoderStatus::LimitReached) => {
            return Ok(None);
        }
        e @ Err(EncoderStatus::Failure) => {
            e?;
        }
        Err(EncoderStatus::NotReady) => {
            unreachable!();
        }
        Err(EncoderStatus::Encoded) => {}
    }
    Ok(Some(packets))
}

fn mux_output_files(out_filename: &Path, num_segments: usize) -> Result<(), Box<dyn Error>> {
    let segments = (1..=num_segments)
        .map(|seg_idx| get_segment_output_filename(out_filename, seg_idx))
        .collect::<Vec<_>>();
    let segments_filename = get_segment_list_filename(out_filename);
    {
        let segments_file = File::create(&segments_filename)?;
        let mut writer = BufWriter::new(&segments_file);
        for segment in &segments {
            writer.write_all(format!("file '{}'\n", segment.to_str().unwrap()).as_bytes())?;
        }
        segments_file.sync_all()?;
    }

    let result = Command::new("ffmpeg")
        .arg("-y")
        .arg("-f")
        .arg("concat")
        .arg("-safe")
        .arg("0")
        .arg("-i")
        .arg(&segments_filename)
        .arg("-c")
        .arg("copy")
        .arg(out_filename)
        .stderr(Stdio::null())
        .status()?;
    if !result.success() {
        return Err(Box::new(EncodeError::CommandFailure("ffmpeg")));
    }

    // Allow the progress indicator thread
    // enough time to output the end-of-encode stats
    thread::sleep(Duration::from_secs(5));

    let _ = remove_file(segments_filename);
    for segment in segments {
        let _ = remove_file(segment);
    }
    let _ = remove_file(get_progress_filename(out_filename));

    Ok(())
}

#[derive(Debug, Clone, Error)]
pub enum EncodeError {
    #[error(display = "Command '{}' failed to complete", _0)]
    CommandFailure(&'static str),
}
