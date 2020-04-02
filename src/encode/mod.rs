mod progress;
pub mod stats;

use crate::analyze::{run_first_pass, AnalyzerChannel, InputFinishedChannel, SegmentData};
use crate::decode::*;
use crate::encode::progress::{watch_progress_receivers, ProgressChannel, ProgressSender};
use crate::encode::stats::{ProgressInfo, SerializableProgressInfo};
use crate::muxer::{create_muxer, Muxer};
use crate::CliOptions;
use clap::ArgMatches;
use console::{style, Term};
use crossbeam_channel::{bounded, unbounded, TryRecvError};
use crossbeam_utils::thread::{scope, Scope};
use rav1e::prelude::*;
use serde::export::Formatter;
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::Display;
use std::fs::remove_file;
use std::fs::File;
use std::io::{BufWriter, Read};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;
use std::{cmp, fmt, thread};
use systemstat::{ByteSize, Platform, System};
use threadpool::ThreadPool;
use y4m::Decoder;

pub fn perform_encode(
    keyframes: BTreeSet<usize>,
    next_analysis_frame: usize,
    opts: &CliOptions,
    progress: Option<ProgressInfo>,
) -> Result<(), Box<dyn Error>> {
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
) -> Result<(), Box<dyn Error>> {
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

    let num_threads = decide_thread_count(opts, &video_info);
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
    loop {
        match analyzer_channel.1.try_recv() {
            Ok(Some(data)) => {
                let slot = data.slot_no;
                num_segments = cmp::max(num_segments, data.segment_no + 1);
                encode_segment(
                    opts,
                    video_info,
                    data,
                    &mut thread_pool,
                    progress_channels[slot].0.clone(),
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

fn encode_segment<T: Pixel>(
    opts: &CliOptions,
    video_info: VideoDetails,
    data: SegmentData<T>,
    thread_pool: &mut ThreadPool,
    progress_sender: ProgressSender,
) -> Result<(), Box<dyn Error>> {
    let progress = ProgressInfo::new(
        Rational {
            num: video_info.time_base.den,
            den: video_info.time_base.num,
        },
        data.frames.len(),
        {
            let mut kf = BTreeSet::new();
            kf.insert(data.start_frameno);
            kf
        },
        data.segment_no + 1,
        data.next_analysis_frame,
    );
    let _ = progress_sender.send(Some(progress.clone()));

    let frames = data.frames.into_iter().map(Arc::new).collect::<Vec<_>>();

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
    let segment_idx = data.segment_no + 1;

    thread_pool.execute(move || {
        let source = Source {
            frames,
            sent_count: 0,
        };
        do_encode(
            cfg,
            source,
            output_file,
            segment_idx,
            progress,
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
                        eprintln!("Input not recognized");
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
            let progress_input: SerializableProgressInfo = serde_json::from_reader(progress_file)
                .expect("Progress file did not contain valid JSON");
            Some(ProgressInfo::from(&progress_input))
        } else {
            None
        }
    } else {
        None
    }
}

fn get_progress_filename(output: &Path) -> PathBuf {
    output.with_extension("progress.json")
}

fn do_encode<T: Pixel>(
    cfg: Config,
    mut source: Source<T>,
    output_file: PathBuf,
    segment_idx: usize,
    mut progress: ProgressInfo,
    progress_sender: ProgressSender,
) -> Result<ProgressInfo, Box<dyn Error>> {
    let mut ctx: Context<T> = cfg.new_context()?;
    let _ = progress_sender.send(Some(progress.clone()));

    let mut output = create_muxer(&get_segment_output_filename(&output_file, segment_idx))
        .expect("Failed to create segment output");

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

#[derive(Debug, Clone, Copy)]
pub enum MemoryUsage {
    Light,
    Heavy,
    Unlimited,
}

impl Default for MemoryUsage {
    fn default() -> Self {
        MemoryUsage::Light
    }
}

impl FromStr for MemoryUsage {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "light" => Ok(MemoryUsage::Light),
            "heavy" => Ok(MemoryUsage::Heavy),
            "unlimited" => Ok(MemoryUsage::Unlimited),
            _ => Err(()),
        }
    }
}

impl Display for MemoryUsage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                MemoryUsage::Light => "light",
                MemoryUsage::Heavy => "heavy",
                MemoryUsage::Unlimited => "unlimited",
            }
        )
    }
}

fn decide_thread_count(opts: &CliOptions, video_info: &VideoDetails) -> usize {
    // Limit to the number of logical CPUs.
    let mut num_threads = num_cpus::get();
    if let Some(max_threads) = opts.max_threads {
        num_threads = cmp::min(num_threads, max_threads);
    }

    // Limit further based on available memory.
    let sys = System::new();
    let sys_memory = sys.memory();
    if let Ok(sys_memory) = sys_memory {
        let bytes_per_frame = bytes_per_frame(video_info);
        // Conservatively account for encoding overhead
        let bytes_per_segment = opts.max_keyint * bytes_per_frame * 22 / 10;
        let total = sys_memory.total.as_u64();
        match opts.memory_usage {
            MemoryUsage::Light => {
                // Uses 50% of memory, minimum 2GB,
                // minimum unreserved memory of 2GB,
                // maximum unreserved memory of 12GB
                let unreserved = cmp::min(
                    ByteSize::gb(12).as_u64(),
                    cmp::max(ByteSize::gb(2).as_u64(), sys_memory.total.as_u64() / 2),
                );
                let limit = cmp::max(ByteSize::gb(2).as_u64(), total.saturating_sub(unreserved));
                num_threads = cmp::min(
                    num_threads,
                    cmp::max(1, (limit / bytes_per_segment) as usize),
                );
            }
            MemoryUsage::Heavy => {
                // Uses 80% of memory, minimum 2GB,
                // minimum unreserved memory of 1GB,
                // maximum unreserved memory of 6GB
                let unreserved = cmp::min(
                    ByteSize::gb(6).as_u64(),
                    cmp::max(ByteSize::gb(1).as_u64(), sys_memory.total.as_u64() * 4 / 5),
                );
                let limit = cmp::max(ByteSize::gb(2).as_u64(), total.saturating_sub(unreserved));
                num_threads = cmp::min(
                    num_threads,
                    cmp::max(1, (limit / bytes_per_segment) as usize),
                );
            }
            MemoryUsage::Unlimited => {
                // do nothing
            }
        }
    }

    num_threads
}

fn bytes_per_frame(video_info: &VideoDetails) -> u64 {
    let bytes_per_plane =
        video_info.width * video_info.height * if video_info.bit_depth > 8 { 2 } else { 1 };
    (bytes_per_plane as f32
        * match video_info.chroma_sampling {
            ChromaSampling::Cs420 => 1.5,
            ChromaSampling::Cs422 => 2.,
            ChromaSampling::Cs444 => 3.,
            ChromaSampling::Cs400 => 1.,
        }) as u64
}
