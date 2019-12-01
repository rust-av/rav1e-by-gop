use crate::decode::{Decoder, VideoDetails};
use crate::muxer::{create_muxer, Muxer};
use crate::CliOptions;
use err_derive::Error;
use rav1e::prelude::*;
use std::cmp;
use std::error::Error;
use std::fmt;
use std::fs::remove_file;
use std::fs::File;
use std::io::stderr;
use std::io::BufWriter;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use std::time::Instant;
use termion::{clear, cursor, is_tty};
use threadpool::ThreadPool;

pub fn perform_encode(keyframes: &[usize], opts: &CliOptions) -> Result<(), Box<dyn Error>> {
    let mut reader = opts.input.as_reader()?;
    let mut dec = y4m::decode(&mut reader).expect("input is not a y4m file");
    let video_info = dec.get_video_details();
    eprintln!(
        "Using y4m decoder: {}x{}p @ {}/{} fps, {}, {}-bit",
        video_info.width,
        video_info.height,
        video_info.time_base.den,
        video_info.time_base.num,
        video_info.chroma_sampling,
        video_info.bit_depth
    );

    let mut num_threads = cmp::min(keyframes.len(), num_cpus::get());
    if let Some(max_threads) = opts.max_threads {
        num_threads = cmp::min(num_threads, max_threads);
    }
    let mut thread_pool = ThreadPool::new(num_threads);
    eprintln!("Using {} encoder threads", num_threads);

    let mut current_frameno = 0;
    let mut iter = keyframes.iter().enumerate().peekable();
    while let Some((idx, &keyframe)) = iter.next() {
        while thread_pool.active_count() + thread_pool.queued_count() == num_threads {
            // Loading frames costs a significant amount of memory,
            // so don't load frames until we're ready to encode them.
            sleep(Duration::from_millis(250));
        }
        let next_keyframe = iter.peek().map(|(_, next_fno)| **next_fno);
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
                keyframes.len(),
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
                keyframes.len(),
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
    segment_count: usize,
) -> Result<(), Box<dyn Error>> {
    let mut frames = Vec::with_capacity(next_keyframe.map(|next| next - keyframe).unwrap_or(0));
    while next_keyframe
        .map(|next| *current_frameno < next)
        .unwrap_or(true)
    {
        if let Ok(frame) = dec.read_frame::<T>(&video_info) {
            frames.push(frame);
            *current_frameno += 1;
        } else {
            break;
        }
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

    let out_filename = opts.output.to_string();
    eprintln!(
        "{}Segment {}/{}: Starting ({} frames)...",
        cursor::Goto(0, segment_idx as u16 + 4),
        segment_idx,
        segment_count,
        frames.len()
    );
    thread_pool.execute(move || {
        let mut output = create_muxer(&get_segment_output_filename(&out_filename, segment_idx))
            .expect("Failed to create segment output");
        do_encode(
            cfg,
            video_info,
            &mut *output,
            frames,
            segment_idx,
            segment_count,
        )
        .expect("Failed encoding segment");
    });
    Ok(())
}

fn get_segment_output_filename(output: &str, segment_idx: usize) -> String {
    Path::new(output)
        .with_extension(&format!("part{}.ivf", segment_idx))
        .to_string_lossy()
        .to_string()
}

fn get_segment_list_filename(output: &str) -> String {
    Path::new(output)
        .with_extension("segments.txt")
        .to_string_lossy()
        .to_string()
}

fn do_encode<T: Pixel>(
    cfg: Config,
    video_info: VideoDetails,
    output: &mut dyn Muxer,
    frames: Vec<Frame<T>>,
    segment_idx: usize,
    segment_count: usize,
) -> Result<(), Box<dyn Error>> {
    let mut ctx: Context<T> = cfg.new_context()?;
    let mut progress = ProgressInfo::new(
        Rational {
            num: video_info.time_base.den,
            den: video_info.time_base.num,
        },
        frames.len(),
    );

    for frame in frames {
        ctx.send_frame(Some(Arc::new(frame)))?;
    }
    ctx.flush();

    while let Some(frame_info) = process_frame(&mut ctx, output)? {
        for frame in frame_info {
            progress.add_frame(frame.clone());
        }
        if is_tty(&stderr()) {
            eprint!(
                "{}{}Segment {}/{}: {}",
                cursor::Goto(0, segment_idx as u16 + 4),
                clear::CurrentLine,
                segment_idx,
                segment_count,
                progress
            );
        }
        output.flush().unwrap();
    }
    if !is_tty(&stderr()) {
        eprint!("Segment {}/{}: {}", segment_idx, segment_count, progress);
    }
    Ok(())
}

fn process_frame<T: Pixel>(
    ctx: &mut Context<T>,
    output: &mut dyn Muxer,
) -> Result<Option<Vec<FrameSummary>>, Box<dyn Error>> {
    let mut frame_summaries = Vec::new();
    let pkt_wrapped = ctx.receive_packet();
    match pkt_wrapped {
        Ok(pkt) => {
            output.write_frame(pkt.input_frameno as u64, pkt.data.as_ref(), pkt.frame_type);
            frame_summaries.push(pkt.into());
        }
        Err(EncoderStatus::NeedMoreData) => {
            unreachable!();
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
    Ok(Some(frame_summaries))
}

#[derive(Debug, Clone)]
pub struct ProgressInfo {
    // Frame rate of the video
    frame_rate: Rational,
    // The length of the whole video, in frames
    total_frames: usize,
    // The time the encode was started
    time_started: Instant,
    // List of frames encoded so far
    frame_info: Vec<FrameSummary>,
    // Video size so far in bytes.
    //
    // This value will be updated in the CLI very frequently, so we cache the previous value
    // to reduce the overall complexity.
    encoded_size: usize,
}

impl ProgressInfo {
    pub fn new(frame_rate: Rational, total_frames: usize) -> Self {
        Self {
            frame_rate,
            total_frames,
            time_started: Instant::now(),
            frame_info: Vec::with_capacity(total_frames),
            encoded_size: 0,
        }
    }

    pub fn add_frame(&mut self, frame: FrameSummary) {
        self.encoded_size += frame.size;
        self.frame_info.push(frame);
    }

    pub fn frames_encoded(&self) -> usize {
        self.frame_info.len()
    }

    pub fn encoding_fps(&self) -> f64 {
        self.frame_info.len() as f64 / self.elapsed_time()
    }

    pub fn video_fps(&self) -> f64 {
        self.frame_rate.num as f64 / self.frame_rate.den as f64
    }

    // Returns the bitrate of the frames so far, in bits/second
    pub fn bitrate(&self) -> usize {
        let bits = self.encoded_size * 8;
        let seconds = self.frame_info.len() as f64 / self.video_fps();
        (bits as f64 / seconds) as usize
    }

    // Estimates the final filesize in bytes, if the number of frames is known
    pub fn estimated_size(&self) -> usize {
        self.encoded_size * self.total_frames / self.frames_encoded()
    }

    // Estimates the remaining encoding time in seconds, if the number of frames is known
    pub fn estimated_time(&self) -> u64 {
        ((self.total_frames - self.frames_encoded()) as f64 / self.encoding_fps()) as u64
    }

    pub fn elapsed_time(&self) -> f64 {
        let duration = Instant::now().duration_since(self.time_started);
        (duration.as_secs() as f64 + duration.subsec_millis() as f64 / 1000f64)
    }
}

impl fmt::Display for ProgressInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.frames_encoded() == self.total_frames {
            write!(
                f,
                "Done, {} frames in {}, {:.3} fps, {:.2} Kb/s, size: {:.2} MB",
                self.frames_encoded(),
                secs_to_human_time(self.elapsed_time().round() as u64),
                self.encoding_fps(),
                self.bitrate() as f64 / 1000f64,
                self.estimated_size() as f64 / (1024 * 1024) as f64,
            )
        } else {
            write!(
                f,
                "{}/{} frames, {:.3} fps, {:.2} Kb/s, est. size: {:.2} MB, est. time: {}",
                self.frames_encoded(),
                self.total_frames,
                self.encoding_fps(),
                self.bitrate() as f64 / 1000f64,
                self.estimated_size() as f64 / (1024 * 1024) as f64,
                secs_to_human_time(self.estimated_time())
            )
        }
    }
}

fn secs_to_human_time(mut secs: u64) -> String {
    let mut mins = secs / 60;
    secs %= 60;
    let hours = mins / 60;
    mins %= 60;
    if hours > 0 {
        format!("{}h {}m {}s", hours, mins, secs)
    } else if mins > 0 {
        format!("{}m {}s", mins, secs)
    } else {
        format!("{}s", secs)
    }
}

#[derive(Debug, Clone)]
pub struct FrameSummary {
    /// Frame size in bytes
    pub size: usize,
    pub input_frameno: u64,
    pub frame_type: FrameType,
}

impl<T: Pixel> From<Packet<T>> for FrameSummary {
    fn from(packet: Packet<T>) -> Self {
        Self {
            size: packet.data.len(),
            input_frameno: packet.input_frameno,
            frame_type: packet.frame_type,
        }
    }
}

fn mux_output_files(out_filename: &str, num_segments: usize) -> Result<(), Box<dyn Error>> {
    let segments = (1..=num_segments)
        .map(|seg_idx| get_segment_output_filename(out_filename, seg_idx))
        .collect::<Vec<_>>();
    let segments_filename = get_segment_list_filename(out_filename);
    {
        let segments_file = File::create(&segments_filename)?;
        let mut writer = BufWriter::new(&segments_file);
        for segment in &segments {
            writer.write_all(format!("file '{}'\n", segment).as_bytes())?;
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

    let _ = remove_file(segments_filename);
    for segment in segments {
        let _ = remove_file(segment);
    }
    Ok(())
}

#[derive(Debug, Clone, Error)]
pub enum EncodeError {
    #[error(display = "Command '{}' failed to complete", _0)]
    CommandFailure(&'static str),
}
