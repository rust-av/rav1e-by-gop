use crate::decode::{Decoder, VideoDetails};
use crate::muxer::{create_muxer, Muxer};
use crate::CliOptions;
use console::Term;
use err_derive::Error;
use rav1e::prelude::*;
use std::cmp;
use std::error::Error;
use std::fmt;
use std::fs::remove_file;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;
use std::time::Instant;
use threadpool::ThreadPool;

pub fn perform_encode(
    keyframes: &[usize],
    total_frames: usize,
    opts: &CliOptions,
) -> Result<(), Box<dyn Error>> {
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
    eprintln!("[00:00:00] Starting encode...");

    let progress_pool = Arc::new(Mutex::new((
        vec![false; num_threads],
        ProgressInfo::new(
            Rational {
                num: video_info.time_base.den,
                den: video_info.time_base.num,
            },
            total_frames,
        ),
    )));
    let mut current_frameno = 0;
    let mut iter = keyframes.iter().enumerate().peekable();
    while let Some((idx, &keyframe)) = iter.next() {
        while thread_pool.active_count() + thread_pool.queued_count() == num_threads {
            // Loading frames costs a significant amount of memory,
            // so don't load frames until we're ready to encode them.
            sleep(Duration::from_millis(250));
        }
        let next_keyframe = iter.peek().map(|(_, next_fno)| **next_fno);

        let mut pool_lock = progress_pool.lock().unwrap();
        let progress_slot = pool_lock.0.iter().position(|slot| !*slot).unwrap();
        pool_lock.0[progress_slot] = true;

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
                progress_slot,
                progress_pool.clone(),
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
                progress_slot,
                progress_pool.clone(),
            )?;
        }
    }
    thread_pool.join();
    let term_err = Term::stderr();
    if term_err.is_term() {
        term_err.move_cursor_to(0, 5).unwrap();
    }
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
    progress_slot: usize,
    progress_pool: Arc<Mutex<(Vec<bool>, ProgressInfo)>>,
) -> Result<(), Box<dyn Error>> {
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
    let term_err = Term::stderr();
    if term_err.is_term() {
        term_err
            .move_cursor_to(0, (progress_slot as u16 + 5) as usize)
            .unwrap();
        eprintln!(
            "[00:00:00] Segment {}/{}: Starting ({} frames)...",
            segment_idx,
            segment_count,
            frames.len()
        );
    }

    thread_pool.execute(move || {
        let source = Source {
            frames,
            sent_count: 0,
        };
        let mut output = create_muxer(&get_segment_output_filename(&out_filename, segment_idx))
            .expect("Failed to create segment output");
        let progress = do_encode(
            cfg,
            video_info,
            source,
            &mut *output,
            segment_idx,
            segment_count,
            progress_slot,
        )
        .expect("Failed encoding segment");
        let mut pool_lock = progress_pool.lock().unwrap();
        pool_lock.0[progress_slot] = false;
        let total_progress = &mut pool_lock.1;
        total_progress
            .frame_info
            .extend_from_slice(&progress.frame_info);
        total_progress.encoded_size += progress.encoded_size;

        if term_err.is_term() {
            term_err.move_cursor_to(0, 4).unwrap();
            term_err.clear_line().unwrap();
            eprintln!(
                "[{}] Progress: {}",
                secs_to_human_time(total_progress.elapsed_time() as u64, true),
                total_progress
            );
        }
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
    mut source: Source<T>,
    output: &mut dyn Muxer,
    segment_idx: usize,
    segment_count: usize,
    progress_slot: usize,
) -> Result<ProgressInfo, Box<dyn Error>> {
    let mut ctx: Context<T> = cfg.new_context()?;
    let mut progress = ProgressInfo::new(
        Rational {
            num: video_info.time_base.den,
            den: video_info.time_base.num,
        },
        source.frames.len(),
    );

    output.write_header(
        video_info.width,
        video_info.height,
        cfg.enc.time_base.den as usize,
        cfg.enc.time_base.num as usize,
    );

    let term_err = Term::stderr();

    while let Some(frame_info) = process_frame(&mut ctx, &mut source, output)? {
        for frame in frame_info {
            progress.add_frame(frame.clone());
        }
        if term_err.is_term() && progress.frames_encoded() > 0 {
            term_err
                .move_cursor_to(0, (progress_slot as u16 + 5) as usize)
                .unwrap();
            term_err.clear_line().unwrap();
            eprint!(
                "[{}] Segment {}/{}: {}",
                secs_to_human_time(progress.elapsed_time() as u64, true),
                segment_idx,
                segment_count,
                progress
            );
        }
        output.flush().unwrap();
    }
    if !term_err.is_term() {
        eprint!(
            "[{}] Segment {}/{}: {}",
            secs_to_human_time(progress.elapsed_time() as u64, true),
            segment_idx,
            segment_count,
            progress
        );
    } else {
        term_err
            .move_cursor_to(0, (progress_slot as u16 + 5) as usize)
            .unwrap();
        term_err.clear_line().unwrap();
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
) -> Result<Option<Vec<FrameSummary>>, Box<dyn Error>> {
    let mut frame_summaries = Vec::new();
    let pkt_wrapped = ctx.receive_packet();
    match pkt_wrapped {
        Ok(pkt) => {
            output.write_frame(pkt.input_frameno as u64, pkt.data.as_ref(), pkt.frame_type);
            frame_summaries.push(pkt.into());
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
        write!(
            f,
            "{}/{} frames, {:.3} fps, {:.2} Kb/s, est. size: {:.2} MB, est. time: {}",
            self.frames_encoded(),
            self.total_frames,
            self.encoding_fps(),
            self.bitrate() as f64 / 1000f64,
            self.estimated_size() as f64 / (1024 * 1024) as f64,
            secs_to_human_time(self.estimated_time(), false)
        )
    }
}

fn secs_to_human_time(mut secs: u64, always_show_hours: bool) -> String {
    let mut mins = secs / 60;
    secs %= 60;
    let hours = mins / 60;
    mins %= 60;
    if hours > 0 || always_show_hours {
        format!("{:02}:{:02}:{:02}", hours, mins, secs)
    } else {
        format!("{:02}:{:02}", mins, secs)
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
