use crate::decode::{Decoder, VideoDetails};
use crate::muxer::{create_muxer, Muxer};
use crate::CliOptions;
use console::Term;
use err_derive::Error;
use rav1e::prelude::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::cmp;
use std::error::Error;
use std::fmt;
use std::fs::remove_file;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::path::{Path, PathBuf};
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
    progress: Option<ProgressInfo>,
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
        if let Some(progress) = progress {
            progress
        } else {
            let progress = ProgressInfo::new(
                Rational {
                    num: video_info.time_base.den,
                    den: video_info.time_base.num,
                },
                total_frames,
                keyframes.to_vec(),
            );

            // Do an initial write of the progress file,
            // so we don't need to redo keyframe search.
            update_progress_file(opts.output, &progress);

            progress
        },
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
                pool_lock.1.completed_segments.contains(&(idx + 1)),
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
                pool_lock.1.completed_segments.contains(&(idx + 1)),
            )?;
        }
    }
    thread_pool.join();
    let term_err = Term::stderr();
    if term_err.is_term() {
        term_err.move_cursor_to(0, 5).unwrap();
    }
    let pool_lock = progress_pool.lock().unwrap();
    pool_lock.1.print_summary();
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
    skip: bool,
) -> Result<(), Box<dyn Error>> {
    let mut term_err = Term::stderr();
    if term_err.is_term() {
        term_err
            .move_cursor_to(0, (progress_slot as u16 + 5) as usize)
            .unwrap();
        let _ = writeln!(
            term_err,
            "[00:00:00] Segment {}/{}: Starting...",
            segment_idx, segment_count,
        );
    }

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

    if term_err.is_term() {
        term_err
            .move_cursor_to(0, (progress_slot as u16 + 5) as usize)
            .unwrap();
        let _ = writeln!(
            term_err,
            "[00:00:00] Segment {}/{}: Starting ({} frames)...",
            segment_idx,
            segment_count,
            frames.len()
        );
    }

    let output_file = opts.output.to_owned();

    thread_pool.execute(move || {
        let source = Source {
            frames,
            sent_count: 0,
        };
        let mut output = create_muxer(&get_segment_output_filename(&output_file, segment_idx))
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
        total_progress.completed_segments.push(segment_idx);

        update_progress_file(&output_file, &*total_progress);

        if term_err.is_term() {
            term_err.move_cursor_to(0, 4).unwrap();
            term_err.clear_line().unwrap();
            let _ = writeln!(
                term_err,
                "[{}] Progress: {}",
                secs_to_human_time(total_progress.elapsed_time() as u64, true),
                total_progress
            );
        }
    });
    Ok(())
}

fn update_progress_file(output: &Path, progress: &ProgressInfo) {
    let progress_file =
        File::create(get_progress_filename(&output)).expect("Failed to open progress file");
    serde_json::to_writer(progress_file, &SerializableProgressInfo::from(progress))
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
        // Don't care about keyframes for the per-segment progress info
        Vec::new(),
    );

    output.write_header(
        video_info.width,
        video_info.height,
        cfg.enc.time_base.den as usize,
        cfg.enc.time_base.num as usize,
    );

    let mut term_err = Term::stderr();

    while let Some(frame_info) = process_frame(&mut ctx, &mut source, output)? {
        for frame in frame_info {
            progress.add_frame(frame.clone());
        }
        if term_err.is_term() && progress.frames_encoded() > 0 {
            term_err
                .move_cursor_to(0, (progress_slot as u16 + 5) as usize)
                .unwrap();
            term_err.clear_line().unwrap();
            let _ = write!(
                term_err,
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
        let _ = write!(
            term_err,
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
    pub frame_rate: Rational,
    // The length of the whole video, in frames
    pub total_frames: usize,
    // The time the encode was started
    // FIXME: This will be broken for resumed encodes
    pub time_started: Instant,
    // List of frames encoded so far
    pub frame_info: Vec<FrameSummary>,
    // Video size so far in bytes.
    //
    // This value will be updated in the CLI very frequently, so we cache the previous value
    // to reduce the overall complexity.
    pub encoded_size: usize,
    // The below are used for resume functionality
    pub keyframes: Vec<usize>,
    pub completed_segments: Vec<usize>,
}

impl ProgressInfo {
    pub fn new(frame_rate: Rational, total_frames: usize, keyframes: Vec<usize>) -> Self {
        Self {
            frame_rate,
            total_frames,
            time_started: Instant::now(),
            frame_info: Vec::with_capacity(total_frames),
            encoded_size: 0,
            keyframes,
            completed_segments: Vec::new(),
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

    // Number of frames of given type which appear in the video
    fn get_frame_type_count(&self, frame_type: FrameType) -> usize {
        self.frame_info
            .iter()
            .filter(|frame| frame.frame_type == frame_type)
            .count()
    }

    fn get_frame_type_avg_size(&self, frame_type: FrameType) -> usize {
        let count = self.get_frame_type_count(frame_type);
        if count == 0 {
            return 0;
        }
        self.frame_info
            .iter()
            .filter(|frame| frame.frame_type == frame_type)
            .map(|frame| frame.size)
            .sum::<usize>()
            / count
    }

    fn get_frame_type_avg_qp(&self, frame_type: FrameType) -> f32 {
        let count = self.get_frame_type_count(frame_type);
        if count == 0 {
            return 0.;
        }
        self.frame_info
            .iter()
            .filter(|frame| frame.frame_type == frame_type)
            .map(|frame| frame.qp as f32)
            .sum::<f32>()
            / count as f32
    }

    pub fn print_summary(&self) {
        self.print_frame_type_summary(FrameType::KEY);
        self.print_frame_type_summary(FrameType::INTER);
        self.print_frame_type_summary(FrameType::INTRA_ONLY);
        self.print_frame_type_summary(FrameType::SWITCH);
    }

    fn print_frame_type_summary(&self, frame_type: FrameType) {
        let count = self.get_frame_type_count(frame_type);
        let size = self.get_frame_type_avg_size(frame_type);
        let avg_qp = self.get_frame_type_avg_qp(frame_type);
        eprintln!(
            "{:17} {:>6} | avg QP: {:6.2} | avg size: {:>7} B",
            format!("{}:", frame_type),
            count,
            avg_qp,
            size
        );
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableProgressInfo {
    frame_rate: (u64, u64),
    total_frames: usize,
    frame_info: Vec<SerializableFrameSummary>,
    encoded_size: usize,
    keyframes: Vec<usize>,
    completed_segments: Vec<usize>,
}

impl From<&ProgressInfo> for SerializableProgressInfo {
    fn from(other: &ProgressInfo) -> Self {
        SerializableProgressInfo {
            frame_rate: (other.frame_rate.num, other.frame_rate.den),
            total_frames: other.total_frames,
            frame_info: other
                .frame_info
                .iter()
                .map(SerializableFrameSummary::from)
                .collect(),
            encoded_size: other.encoded_size,
            keyframes: other.keyframes.clone(),
            completed_segments: other.completed_segments.clone(),
        }
    }
}

impl From<&SerializableProgressInfo> for ProgressInfo {
    fn from(other: &SerializableProgressInfo) -> Self {
        ProgressInfo {
            frame_rate: Rational::new(other.frame_rate.0, other.frame_rate.1),
            total_frames: other.total_frames,
            frame_info: other.frame_info.iter().map(FrameSummary::from).collect(),
            encoded_size: other.encoded_size,
            keyframes: other.keyframes.clone(),
            completed_segments: other.completed_segments.clone(),
            time_started: Instant::now(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct FrameSummary {
    /// Frame size in bytes
    pub size: usize,
    pub input_frameno: u64,
    pub frame_type: FrameType,
    /// QP selected for the frame.
    pub qp: u8,
}

impl<T: Pixel> From<Packet<T>> for FrameSummary {
    fn from(packet: Packet<T>) -> Self {
        Self {
            size: packet.data.len(),
            input_frameno: packet.input_frameno,
            frame_type: packet.frame_type,
            qp: packet.qp,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SerializableFrameSummary {
    pub size: usize,
    pub input_frameno: u64,
    pub frame_type: u8,
    pub qp: u8,
}

impl From<&FrameSummary> for SerializableFrameSummary {
    fn from(summary: &FrameSummary) -> Self {
        SerializableFrameSummary {
            size: summary.size,
            input_frameno: summary.input_frameno,
            frame_type: summary.frame_type as u8,
            qp: summary.qp,
        }
    }
}

impl From<&SerializableFrameSummary> for FrameSummary {
    fn from(summary: &SerializableFrameSummary) -> Self {
        FrameSummary {
            size: summary.size,
            input_frameno: summary.input_frameno,
            frame_type: match summary.frame_type {
                0 => FrameType::KEY,
                1 => FrameType::INTER,
                2 => FrameType::INTRA_ONLY,
                3 => FrameType::SWITCH,
                _ => unreachable!(),
            },
            qp: summary.qp,
        }
    }
}

impl Serialize for FrameSummary {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let ser = SerializableFrameSummary::from(self);
        ser.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for FrameSummary {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let de = SerializableFrameSummary::deserialize(deserializer)?;
        Ok(FrameSummary::from(&de))
    }
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
