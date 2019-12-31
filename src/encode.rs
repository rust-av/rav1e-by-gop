use crate::decode::{Decoder, VideoDetails};
use crate::muxer::{create_muxer, Muxer};
use crate::CliOptions;
use console::Style;
use crossbeam_channel::{unbounded, Receiver, Sender};
use err_derive::Error;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rav1e::prelude::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
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
use std::time::Instant;
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
    thread::spawn(move || {
        watch_progress_receivers(receivers, slots_ref, output_file, overall_progress);
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

    while let Some(frame_info) = process_frame(&mut ctx, &mut source, &mut *output)? {
        for frame in frame_info {
            progress.add_frame(frame.clone());
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

type ProgressSender = Sender<Option<ProgressInfo>>;
type ProgressReceiver = Receiver<Option<ProgressInfo>>;
type ProgressChannel = (ProgressSender, ProgressReceiver);

#[derive(Debug, Clone)]
pub struct ProgressInfo {
    // Frame rate of the video
    pub frame_rate: Rational,
    // The length of the whole video, in frames
    pub total_frames: usize,
    // The time the encode was started
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
    pub segment_idx: usize,
}

impl ProgressInfo {
    pub fn new(
        frame_rate: Rational,
        total_frames: usize,
        keyframes: Vec<usize>,
        segment_idx: usize,
    ) -> Self {
        Self {
            frame_rate,
            total_frames,
            time_started: Instant::now(),
            frame_info: Vec::with_capacity(total_frames),
            encoded_size: 0,
            keyframes,
            completed_segments: Vec::new(),
            segment_idx,
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
        eprintln!("{}", self.end_of_encode_progress());
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

    fn progress(&self) -> String {
        format!(
            "{:.2} fps, {:.1} Kb/s, ETA {}",
            self.encoding_fps(),
            self.bitrate() as f64 / 1000f64,
            secs_to_human_time(self.estimated_time(), false)
        )
    }

    fn progress_verbose(&self) -> String {
        format!(
            "{:.2} fps, {:.1} Kb/s, est. {:.2} MB, {}",
            self.encoding_fps(),
            self.bitrate() as f64 / 1000f64,
            self.estimated_size() as f64 / (1024 * 1024) as f64,
            secs_to_human_time(self.estimated_time(), false)
        )
    }

    fn end_of_encode_progress(&self) -> String {
        format!(
            "Elapsed time: {}, {:.3} fps, {:.2} Kb/s, size: {:.2} MB",
            secs_to_human_time(self.elapsed_time() as u64, true),
            self.encoding_fps(),
            self.bitrate() as f64 / 1000f64,
            self.estimated_size() as f64 / (1024 * 1024) as f64,
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
    // Wall encoding time elapsed so far, in seconds
    #[serde(default)]
    elapsed_time: u64,
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
            elapsed_time: other.elapsed_time() as u64,
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
            time_started: Instant::now() - Duration::from_secs(other.elapsed_time),
            segment_idx: 0,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct FrameSummary {
    /// Frame size in bytes
    pub size: usize,
    pub frame_type: FrameType,
    /// QP selected for the frame.
    pub qp: u8,
}

impl<T: Pixel> From<Packet<T>> for FrameSummary {
    fn from(packet: Packet<T>) -> Self {
        Self {
            size: packet.data.len(),
            frame_type: packet.frame_type,
            qp: packet.qp,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SerializableFrameSummary {
    pub size: usize,
    pub frame_type: u8,
    pub qp: u8,
}

impl From<&FrameSummary> for SerializableFrameSummary {
    fn from(summary: &FrameSummary) -> Self {
        SerializableFrameSummary {
            size: summary.size,
            frame_type: summary.frame_type as u8,
            qp: summary.qp,
        }
    }
}

impl From<&SerializableFrameSummary> for FrameSummary {
    fn from(summary: &SerializableFrameSummary) -> Self {
        FrameSummary {
            size: summary.size,
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

fn watch_progress_receivers(
    receivers: Vec<ProgressReceiver>,
    slots: Arc<Mutex<Vec<bool>>>,
    output_file: PathBuf,
    mut overall_progress: ProgressInfo,
) {
    let segments_pb_holder = MultiProgress::new();
    let main_pb = segments_pb_holder.add(ProgressBar::new(overall_progress.total_frames as u64));
    main_pb.set_style(main_progress_style());
    main_pb.set_prefix(&Style::new().blue().apply_to("Overall").to_string());
    main_pb.set_position(0);
    let segment_pbs = (0..receivers.len())
        .map(|_| {
            let pb = segments_pb_holder.add(ProgressBar::new_spinner());
            pb.set_style(progress_idle_style());
            pb.set_prefix("Idle");
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

    overall_progress.print_summary();
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
            pb.set_prefix(&format!("{:>4}/{}", segment_idx, segment_count));
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
            pb.set_prefix("Idle");
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
        pb.set_prefix("Idle");
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

#[derive(Debug, Clone, Error)]
pub enum EncodeError {
    #[error(display = "Command '{}' failed to complete", _0)]
    CommandFailure(&'static str),
}

fn progress_idle_style() -> ProgressStyle {
    ProgressStyle::default_spinner().template("[{prefix}]")
}

fn main_progress_style() -> ProgressStyle {
    ProgressStyle::default_bar()
        .template("[{prefix}] [{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {wide_msg}")
        .progress_chars("##-")
}

fn progress_active_style() -> ProgressStyle {
    ProgressStyle::default_bar()
        .template("[{prefix}] [{elapsed_precise}] {bar:40} {pos:>4}/{len:4} {wide_msg}")
        .progress_chars("##-")
}
