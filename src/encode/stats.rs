use console::style;
use rav1e::data::EncoderStats;
use rav1e::prelude::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

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
    // Summarized verbose encoding stats, split into I and P frames
    pub encoding_stats: (EncoderStats, EncoderStats),
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
            encoding_stats: (EncoderStats::default(), EncoderStats::default()),
        }
    }

    pub fn add_packet<T: Pixel>(&mut self, packet: Packet<T>) {
        self.encoded_size += packet.data.len();
        match packet.frame_type {
            FrameType::KEY => self.encoding_stats.0 += &packet.enc_stats,
            _ => self.encoding_stats.1 += &packet.enc_stats,
        };
        self.frame_info.push(packet.into());
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

    fn get_block_count_by_frame_type(&self, frame_type: FrameType) -> usize {
        match frame_type {
            FrameType::KEY => self
                .encoding_stats
                .0
                .block_size_counts
                .values()
                .sum::<usize>(),
            FrameType::INTER => self
                .encoding_stats
                .1
                .block_size_counts
                .values()
                .sum::<usize>(),
            _ => unreachable!(),
        }
    }

    fn get_tx_count_by_frame_type(&self, frame_type: FrameType) -> usize {
        match frame_type {
            FrameType::KEY => self.encoding_stats.0.tx_type_counts.values().sum::<usize>(),
            FrameType::INTER => self.encoding_stats.1.tx_type_counts.values().sum::<usize>(),
            _ => unreachable!(),
        }
    }

    fn get_bsize_pct_by_frame_type(&self, bsize: BlockSize, frame_type: FrameType) -> f32 {
        let count = self.get_block_count_by_frame_type(frame_type);
        if count == 0 {
            return 0.;
        }
        (match frame_type {
            FrameType::KEY => self
                .encoding_stats
                .0
                .block_size_counts
                .get(&bsize)
                .copied()
                .unwrap_or(0),
            FrameType::INTER => self
                .encoding_stats
                .1
                .block_size_counts
                .get(&bsize)
                .copied()
                .unwrap_or(0),
            _ => unreachable!(),
        }) as f32
            / count as f32
            * 100.
    }

    fn get_skip_pct_by_frame_type(&self, frame_type: FrameType) -> f32 {
        let count = self.get_block_count_by_frame_type(frame_type);
        if count == 0 {
            return 0.;
        }
        (match frame_type {
            FrameType::KEY => self.encoding_stats.0.skip_block_count,
            FrameType::INTER => self.encoding_stats.1.skip_block_count,
            _ => unreachable!(),
        }) as f32
            / count as f32
            * 100.
    }

    fn get_txtype_pct_by_frame_type(&self, tx_type: TxType, frame_type: FrameType) -> f32 {
        let count = self.get_tx_count_by_frame_type(frame_type);
        if count == 0 {
            return 0.;
        }
        (match frame_type {
            FrameType::KEY => self
                .encoding_stats
                .0
                .tx_type_counts
                .get(&tx_type)
                .copied()
                .unwrap_or(0),
            FrameType::INTER => self
                .encoding_stats
                .1
                .tx_type_counts
                .get(&tx_type)
                .copied()
                .unwrap_or(0),
            _ => unreachable!(),
        }) as f32
            / count as f32
            * 100.
    }

    fn get_luma_pred_count_by_frame_type(&self, frame_type: FrameType) -> usize {
        match frame_type {
            FrameType::KEY => self
                .encoding_stats
                .0
                .luma_pred_mode_counts
                .values()
                .sum::<usize>(),
            FrameType::INTER => self
                .encoding_stats
                .1
                .luma_pred_mode_counts
                .values()
                .sum::<usize>(),
            _ => unreachable!(),
        }
    }

    fn get_chroma_pred_count_by_frame_type(&self, frame_type: FrameType) -> usize {
        match frame_type {
            FrameType::KEY => self
                .encoding_stats
                .0
                .chroma_pred_mode_counts
                .values()
                .sum::<usize>(),
            FrameType::INTER => self
                .encoding_stats
                .1
                .chroma_pred_mode_counts
                .values()
                .sum::<usize>(),
            _ => unreachable!(),
        }
    }

    fn get_luma_pred_mode_pct_by_frame_type(
        &self,
        pred_mode: PredictionMode,
        frame_type: FrameType,
    ) -> f32 {
        let count = self.get_luma_pred_count_by_frame_type(frame_type);
        if count == 0 {
            return 0.;
        }
        (match frame_type {
            FrameType::KEY => self
                .encoding_stats
                .0
                .luma_pred_mode_counts
                .get(&pred_mode)
                .copied()
                .unwrap_or(0),
            FrameType::INTER => self
                .encoding_stats
                .1
                .luma_pred_mode_counts
                .get(&pred_mode)
                .copied()
                .unwrap_or(0),
            _ => unreachable!(),
        }) as f32
            / count as f32
            * 100.
    }

    fn get_chroma_pred_mode_pct_by_frame_type(
        &self,
        pred_mode: PredictionMode,
        frame_type: FrameType,
    ) -> f32 {
        let count = self.get_chroma_pred_count_by_frame_type(frame_type);
        if count == 0 {
            return 0.;
        }
        (match frame_type {
            FrameType::KEY => self
                .encoding_stats
                .0
                .chroma_pred_mode_counts
                .get(&pred_mode)
                .copied()
                .unwrap_or(0),
            FrameType::INTER => self
                .encoding_stats
                .1
                .chroma_pred_mode_counts
                .get(&pred_mode)
                .copied()
                .unwrap_or(0),
            _ => unreachable!(),
        }) as f32
            / count as f32
            * 100.
    }

    pub fn print_summary(&self, verbose: bool) {
        eprintln!("{}", self.end_of_encode_progress());

        eprintln!();

        eprintln!("{}", style("Summary by Frame Type").yellow());
        eprintln!(
            "{:10} | {:>6} | {:>9} | {:>6}",
            style("Frame Type").blue(),
            style("Count").blue(),
            style("Avg Size").blue(),
            style("Avg QP").blue(),
        );
        self.print_frame_type_summary(FrameType::KEY);
        self.print_frame_type_summary(FrameType::INTER);
        self.print_frame_type_summary(FrameType::INTRA_ONLY);
        self.print_frame_type_summary(FrameType::SWITCH);

        eprintln!();

        if verbose {
            eprintln!("{}", style("Block Type Usage").yellow());
            self.print_block_type_summary();
            eprintln!();

            eprintln!("{}", style("Transform Type Usage").yellow());
            self.print_transform_type_summary();
            eprintln!();

            eprintln!("{}", style("Prediction Mode Usage").yellow());
            self.print_prediction_modes_summary();
            eprintln!();
        }
    }

    fn print_frame_type_summary(&self, frame_type: FrameType) {
        let count = self.get_frame_type_count(frame_type);
        let size = self.get_frame_type_avg_size(frame_type);
        let avg_qp = self.get_frame_type_avg_qp(frame_type);
        eprintln!(
            "{:10} | {:>6} | {:>9} | {:>6.2}",
            style(frame_type.to_string().replace(" frame", "")).blue(),
            style(count).cyan(),
            style(format!("{} B", size)).cyan(),
            style(avg_qp).cyan(),
        );
    }

    pub fn progress(&self) -> String {
        format!(
            "{:.2} fps, {:.1} Kb/s, ETA {}",
            self.encoding_fps(),
            self.bitrate() as f64 / 1000f64,
            secs_to_human_time(self.estimated_time(), false)
        )
    }

    pub fn progress_verbose(&self) -> String {
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
            "{} in {}, {:.3} fps, {:.2} Kb/s, size: {:.2} MB",
            style("Finished").yellow(),
            style(secs_to_human_time(self.elapsed_time() as u64, true)).cyan(),
            self.encoding_fps(),
            self.bitrate() as f64 / 1000f64,
            self.estimated_size() as f64 / (1024 * 1024) as f64,
        )
    }

    fn print_block_type_summary(&self) {
        self.print_block_type_summary_for_frame_type(FrameType::KEY, 'I');
        self.print_block_type_summary_for_frame_type(FrameType::INTER, 'P');
    }

    fn print_block_type_summary_for_frame_type(&self, frame_type: FrameType, type_label: char) {
        eprintln!(
            "{:8} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6}",
            style(format!("{} Frames", type_label)).yellow(),
            style("x128").blue(),
            style("x64").blue(),
            style("x32").blue(),
            style("x16").blue(),
            style("x8").blue(),
            style("x4").blue()
        );
        eprintln!(
            "{:>8} {:>5.1}% {:>5.1}%                              {}",
            style("128x").blue(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_128X128, frame_type)).cyan(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_128X64, frame_type)).cyan(),
            if frame_type == FrameType::INTER {
                format!(
                    "{}: {:>5.1}%",
                    style("skip").blue(),
                    style(self.get_skip_pct_by_frame_type(frame_type)).cyan()
                )
            } else {
                String::new()
            }
        );
        eprintln!(
            "{:>8} {:>5.1}% {:>5.1}% {:>5.1}% {:>5.1}%",
            style("64x").blue(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_64X128, frame_type)).cyan(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_64X64, frame_type)).cyan(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_64X32, frame_type)).cyan(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_64X16, frame_type)).cyan(),
        );
        eprintln!(
            "{:>8}        {:>5.1}% {:>5.1}% {:>5.1}% {:>5.1}%",
            style("32x").blue(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_32X64, frame_type)).cyan(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_32X32, frame_type)).cyan(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_32X16, frame_type)).cyan(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_32X8, frame_type)).cyan(),
        );
        eprintln!(
            "{:>8}        {:>5.1}% {:>5.1}% {:>5.1}% {:>5.1}% {:>5.1}%",
            style("16x").blue(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_16X64, frame_type)).cyan(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_16X32, frame_type)).cyan(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_16X16, frame_type)).cyan(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_16X8, frame_type)).cyan(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_16X4, frame_type)).cyan(),
        );
        eprintln!(
            "{:>8}               {:>5.1}% {:>5.1}% {:>5.1}% {:>5.1}%",
            style("8x").blue(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_8X32, frame_type)).cyan(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_8X16, frame_type)).cyan(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_8X8, frame_type)).cyan(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_8X4, frame_type)).cyan(),
        );
        eprintln!(
            "{:>8}                      {:>5.1}% {:>5.1}% {:>5.1}%",
            style("4x").blue(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_4X16, frame_type)).cyan(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_4X8, frame_type)).cyan(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_4X4, frame_type)).cyan(),
        );
    }

    fn print_transform_type_summary(&self) {
        self.print_transform_type_summary_by_frame_type(FrameType::KEY, 'I');
        self.print_transform_type_summary_by_frame_type(FrameType::INTER, 'P');
    }

    fn print_transform_type_summary_by_frame_type(&self, frame_type: FrameType, type_label: char) {
        eprintln!("{:8}", style(format!("{} Frames", type_label)).yellow());
        eprintln!(
            "{:9} {:>5.1}%",
            style("DCT_DCT").blue(),
            style(self.get_txtype_pct_by_frame_type(TxType::DCT_DCT, frame_type)).cyan()
        );
        eprintln!(
            "{:9} {:>5.1}%",
            style("ADST_DCT").blue(),
            style(self.get_txtype_pct_by_frame_type(TxType::ADST_DCT, frame_type)).cyan()
        );
        eprintln!(
            "{:9} {:>5.1}%",
            style("DCT_ADST").blue(),
            style(self.get_txtype_pct_by_frame_type(TxType::DCT_ADST, frame_type)).cyan()
        );
        eprintln!(
            "{:9} {:>5.1}%",
            style("ADST_ADST").blue(),
            style(self.get_txtype_pct_by_frame_type(TxType::ADST_ADST, frame_type)).cyan()
        );
        eprintln!(
            "{:9} {:>5.1}%",
            style("IDTX").blue(),
            style(self.get_txtype_pct_by_frame_type(TxType::IDTX, frame_type)).cyan()
        );
        eprintln!(
            "{:9} {:>5.1}%",
            style("V_DCT").blue(),
            style(self.get_txtype_pct_by_frame_type(TxType::V_DCT, frame_type)).cyan()
        );
        eprintln!(
            "{:9} {:>5.1}%",
            style("H_DCT").blue(),
            style(self.get_txtype_pct_by_frame_type(TxType::H_DCT, frame_type)).cyan()
        );
    }

    fn print_prediction_modes_summary(&self) {
        self.print_luma_prediction_mode_summary_by_frame_type(FrameType::KEY, 'I');
        self.print_chroma_prediction_mode_summary_by_frame_type(FrameType::KEY, 'I');
        self.print_luma_prediction_mode_summary_by_frame_type(FrameType::INTER, 'P');
        self.print_chroma_prediction_mode_summary_by_frame_type(FrameType::INTER, 'P');
    }

    fn print_luma_prediction_mode_summary_by_frame_type(
        &self,
        frame_type: FrameType,
        type_label: char,
    ) {
        eprintln!(
            "{}",
            style(format!("{} Frame Luma Modes", type_label)).yellow()
        );
        if frame_type == FrameType::KEY {
            eprintln!(
                "{:8} {:>5.1}%",
                style("DC").blue(),
                style(
                    self.get_luma_pred_mode_pct_by_frame_type(PredictionMode::DC_PRED, frame_type)
                )
                .cyan()
            );

            eprintln!(
                "{:8} {:>5.1}%",
                style("Vert").blue(),
                style(
                    self.get_luma_pred_mode_pct_by_frame_type(PredictionMode::V_PRED, frame_type)
                )
                .cyan()
            );
            eprintln!(
                "{:8} {:>5.1}%",
                style("Horiz").blue(),
                style(
                    self.get_luma_pred_mode_pct_by_frame_type(PredictionMode::H_PRED, frame_type)
                )
                .cyan()
            );
            eprintln!(
                "{:8} {:>5.1}%",
                style("Paeth").blue(),
                style(
                    self.get_luma_pred_mode_pct_by_frame_type(
                        PredictionMode::PAETH_PRED,
                        frame_type
                    )
                )
                .cyan()
            );
            eprintln!(
                "{:8} {:>5.1}%",
                style("Smooth").blue(),
                style(
                    self.get_luma_pred_mode_pct_by_frame_type(
                        PredictionMode::SMOOTH_PRED,
                        frame_type
                    )
                )
                .cyan()
            );
            eprintln!(
                "{:8} {:>5.1}%",
                style("Smooth V").blue(),
                style(self.get_luma_pred_mode_pct_by_frame_type(
                    PredictionMode::SMOOTH_V_PRED,
                    frame_type
                ))
                .cyan()
            );
            eprintln!(
                "{:8} {:>5.1}%",
                style("Smooth H").blue(),
                style(self.get_luma_pred_mode_pct_by_frame_type(
                    PredictionMode::SMOOTH_H_PRED,
                    frame_type
                ))
                .cyan()
            );
            eprintln!(
                "{:8} {:>5.1}%",
                style("45-Deg").blue(),
                style(
                    self.get_luma_pred_mode_pct_by_frame_type(PredictionMode::D45_PRED, frame_type)
                )
                .cyan()
            );
            eprintln!(
                "{:8} {:>5.1}%",
                style("63-Deg").blue(),
                style(
                    self.get_luma_pred_mode_pct_by_frame_type(PredictionMode::D63_PRED, frame_type)
                )
                .cyan()
            );
            eprintln!(
                "{:8} {:>5.1}%",
                style("117-Deg").blue(),
                style(
                    self.get_luma_pred_mode_pct_by_frame_type(
                        PredictionMode::D117_PRED,
                        frame_type
                    )
                )
                .cyan()
            );
            eprintln!(
                "{:8} {:>5.1}%",
                style("135-Deg").blue(),
                style(
                    self.get_luma_pred_mode_pct_by_frame_type(
                        PredictionMode::D135_PRED,
                        frame_type
                    )
                )
                .cyan()
            );
            eprintln!(
                "{:8} {:>5.1}%",
                style("153-Deg").blue(),
                style(
                    self.get_luma_pred_mode_pct_by_frame_type(
                        PredictionMode::D153_PRED,
                        frame_type
                    )
                )
                .cyan()
            );
            eprintln!(
                "{:8} {:>5.1}%",
                style("207-Deg").blue(),
                style(
                    self.get_luma_pred_mode_pct_by_frame_type(
                        PredictionMode::D207_PRED,
                        frame_type
                    )
                )
                .cyan()
            );
        } else if frame_type == FrameType::INTER {
            eprintln!(
                "{:15} {:>5.1}%",
                style("Nearest").blue(),
                style(
                    self.get_luma_pred_mode_pct_by_frame_type(
                        PredictionMode::NEARESTMV,
                        frame_type
                    )
                )
                .cyan()
            );
            eprintln!(
                "{:15} {:>5.1}%",
                style("Near-0").blue(),
                style(
                    self.get_luma_pred_mode_pct_by_frame_type(PredictionMode::NEAR0MV, frame_type)
                )
                .cyan()
            );
            eprintln!(
                "{:15} {:>5.1}%",
                style("Near-1").blue(),
                style(
                    self.get_luma_pred_mode_pct_by_frame_type(PredictionMode::NEAR1MV, frame_type)
                )
                .cyan()
            );
            eprintln!(
                "{:15} {:>5.1}%",
                style("Near-Near").blue(),
                style(
                    self.get_luma_pred_mode_pct_by_frame_type(
                        PredictionMode::NEAR_NEARMV,
                        frame_type
                    )
                )
                .cyan()
            );
            eprintln!(
                "{:15} {:>5.1}%",
                style("New").blue(),
                style(self.get_luma_pred_mode_pct_by_frame_type(PredictionMode::NEWMV, frame_type))
                    .cyan()
            );
            eprintln!(
                "{:15} {:>5.1}%",
                style("New-New").blue(),
                style(
                    self.get_luma_pred_mode_pct_by_frame_type(
                        PredictionMode::NEW_NEWMV,
                        frame_type
                    )
                )
                .cyan()
            );
            eprintln!(
                "{:15} {:>5.1}%",
                style("Nearest-Nearest").blue(),
                style(self.get_luma_pred_mode_pct_by_frame_type(
                    PredictionMode::NEAREST_NEARESTMV,
                    frame_type
                ))
                .cyan()
            );
            eprintln!(
                "{:15} {:>5.1}%",
                style("Global-Global").blue(),
                style(self.get_luma_pred_mode_pct_by_frame_type(
                    PredictionMode::GLOBAL_GLOBALMV,
                    frame_type
                ))
                .cyan()
            );
        }
    }

    fn print_chroma_prediction_mode_summary_by_frame_type(
        &self,
        frame_type: FrameType,
        type_label: char,
    ) {
        eprintln!(
            "{}",
            style(format!("{} Frame Chroma Modes", type_label)).yellow()
        );
        if frame_type == FrameType::KEY {
            eprintln!(
                "{:8} {:>5.1}%",
                style("DC").blue(),
                style(
                    self.get_chroma_pred_mode_pct_by_frame_type(
                        PredictionMode::DC_PRED,
                        frame_type
                    )
                )
                .cyan()
            );

            eprintln!(
                "{:8} {:>5.1}%",
                style("Vert").blue(),
                style(
                    self.get_chroma_pred_mode_pct_by_frame_type(PredictionMode::V_PRED, frame_type)
                )
                .cyan()
            );
            eprintln!(
                "{:8} {:>5.1}%",
                style("Horiz").blue(),
                style(
                    self.get_chroma_pred_mode_pct_by_frame_type(PredictionMode::H_PRED, frame_type)
                )
                .cyan()
            );
            eprintln!(
                "{:8} {:>5.1}%",
                style("Paeth").blue(),
                style(self.get_chroma_pred_mode_pct_by_frame_type(
                    PredictionMode::PAETH_PRED,
                    frame_type
                ))
                .cyan()
            );
            eprintln!(
                "{:8} {:>5.1}%",
                style("Smooth").blue(),
                style(self.get_chroma_pred_mode_pct_by_frame_type(
                    PredictionMode::SMOOTH_PRED,
                    frame_type
                ))
                .cyan()
            );
            eprintln!(
                "{:8} {:>5.1}%",
                style("Smooth V").blue(),
                style(self.get_chroma_pred_mode_pct_by_frame_type(
                    PredictionMode::SMOOTH_V_PRED,
                    frame_type
                ))
                .cyan()
            );
            eprintln!(
                "{:8} {:>5.1}%",
                style("Smooth H").blue(),
                style(self.get_chroma_pred_mode_pct_by_frame_type(
                    PredictionMode::SMOOTH_H_PRED,
                    frame_type
                ))
                .cyan()
            );
            eprintln!(
                "{:8} {:>5.1}%",
                style("45-Deg").blue(),
                style(
                    self.get_chroma_pred_mode_pct_by_frame_type(
                        PredictionMode::D45_PRED,
                        frame_type
                    )
                )
                .cyan()
            );
            eprintln!(
                "{:8} {:>5.1}%",
                style("63-Deg").blue(),
                style(
                    self.get_chroma_pred_mode_pct_by_frame_type(
                        PredictionMode::D63_PRED,
                        frame_type
                    )
                )
                .cyan()
            );
            eprintln!(
                "{:8} {:>5.1}%",
                style("117-Deg").blue(),
                style(
                    self.get_chroma_pred_mode_pct_by_frame_type(
                        PredictionMode::D117_PRED,
                        frame_type
                    )
                )
                .cyan()
            );
            eprintln!(
                "{:8} {:>5.1}%",
                style("135-Deg").blue(),
                style(
                    self.get_chroma_pred_mode_pct_by_frame_type(
                        PredictionMode::D135_PRED,
                        frame_type
                    )
                )
                .cyan()
            );
            eprintln!(
                "{:8} {:>5.1}%",
                style("153-Deg").blue(),
                style(
                    self.get_chroma_pred_mode_pct_by_frame_type(
                        PredictionMode::D153_PRED,
                        frame_type
                    )
                )
                .cyan()
            );
            eprintln!(
                "{:8} {:>5.1}%",
                style("207-Deg").blue(),
                style(
                    self.get_chroma_pred_mode_pct_by_frame_type(
                        PredictionMode::D207_PRED,
                        frame_type
                    )
                )
                .cyan()
            );
            eprintln!(
                "{:8} {:>5.1}%",
                style("UV CFL").blue(),
                style(self.get_chroma_pred_mode_pct_by_frame_type(
                    PredictionMode::UV_CFL_PRED,
                    frame_type
                ))
                .cyan()
            );
        } else if frame_type == FrameType::INTER {
            eprintln!(
                "{:15} {:>5.1}%",
                style("Nearest").blue(),
                style(
                    self.get_chroma_pred_mode_pct_by_frame_type(
                        PredictionMode::NEARESTMV,
                        frame_type
                    )
                )
                .cyan()
            );
            eprintln!(
                "{:15} {:>5.1}%",
                style("Near-0").blue(),
                style(
                    self.get_chroma_pred_mode_pct_by_frame_type(
                        PredictionMode::NEAR0MV,
                        frame_type
                    )
                )
                .cyan()
            );
            eprintln!(
                "{:15} {:>5.1}%",
                style("Near-1").blue(),
                style(
                    self.get_chroma_pred_mode_pct_by_frame_type(
                        PredictionMode::NEAR1MV,
                        frame_type
                    )
                )
                .cyan()
            );
            eprintln!(
                "{:15} {:>5.1}%",
                style("Near-Near").blue(),
                style(self.get_chroma_pred_mode_pct_by_frame_type(
                    PredictionMode::NEAR_NEARMV,
                    frame_type
                ))
                .cyan()
            );
            eprintln!(
                "{:15} {:>5.1}%",
                style("New").blue(),
                style(
                    self.get_chroma_pred_mode_pct_by_frame_type(PredictionMode::NEWMV, frame_type)
                )
                .cyan()
            );
            eprintln!(
                "{:15} {:>5.1}%",
                style("New-New").blue(),
                style(
                    self.get_chroma_pred_mode_pct_by_frame_type(
                        PredictionMode::NEW_NEWMV,
                        frame_type
                    )
                )
                .cyan()
            );
            eprintln!(
                "{:15} {:>5.1}%",
                style("Nearest-Nearest").blue(),
                style(self.get_chroma_pred_mode_pct_by_frame_type(
                    PredictionMode::NEAREST_NEARESTMV,
                    frame_type
                ))
                .cyan()
            );
            eprintln!(
                "{:15} {:>5.1}%",
                style("Global-Global").blue(),
                style(self.get_chroma_pred_mode_pct_by_frame_type(
                    PredictionMode::GLOBAL_GLOBALMV,
                    frame_type
                ))
                .cyan()
            );
        }
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
    #[serde(default)]
    encoding_stats: (SerializableEncoderStats, SerializableEncoderStats),
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
            encoding_stats: (
                (&other.encoding_stats.0).into(),
                (&other.encoding_stats.1).into(),
            ),
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
            encoding_stats: (
                (&other.encoding_stats.0).into(),
                (&other.encoding_stats.1).into(),
            ),
        }
    }
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SerializableEncoderStats {
    /// Stores count of pixels belonging to each block size in this frame
    pub block_size_counts: BTreeMap<u8, usize>,
    /// Stores count of pixels belonging to skip blocks in this frame
    pub skip_block_count: usize,
    /// Stores count of pixels belonging to each transform type in this frame
    pub tx_type_counts: BTreeMap<u8, usize>,
    /// Stores count of pixels belonging to each luma prediction mode in this frame
    pub luma_pred_mode_counts: BTreeMap<u8, usize>,
    /// Stores count of pixels belonging to each chroma prediction mode in this frame
    pub chroma_pred_mode_counts: BTreeMap<u8, usize>,
}

impl From<&EncoderStats> for SerializableEncoderStats {
    fn from(stats: &EncoderStats) -> Self {
        SerializableEncoderStats {
            block_size_counts: stats
                .block_size_counts
                .iter()
                .map(|(k, v)| (*k as u8, *v))
                .collect(),
            skip_block_count: stats.skip_block_count,
            tx_type_counts: stats
                .tx_type_counts
                .iter()
                .map(|(k, v)| (*k as u8, *v))
                .collect(),
            luma_pred_mode_counts: stats
                .luma_pred_mode_counts
                .iter()
                .map(|(k, v)| (*k as u8, *v))
                .collect(),
            chroma_pred_mode_counts: stats
                .chroma_pred_mode_counts
                .iter()
                .map(|(k, v)| (*k as u8, *v))
                .collect(),
        }
    }
}

impl From<&SerializableEncoderStats> for EncoderStats {
    fn from(stats: &SerializableEncoderStats) -> Self {
        EncoderStats {
            block_size_counts: stats
                .block_size_counts
                .iter()
                .map(|(k, v)| (block_size_from_u8(*k), *v))
                .collect(),
            skip_block_count: stats.skip_block_count,
            tx_type_counts: stats
                .tx_type_counts
                .iter()
                .map(|(k, v)| (tx_type_from_u8(*k), *v))
                .collect(),
            luma_pred_mode_counts: stats
                .luma_pred_mode_counts
                .iter()
                .map(|(k, v)| (pred_mode_from_u8(*k), *v))
                .collect(),
            chroma_pred_mode_counts: stats
                .chroma_pred_mode_counts
                .iter()
                .map(|(k, v)| (pred_mode_from_u8(*k), *v))
                .collect(),
        }
    }
}

fn block_size_from_u8(val: u8) -> BlockSize {
    use BlockSize::*;
    match val {
        0 => BLOCK_4X4,
        1 => BLOCK_4X8,
        2 => BLOCK_8X4,
        3 => BLOCK_8X8,
        4 => BLOCK_8X16,
        5 => BLOCK_16X8,
        6 => BLOCK_16X16,
        7 => BLOCK_16X32,
        8 => BLOCK_32X16,
        9 => BLOCK_32X32,
        10 => BLOCK_32X64,
        11 => BLOCK_64X32,
        12 => BLOCK_64X64,
        13 => BLOCK_64X128,
        14 => BLOCK_128X64,
        15 => BLOCK_128X128,
        16 => BLOCK_4X16,
        17 => BLOCK_16X4,
        18 => BLOCK_8X32,
        19 => BLOCK_32X8,
        20 => BLOCK_16X64,
        21 => BLOCK_64X16,
        _ => unreachable!(),
    }
}

fn tx_type_from_u8(val: u8) -> TxType {
    use TxType::*;
    match val {
        0 => DCT_DCT,
        1 => ADST_DCT,
        2 => DCT_ADST,
        3 => ADST_ADST,
        4 => FLIPADST_DCT,
        5 => DCT_FLIPADST,
        6 => FLIPADST_FLIPADST,
        7 => ADST_FLIPADST,
        8 => FLIPADST_ADST,
        9 => IDTX,
        10 => V_DCT,
        11 => H_DCT,
        12 => V_ADST,
        13 => H_ADST,
        14 => V_FLIPADST,
        15 => H_FLIPADST,
        _ => unreachable!(),
    }
}

fn pred_mode_from_u8(val: u8) -> PredictionMode {
    use PredictionMode::*;
    match val {
        0 => DC_PRED,
        1 => V_PRED,
        2 => H_PRED,
        3 => D45_PRED,
        4 => D135_PRED,
        5 => D117_PRED,
        6 => D153_PRED,
        7 => D207_PRED,
        8 => D63_PRED,
        9 => SMOOTH_PRED,
        10 => SMOOTH_V_PRED,
        11 => SMOOTH_H_PRED,
        12 => PAETH_PRED,
        13 => UV_CFL_PRED,
        14 => NEARESTMV,
        15 => NEAR0MV,
        16 => NEAR1MV,
        17 => NEAR2MV,
        18 => GLOBALMV,
        19 => NEWMV,
        20 => NEAREST_NEARESTMV,
        21 => NEAR_NEARMV,
        22 => NEAREST_NEWMV,
        23 => NEW_NEARESTMV,
        24 => NEAR_NEWMV,
        25 => NEW_NEARMV,
        26 => GLOBAL_GLOBALMV,
        27 => NEW_NEWMV,
        _ => unreachable!(),
    }
}
