use arrayvec::ArrayVec;
#[cfg(feature = "binary")]
use console::style;
#[cfg(feature = "binary")]
use log::info;
use rav1e::data::EncoderStats;
use rav1e::prelude::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeSet;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct ProgressInfo {
    // Frame rate of the video
    pub frame_rate: Rational,
    // The length of the whole video, in frames. `None` if not known.
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
    pub keyframes: BTreeSet<usize>,
    pub completed_segments: BTreeSet<usize>,
    pub segment_idx: usize,
    pub next_analysis_frame: usize,
    pub frame_limit: Option<u64>,
}

impl ProgressInfo {
    pub fn new(
        frame_rate: Rational,
        total_frames: usize,
        keyframes: BTreeSet<usize>,
        segment_idx: usize,
        next_analysis_frame: usize,
        frame_limit: Option<u64>,
    ) -> Self {
        Self {
            frame_rate,
            total_frames,
            time_started: Instant::now(),
            frame_info: Vec::with_capacity(total_frames),
            encoded_size: 0,
            keyframes,
            completed_segments: BTreeSet::new(),
            segment_idx,
            encoding_stats: (EncoderStats::default(), EncoderStats::default()),
            next_analysis_frame,
            frame_limit,
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

    #[cfg(feature = "binary")]
    fn frames_encoded(&self) -> usize {
        self.frame_info.len()
    }

    #[cfg(feature = "binary")]
    fn encoding_fps(&self) -> f64 {
        self.frame_info.len() as f64 / self.elapsed_time()
    }

    #[cfg(feature = "binary")]
    fn video_fps(&self) -> f64 {
        self.frame_rate.num as f64 / self.frame_rate.den as f64
    }

    #[cfg(feature = "binary")]
    // Returns the bitrate of the frames so far, in bits/second
    fn bitrate(&self) -> usize {
        let bits = self.encoded_size * 8;
        let seconds = self.frame_info.len() as f64 / self.video_fps();
        (bits as f64 / seconds) as usize
    }

    #[cfg(feature = "binary")]
    // Estimates the final filesize in bytes, if the number of frames is known
    fn estimated_size(&self) -> usize {
        self.encoded_size * self.total_frames / self.frames_encoded()
    }

    #[cfg(feature = "binary")]
    // Estimates the remaining encoding time in seconds
    fn estimated_time(&self, total_frames: usize) -> u64 {
        ((total_frames - self.frames_encoded()) as f64 / self.encoding_fps()) as u64
    }

    pub fn elapsed_time(&self) -> f64 {
        let duration = Instant::now().duration_since(self.time_started);
        duration.as_secs() as f64 + duration.subsec_millis() as f64 / 1000f64
    }

    #[cfg(feature = "binary")]
    // Number of frames of given type which appear in the video
    fn get_frame_type_count(&self, frame_type: FrameType) -> usize {
        self.frame_info
            .iter()
            .filter(|frame| frame.frame_type == frame_type)
            .count()
    }

    #[cfg(feature = "binary")]
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

    #[cfg(feature = "binary")]
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

    #[cfg(feature = "binary")]
    fn get_block_count_by_frame_type(&self, frame_type: FrameType) -> usize {
        match frame_type {
            FrameType::KEY => self
                .encoding_stats
                .0
                .block_size_counts
                .iter()
                .sum::<usize>(),
            FrameType::INTER => self
                .encoding_stats
                .1
                .block_size_counts
                .iter()
                .sum::<usize>(),
            _ => unreachable!(),
        }
    }

    #[cfg(feature = "binary")]
    fn get_tx_count_by_frame_type(&self, frame_type: FrameType) -> usize {
        match frame_type {
            FrameType::KEY => self.encoding_stats.0.tx_type_counts.iter().sum::<usize>(),
            FrameType::INTER => self.encoding_stats.1.tx_type_counts.iter().sum::<usize>(),
            _ => unreachable!(),
        }
    }

    #[cfg(feature = "binary")]
    fn get_bsize_pct_by_frame_type(&self, bsize: BlockSize, frame_type: FrameType) -> f32 {
        let count = self.get_block_count_by_frame_type(frame_type);
        if count == 0 {
            return 0.;
        }
        (match frame_type {
            FrameType::KEY => self.encoding_stats.0.block_size_counts[bsize as usize],
            FrameType::INTER => self.encoding_stats.1.block_size_counts[bsize as usize],
            _ => unreachable!(),
        }) as f32
            / count as f32
            * 100.
    }

    #[cfg(feature = "binary")]
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

    #[cfg(feature = "binary")]
    fn get_txtype_pct_by_frame_type(&self, tx_type: TxType, frame_type: FrameType) -> f32 {
        let count = self.get_tx_count_by_frame_type(frame_type);
        if count == 0 {
            return 0.;
        }
        (match frame_type {
            FrameType::KEY => self.encoding_stats.0.tx_type_counts[tx_type as usize],
            FrameType::INTER => self.encoding_stats.1.tx_type_counts[tx_type as usize],
            _ => unreachable!(),
        }) as f32
            / count as f32
            * 100.
    }

    #[cfg(feature = "binary")]
    fn get_luma_pred_count_by_frame_type(&self, frame_type: FrameType) -> usize {
        match frame_type {
            FrameType::KEY => self
                .encoding_stats
                .0
                .luma_pred_mode_counts
                .iter()
                .sum::<usize>(),
            FrameType::INTER => self
                .encoding_stats
                .1
                .luma_pred_mode_counts
                .iter()
                .sum::<usize>(),
            _ => unreachable!(),
        }
    }

    #[cfg(feature = "binary")]
    fn get_chroma_pred_count_by_frame_type(&self, frame_type: FrameType) -> usize {
        match frame_type {
            FrameType::KEY => self
                .encoding_stats
                .0
                .chroma_pred_mode_counts
                .iter()
                .sum::<usize>(),
            FrameType::INTER => self
                .encoding_stats
                .1
                .chroma_pred_mode_counts
                .iter()
                .sum::<usize>(),
            _ => unreachable!(),
        }
    }

    #[cfg(feature = "binary")]
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
            FrameType::KEY => self.encoding_stats.0.luma_pred_mode_counts[pred_mode as usize],
            FrameType::INTER => self.encoding_stats.1.luma_pred_mode_counts[pred_mode as usize],
            _ => unreachable!(),
        }) as f32
            / count as f32
            * 100.
    }

    #[cfg(feature = "binary")]
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
            FrameType::KEY => self.encoding_stats.0.chroma_pred_mode_counts[pred_mode as usize],
            FrameType::INTER => self.encoding_stats.1.chroma_pred_mode_counts[pred_mode as usize],
            _ => unreachable!(),
        }) as f32
            / count as f32
            * 100.
    }

    #[cfg(feature = "binary")]
    pub fn print_summary(&self, verbose: bool) {
        info!("{}", self.end_of_encode_progress());

        info!("");

        info!("{}", style("Summary by Frame Type").yellow());
        info!(
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

        info!("");

        if verbose {
            info!("{}", style("Block Type Usage").yellow());
            self.print_block_type_summary();
            info!("");

            info!("{}", style("Transform Type Usage").yellow());
            self.print_transform_type_summary();
            info!("");

            info!("{}", style("Prediction Mode Usage").yellow());
            self.print_prediction_modes_summary();
            info!("");
        }
    }

    #[cfg(feature = "binary")]
    fn print_frame_type_summary(&self, frame_type: FrameType) {
        let count = self.get_frame_type_count(frame_type);
        let size = self.get_frame_type_avg_size(frame_type);
        let avg_qp = self.get_frame_type_avg_qp(frame_type);
        info!(
            "{:10} | {:>6} | {:>9} | {:>6.2}",
            style(frame_type.to_string().replace(" frame", "")).blue(),
            style(count).cyan(),
            style(format!("{} B", size)).cyan(),
            style(avg_qp).cyan(),
        );
    }

    #[cfg(feature = "binary")]
    pub fn progress(&self) -> String {
        format!(
            "{:.2} fps, {:.1} Kb/s, ETA {}",
            self.encoding_fps(),
            self.bitrate() as f64 / 1000f64,
            secs_to_human_time(self.estimated_time(self.total_frames), false)
        )
    }

    #[cfg(feature = "binary")]
    pub fn progress_overall(&self) -> String {
        if self.frames_encoded() == 0 {
            if let Some(max_frames) = self.frame_limit {
                format!(
                    "Input Frame {}/{}, Output Frame {}/{}",
                    self.next_analysis_frame + 1,
                    max_frames,
                    self.frames_encoded(),
                    max_frames,
                )
            } else {
                format!(
                    "Input Frame {}, Output Frame {}",
                    self.next_analysis_frame + 1,
                    self.frames_encoded(),
                )
            }
        } else if let Some(max_frames) = self.frame_limit {
            format!(
                "Input Frame {}/{}, Output Frame {}/{}, {:.2} fps, {:.1} Kb/s, ETA {}",
                self.next_analysis_frame + 1,
                max_frames,
                self.frames_encoded(),
                max_frames,
                self.encoding_fps(),
                self.bitrate() as f64 / 1000f64,
                secs_to_human_time(self.estimated_time(max_frames as usize), false)
            )
        } else {
            format!(
                "Input Frame {}, Output Frame {}, {:.2} fps, {:.1} Kb/s",
                self.next_analysis_frame + 1,
                self.frames_encoded(),
                self.encoding_fps(),
                self.bitrate() as f64 / 1000f64,
            )
        }
    }

    #[cfg(feature = "binary")]
    fn end_of_encode_progress(&self) -> String {
        format!(
            "Encoded {} in {}, {:.3} fps, {:.2} Kb/s, size: {:.2} MB",
            style(format!("{} frames", self.total_frames)).yellow(),
            style(secs_to_human_time(self.elapsed_time() as u64, true)).cyan(),
            self.encoding_fps(),
            self.bitrate() as f64 / 1000f64,
            self.estimated_size() as f64 / (1024 * 1024) as f64,
        )
    }

    #[cfg(feature = "binary")]
    fn print_block_type_summary(&self) {
        self.print_block_type_summary_for_frame_type(FrameType::KEY, 'I');
        self.print_block_type_summary_for_frame_type(FrameType::INTER, 'P');
    }

    #[cfg(feature = "binary")]
    fn print_block_type_summary_for_frame_type(&self, frame_type: FrameType, type_label: char) {
        info!(
            "{:8} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6}",
            style(format!("{} Frames", type_label)).yellow(),
            style("x128").blue(),
            style("x64").blue(),
            style("x32").blue(),
            style("x16").blue(),
            style("x8").blue(),
            style("x4").blue()
        );
        info!(
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
        info!(
            "{:>8} {:>5.1}% {:>5.1}% {:>5.1}% {:>5.1}%",
            style("64x").blue(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_64X128, frame_type)).cyan(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_64X64, frame_type)).cyan(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_64X32, frame_type)).cyan(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_64X16, frame_type)).cyan(),
        );
        info!(
            "{:>8}        {:>5.1}% {:>5.1}% {:>5.1}% {:>5.1}%",
            style("32x").blue(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_32X64, frame_type)).cyan(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_32X32, frame_type)).cyan(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_32X16, frame_type)).cyan(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_32X8, frame_type)).cyan(),
        );
        info!(
            "{:>8}        {:>5.1}% {:>5.1}% {:>5.1}% {:>5.1}% {:>5.1}%",
            style("16x").blue(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_16X64, frame_type)).cyan(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_16X32, frame_type)).cyan(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_16X16, frame_type)).cyan(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_16X8, frame_type)).cyan(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_16X4, frame_type)).cyan(),
        );
        info!(
            "{:>8}               {:>5.1}% {:>5.1}% {:>5.1}% {:>5.1}%",
            style("8x").blue(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_8X32, frame_type)).cyan(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_8X16, frame_type)).cyan(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_8X8, frame_type)).cyan(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_8X4, frame_type)).cyan(),
        );
        info!(
            "{:>8}                      {:>5.1}% {:>5.1}% {:>5.1}%",
            style("4x").blue(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_4X16, frame_type)).cyan(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_4X8, frame_type)).cyan(),
            style(self.get_bsize_pct_by_frame_type(BlockSize::BLOCK_4X4, frame_type)).cyan(),
        );
    }

    #[cfg(feature = "binary")]
    fn print_transform_type_summary(&self) {
        self.print_transform_type_summary_by_frame_type(FrameType::KEY, 'I');
        self.print_transform_type_summary_by_frame_type(FrameType::INTER, 'P');
    }

    #[cfg(feature = "binary")]
    fn print_transform_type_summary_by_frame_type(&self, frame_type: FrameType, type_label: char) {
        info!("{:8}", style(format!("{} Frames", type_label)).yellow());
        info!(
            "{:9} {:>5.1}%",
            style("DCT_DCT").blue(),
            style(self.get_txtype_pct_by_frame_type(TxType::DCT_DCT, frame_type)).cyan()
        );
        info!(
            "{:9} {:>5.1}%",
            style("ADST_DCT").blue(),
            style(self.get_txtype_pct_by_frame_type(TxType::ADST_DCT, frame_type)).cyan()
        );
        info!(
            "{:9} {:>5.1}%",
            style("DCT_ADST").blue(),
            style(self.get_txtype_pct_by_frame_type(TxType::DCT_ADST, frame_type)).cyan()
        );
        info!(
            "{:9} {:>5.1}%",
            style("ADST_ADST").blue(),
            style(self.get_txtype_pct_by_frame_type(TxType::ADST_ADST, frame_type)).cyan()
        );
        info!(
            "{:9} {:>5.1}%",
            style("IDTX").blue(),
            style(self.get_txtype_pct_by_frame_type(TxType::IDTX, frame_type)).cyan()
        );
        info!(
            "{:9} {:>5.1}%",
            style("V_DCT").blue(),
            style(self.get_txtype_pct_by_frame_type(TxType::V_DCT, frame_type)).cyan()
        );
        info!(
            "{:9} {:>5.1}%",
            style("H_DCT").blue(),
            style(self.get_txtype_pct_by_frame_type(TxType::H_DCT, frame_type)).cyan()
        );
    }

    #[cfg(feature = "binary")]
    fn print_prediction_modes_summary(&self) {
        self.print_luma_prediction_mode_summary_by_frame_type(FrameType::KEY, 'I');
        self.print_chroma_prediction_mode_summary_by_frame_type(FrameType::KEY, 'I');
        self.print_luma_prediction_mode_summary_by_frame_type(FrameType::INTER, 'P');
        self.print_chroma_prediction_mode_summary_by_frame_type(FrameType::INTER, 'P');
    }

    #[cfg(feature = "binary")]
    fn print_luma_prediction_mode_summary_by_frame_type(
        &self,
        frame_type: FrameType,
        type_label: char,
    ) {
        info!(
            "{}",
            style(format!("{} Frame Luma Modes", type_label)).yellow()
        );
        if frame_type == FrameType::KEY {
            info!(
                "{:8} {:>5.1}%",
                style("DC").blue(),
                style(
                    self.get_luma_pred_mode_pct_by_frame_type(PredictionMode::DC_PRED, frame_type)
                )
                .cyan()
            );

            info!(
                "{:8} {:>5.1}%",
                style("Vert").blue(),
                style(
                    self.get_luma_pred_mode_pct_by_frame_type(PredictionMode::V_PRED, frame_type)
                )
                .cyan()
            );
            info!(
                "{:8} {:>5.1}%",
                style("Horiz").blue(),
                style(
                    self.get_luma_pred_mode_pct_by_frame_type(PredictionMode::H_PRED, frame_type)
                )
                .cyan()
            );
            info!(
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
            info!(
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
            info!(
                "{:8} {:>5.1}%",
                style("Smooth V").blue(),
                style(self.get_luma_pred_mode_pct_by_frame_type(
                    PredictionMode::SMOOTH_V_PRED,
                    frame_type
                ))
                .cyan()
            );
            info!(
                "{:8} {:>5.1}%",
                style("Smooth H").blue(),
                style(self.get_luma_pred_mode_pct_by_frame_type(
                    PredictionMode::SMOOTH_H_PRED,
                    frame_type
                ))
                .cyan()
            );
            info!(
                "{:8} {:>5.1}%",
                style("45-Deg").blue(),
                style(
                    self.get_luma_pred_mode_pct_by_frame_type(PredictionMode::D45_PRED, frame_type)
                )
                .cyan()
            );
            info!(
                "{:8} {:>5.1}%",
                style("63-Deg").blue(),
                style(
                    self.get_luma_pred_mode_pct_by_frame_type(PredictionMode::D67_PRED, frame_type)
                )
                .cyan()
            );
            info!(
                "{:8} {:>5.1}%",
                style("117-Deg").blue(),
                style(
                    self.get_luma_pred_mode_pct_by_frame_type(
                        PredictionMode::D113_PRED,
                        frame_type
                    )
                )
                .cyan()
            );
            info!(
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
            info!(
                "{:8} {:>5.1}%",
                style("153-Deg").blue(),
                style(
                    self.get_luma_pred_mode_pct_by_frame_type(
                        PredictionMode::D157_PRED,
                        frame_type
                    )
                )
                .cyan()
            );
            info!(
                "{:8} {:>5.1}%",
                style("207-Deg").blue(),
                style(
                    self.get_luma_pred_mode_pct_by_frame_type(
                        PredictionMode::D203_PRED,
                        frame_type
                    )
                )
                .cyan()
            );
        } else if frame_type == FrameType::INTER {
            info!(
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
            info!(
                "{:15} {:>5.1}%",
                style("Near-0").blue(),
                style(
                    self.get_luma_pred_mode_pct_by_frame_type(PredictionMode::NEAR0MV, frame_type)
                )
                .cyan()
            );
            info!(
                "{:15} {:>5.1}%",
                style("Near-1").blue(),
                style(
                    self.get_luma_pred_mode_pct_by_frame_type(PredictionMode::NEAR1MV, frame_type)
                )
                .cyan()
            );
            info!(
                "{:15} {:>5.1}%",
                style("Near-Near-0").blue(),
                style(self.get_luma_pred_mode_pct_by_frame_type(
                    PredictionMode::NEAR_NEAR0MV,
                    frame_type
                ))
                .cyan()
            );
            info!(
                "{:15} {:>5.1}%",
                style("Near-Near-1").blue(),
                style(self.get_luma_pred_mode_pct_by_frame_type(
                    PredictionMode::NEAR_NEAR1MV,
                    frame_type
                ))
                .cyan()
            );
            info!(
                "{:15} {:>5.1}%",
                style("Near-Near-2").blue(),
                style(self.get_luma_pred_mode_pct_by_frame_type(
                    PredictionMode::NEAR_NEAR2MV,
                    frame_type
                ))
                .cyan()
            );
            info!(
                "{:15} {:>5.1}%",
                style("New").blue(),
                style(self.get_luma_pred_mode_pct_by_frame_type(PredictionMode::NEWMV, frame_type))
                    .cyan()
            );
            info!(
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
            info!(
                "{:15} {:>5.1}%",
                style("Nearest-Nearest").blue(),
                style(self.get_luma_pred_mode_pct_by_frame_type(
                    PredictionMode::NEAREST_NEARESTMV,
                    frame_type
                ))
                .cyan()
            );
            info!(
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

    #[cfg(feature = "binary")]
    fn print_chroma_prediction_mode_summary_by_frame_type(
        &self,
        frame_type: FrameType,
        type_label: char,
    ) {
        info!(
            "{}",
            style(format!("{} Frame Chroma Modes", type_label)).yellow()
        );
        if frame_type == FrameType::KEY {
            info!(
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

            info!(
                "{:8} {:>5.1}%",
                style("Vert").blue(),
                style(
                    self.get_chroma_pred_mode_pct_by_frame_type(PredictionMode::V_PRED, frame_type)
                )
                .cyan()
            );
            info!(
                "{:8} {:>5.1}%",
                style("Horiz").blue(),
                style(
                    self.get_chroma_pred_mode_pct_by_frame_type(PredictionMode::H_PRED, frame_type)
                )
                .cyan()
            );
            info!(
                "{:8} {:>5.1}%",
                style("Paeth").blue(),
                style(self.get_chroma_pred_mode_pct_by_frame_type(
                    PredictionMode::PAETH_PRED,
                    frame_type
                ))
                .cyan()
            );
            info!(
                "{:8} {:>5.1}%",
                style("Smooth").blue(),
                style(self.get_chroma_pred_mode_pct_by_frame_type(
                    PredictionMode::SMOOTH_PRED,
                    frame_type
                ))
                .cyan()
            );
            info!(
                "{:8} {:>5.1}%",
                style("Smooth V").blue(),
                style(self.get_chroma_pred_mode_pct_by_frame_type(
                    PredictionMode::SMOOTH_V_PRED,
                    frame_type
                ))
                .cyan()
            );
            info!(
                "{:8} {:>5.1}%",
                style("Smooth H").blue(),
                style(self.get_chroma_pred_mode_pct_by_frame_type(
                    PredictionMode::SMOOTH_H_PRED,
                    frame_type
                ))
                .cyan()
            );
            info!(
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
            info!(
                "{:8} {:>5.1}%",
                style("63-Deg").blue(),
                style(
                    self.get_chroma_pred_mode_pct_by_frame_type(
                        PredictionMode::D67_PRED,
                        frame_type
                    )
                )
                .cyan()
            );
            info!(
                "{:8} {:>5.1}%",
                style("117-Deg").blue(),
                style(
                    self.get_chroma_pred_mode_pct_by_frame_type(
                        PredictionMode::D113_PRED,
                        frame_type
                    )
                )
                .cyan()
            );
            info!(
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
            info!(
                "{:8} {:>5.1}%",
                style("153-Deg").blue(),
                style(
                    self.get_chroma_pred_mode_pct_by_frame_type(
                        PredictionMode::D157_PRED,
                        frame_type
                    )
                )
                .cyan()
            );
            info!(
                "{:8} {:>5.1}%",
                style("207-Deg").blue(),
                style(
                    self.get_chroma_pred_mode_pct_by_frame_type(
                        PredictionMode::D203_PRED,
                        frame_type
                    )
                )
                .cyan()
            );
            info!(
                "{:8} {:>5.1}%",
                style("UV CFL").blue(),
                style(self.get_chroma_pred_mode_pct_by_frame_type(
                    PredictionMode::UV_CFL_PRED,
                    frame_type
                ))
                .cyan()
            );
        } else if frame_type == FrameType::INTER {
            info!(
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
            info!(
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
            info!(
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
            info!(
                "{:15} {:>5.1}%",
                style("Near-Near-0").blue(),
                style(self.get_chroma_pred_mode_pct_by_frame_type(
                    PredictionMode::NEAR_NEAR0MV,
                    frame_type
                ))
                .cyan()
            );
            info!(
                "{:15} {:>5.1}%",
                style("Near-Near-1").blue(),
                style(self.get_chroma_pred_mode_pct_by_frame_type(
                    PredictionMode::NEAR_NEAR1MV,
                    frame_type
                ))
                .cyan()
            );
            info!(
                "{:15} {:>5.1}%",
                style("Near-Near-2").blue(),
                style(self.get_chroma_pred_mode_pct_by_frame_type(
                    PredictionMode::NEAR_NEAR2MV,
                    frame_type
                ))
                .cyan()
            );
            info!(
                "{:15} {:>5.1}%",
                style("New").blue(),
                style(
                    self.get_chroma_pred_mode_pct_by_frame_type(PredictionMode::NEWMV, frame_type)
                )
                .cyan()
            );
            info!(
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
            info!(
                "{:15} {:>5.1}%",
                style("Nearest-Nearest").blue(),
                style(self.get_chroma_pred_mode_pct_by_frame_type(
                    PredictionMode::NEAREST_NEARESTMV,
                    frame_type
                ))
                .cyan()
            );
            info!(
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

#[cfg(feature = "binary")]
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
    frame_info: Vec<SerializableFrameSummary>,
    encoded_size: usize,
    keyframes: BTreeSet<usize>,
    completed_segments: BTreeSet<usize>,
    next_analysis_frame: usize,
    // Wall encoding time elapsed so far, in seconds
    #[serde(default)]
    elapsed_time: u64,
    #[serde(default)]
    encoding_stats: (SerializableEncoderStats, SerializableEncoderStats),
    total_frames: usize,
    #[serde(default)]
    frame_limit: Option<u64>,
}

impl From<&ProgressInfo> for SerializableProgressInfo {
    fn from(other: &ProgressInfo) -> Self {
        SerializableProgressInfo {
            frame_rate: (other.frame_rate.num, other.frame_rate.den),
            frame_info: other
                .frame_info
                .iter()
                .map(SerializableFrameSummary::from)
                .collect(),
            encoded_size: other.encoded_size,
            keyframes: other.keyframes.clone(),
            next_analysis_frame: other.next_analysis_frame,
            completed_segments: other.completed_segments.clone(),
            elapsed_time: other.elapsed_time() as u64,
            encoding_stats: (
                (&other.encoding_stats.0).into(),
                (&other.encoding_stats.1).into(),
            ),
            total_frames: other.total_frames,
            frame_limit: other.frame_limit,
        }
    }
}

impl From<&SerializableProgressInfo> for ProgressInfo {
    fn from(other: &SerializableProgressInfo) -> Self {
        ProgressInfo {
            frame_rate: Rational::new(other.frame_rate.0, other.frame_rate.1),
            frame_info: other.frame_info.iter().map(FrameSummary::from).collect(),
            encoded_size: other.encoded_size,
            keyframes: other.keyframes.clone(),
            next_analysis_frame: other.next_analysis_frame,
            completed_segments: other.completed_segments.clone(),
            time_started: Instant::now() - Duration::from_secs(other.elapsed_time),
            segment_idx: 0,
            encoding_stats: (
                (&other.encoding_stats.0).into(),
                (&other.encoding_stats.1).into(),
            ),
            total_frames: other.total_frames,
            frame_limit: other.frame_limit,
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

pub const TX_TYPES: usize = 16;
pub const PREDICTION_MODES: usize = 34;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SerializableEncoderStats {
    /// Stores count of pixels belonging to each block size in this frame
    pub block_size_counts: [usize; BlockSize::BLOCK_SIZES_ALL],
    /// Stores count of pixels belonging to skip blocks in this frame
    pub skip_block_count: usize,
    /// Stores count of pixels belonging to each transform type in this frame
    pub tx_type_counts: [usize; TX_TYPES],
    /// Stores count of pixels belonging to each luma prediction mode in this frame
    pub luma_pred_mode_counts: ArrayVec<[usize; PREDICTION_MODES]>,
    /// Stores count of pixels belonging to each chroma prediction mode in this frame
    pub chroma_pred_mode_counts: ArrayVec<[usize; PREDICTION_MODES]>,
}

impl From<&EncoderStats> for SerializableEncoderStats {
    fn from(stats: &EncoderStats) -> Self {
        SerializableEncoderStats {
            block_size_counts: stats.block_size_counts,
            skip_block_count: stats.skip_block_count,
            tx_type_counts: stats.tx_type_counts,
            luma_pred_mode_counts: stats.luma_pred_mode_counts.clone(),
            chroma_pred_mode_counts: stats.chroma_pred_mode_counts.clone(),
        }
    }
}

impl From<&SerializableEncoderStats> for EncoderStats {
    fn from(stats: &SerializableEncoderStats) -> Self {
        EncoderStats {
            block_size_counts: stats.block_size_counts,
            skip_block_count: stats.skip_block_count,
            tx_type_counts: stats.tx_type_counts,
            luma_pred_mode_counts: stats.luma_pred_mode_counts.clone(),
            chroma_pred_mode_counts: stats.chroma_pred_mode_counts.clone(),
        }
    }
}
