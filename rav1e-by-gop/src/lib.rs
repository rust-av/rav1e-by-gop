#![allow(clippy::cognitive_complexity)]

pub mod compress;
pub mod encode;
pub mod muxer;
#[cfg(feature = "remote")]
pub mod remote;

use std::{path::PathBuf, sync::Arc};

use crossbeam_channel::{Receiver, Sender};
use rav1e::prelude::*;
use serde::{Deserialize, Serialize};
#[cfg(feature = "remote")]
use url::Url;
#[cfg(feature = "remote")]
use uuid::Uuid;

#[cfg(feature = "remote")]
pub use self::remote::*;
pub use self::{compress::*, encode::*, muxer::*};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct VideoDetails {
    pub width: usize,
    pub height: usize,
    pub bit_depth: usize,
    pub chroma_sampling: ChromaSampling,
    pub chroma_sample_position: ChromaSamplePosition,
    pub time_base: Rational,
}

impl Default for VideoDetails {
    fn default() -> Self {
        VideoDetails {
            width: 640,
            height: 480,
            bit_depth: 8,
            chroma_sampling: ChromaSampling::Cs420,
            chroma_sample_position: ChromaSamplePosition::Unknown,
            time_base: Rational { num: 30, den: 1 },
        }
    }
}

pub enum Slot {
    Local(usize),
    #[cfg(feature = "remote")]
    Remote(Box<ActiveConnection>),
}

impl Slot {
    pub fn is_remote(&self) -> bool {
        match self {
            Slot::Local(_) => false,
            #[cfg(feature = "remote")]
            Slot::Remote(_) => true,
        }
    }
}

pub struct SegmentData {
    pub segment_no: usize,
    pub slot: usize,
    pub next_analysis_frame: usize,
    pub start_frameno: usize,
    pub frame_data: SegmentFrameData,
}

pub enum SegmentFrameData {
    CompressedFrames(Vec<Vec<u8>>),
    Y4MFile { path: PathBuf, frame_count: usize },
}

impl SegmentFrameData {
    pub fn frame_count(&self) -> usize {
        match self {
            SegmentFrameData::CompressedFrames(frames) => frames.len(),
            SegmentFrameData::Y4MFile { frame_count, .. } => *frame_count,
        }
    }
}

#[cfg(feature = "remote")]
pub struct ActiveConnection {
    pub worker_uri: Url,
    pub worker_password: String,
    pub request_id: Uuid,
    pub slot_in_worker: usize,
    pub video_info: VideoDetails,
    pub encode_info: Option<EncodeInfo>,
    pub worker_update_sender: WorkerUpdateSender,
    pub progress_sender: ProgressSender,
}

#[derive(Debug, Clone)]
pub struct EncodeInfo {
    pub output_file: Output,
    pub frame_count: usize,
    pub next_analysis_frame: usize,
    pub segment_idx: usize,
    pub start_frameno: usize,
}

pub type WorkerUpdateSender = Sender<WorkerStatusUpdate>;
pub type WorkerUpdateReceiver = Receiver<WorkerStatusUpdate>;
pub type WorkerUpdateChannel = (WorkerUpdateSender, WorkerUpdateReceiver);

#[derive(Debug, Clone, Copy)]
pub struct WorkerStatusUpdate {
    pub status: Option<SlotStatus>,
    pub slot_delta: Option<(usize, bool)>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SlotStatus {
    Empty,
    Requested,
}

#[allow(clippy::clippy::too_many_arguments)]
pub fn build_config(
    speed: usize,
    qp: usize,
    max_bitrate: Option<i32>,
    tiles: usize,
    video_info: VideoDetails,
    pool: Arc<rayon::ThreadPool>,
    color_primaries: ColorPrimaries,
    transfer_characteristics: TransferCharacteristics,
    matrix_coefficients: MatrixCoefficients,
) -> Config {
    Config::new()
        .with_encoder_config(build_encoder_config(
            speed,
            qp,
            max_bitrate,
            tiles,
            video_info,
            color_primaries,
            transfer_characteristics,
            matrix_coefficients,
        ))
        .with_thread_pool(pool)
}

#[allow(clippy::clippy::too_many_arguments)]
pub fn build_encoder_config(
    speed: usize,
    qp: usize,
    max_bitrate: Option<i32>,
    tiles: usize,
    video_info: VideoDetails,
    color_primaries: ColorPrimaries,
    transfer_characteristics: TransferCharacteristics,
    matrix_coefficients: MatrixCoefficients,
) -> EncoderConfig {
    let mut enc_config = EncoderConfig::with_speed_preset(speed);
    enc_config.width = video_info.width;
    enc_config.height = video_info.height;
    enc_config.bit_depth = video_info.bit_depth;
    enc_config.chroma_sampling = video_info.chroma_sampling;
    enc_config.chroma_sample_position = video_info.chroma_sample_position;
    enc_config.time_base = video_info.time_base;
    enc_config.tiles = tiles;
    enc_config.min_key_frame_interval = 0;
    enc_config.max_key_frame_interval = u16::max_value() as u64;
    enc_config.speed_settings.no_scene_detection = true;
    enc_config.color_description = if color_primaries == ColorPrimaries::Unspecified
        && transfer_characteristics == TransferCharacteristics::Unspecified
        && matrix_coefficients == MatrixCoefficients::Unspecified
    {
        // No need to set a color description with all parameters unspecified.
        None
    } else {
        Some(ColorDescription {
            color_primaries,
            transfer_characteristics,
            matrix_coefficients,
        })
    };

    if let Some(max_bitrate) = max_bitrate {
        enc_config.min_quantizer = qp as u8;
        enc_config.bitrate = max_bitrate;
    } else {
        enc_config.quantizer = qp;
    }

    enc_config
}

#[derive(Debug, Clone)]
pub enum Output {
    File(PathBuf),
    Null,
    Memory,
}
