#![allow(clippy::cognitive_complexity)]

use rav1e::prelude::*;

pub mod encode;
pub mod muxer;

pub use self::encode::*;
pub use self::muxer::*;

#[derive(Debug, Clone, Copy)]
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

pub struct SegmentData<T: Pixel> {
    pub segment_no: usize,
    pub slot_no: usize,
    pub next_analysis_frame: usize,
    pub start_frameno: usize,
    pub frames: Vec<Frame<T>>,
}
