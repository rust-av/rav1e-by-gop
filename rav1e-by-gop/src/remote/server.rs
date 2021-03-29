use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{ProgressInfo, SegmentFrameData, SerializableProgressInfo};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct GetInfoResponse {
    pub worker_count: usize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct PostEnqueueResponse {
    pub request_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetProgressResponse {
    pub progress: SerializableProgressInfo,
    pub done: bool,
}

pub enum EncodeState {
    Enqueued,
    AwaitingInfo {
        time_ready: DateTime<Utc>,
    },
    AwaitingData {
        keyframe_number: usize,
        segment_idx: usize,
        next_analysis_frame: usize,
        time_ready: DateTime<Utc>,
    },
    Ready {
        keyframe_number: usize,
        segment_idx: usize,
        next_analysis_frame: usize,
        raw_frames: Arc<SegmentFrameData>,
    },
    InProgress {
        progress: ProgressInfo,
    },
    EncodingDone {
        progress: ProgressInfo,
        encoded_data: Vec<u8>,
        time_finished: DateTime<Utc>,
    },
}
