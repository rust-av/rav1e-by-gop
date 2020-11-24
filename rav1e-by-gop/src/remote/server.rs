use crate::{ProgressInfo, SegmentFrameData, SerializableProgressInfo};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

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
    AwaitingData {
        time_ready: DateTime<Utc>,
    },
    Ready {
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
