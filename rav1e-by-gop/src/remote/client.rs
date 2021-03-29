use serde::{Deserialize, Serialize};

use crate::{EncodeOptions, VideoDetails};

/// The client sends this as a text-based "handshake" to discover
/// the number of worker threads available in the server.
pub const WORKER_QUERY_MESSAGE: &str = "handshake";

/// Indicates that the client is requesting a worker slot on the server.
/// Intended to be a very small message to acquire a slot
/// before loading frames into memory and sending them over the network.
///
/// This is needed to be a separate struct due to needing to know the pixel depth
/// before receiving an encoder message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotRequestMessage {
    pub options: EncodeOptions,
    pub video_info: VideoDetails,
    pub client_version: semver::Version,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostSegmentMessage {
    pub keyframe_number: usize,
    pub segment_idx: usize,
    pub next_analysis_frame: usize,
}
