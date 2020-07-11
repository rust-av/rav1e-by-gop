use crate::{EncodeOptions, VideoDetails};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The client sends this as a text-based "handshake" to discover
/// the number of worker threads available in the server.
pub const WORKER_QUERY_MESSAGE: &str = "handshake";

/// Indicates that the client is requesting a worker slot on the server.
/// Intended to be a very small message to acquire a slot
/// before loading frames into memory and sending them over the network.
///
/// This is needed to be a separate struct due to needing to know the pixel depth
/// before receiving an encoder message.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SlotRequestMessage {
    pub options: EncodeOptions,
    pub video_info: VideoDetails,
}

/// Use this to send y4m frame data, loaded into the v_frame struct,
/// from the client to the server.
#[derive(Serialize, Deserialize)]
pub struct RawFrameData {
    pub connection_id: Uuid,
    pub compressed_frames: Vec<Vec<u8>>,
}
