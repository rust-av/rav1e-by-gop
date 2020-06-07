use crate::{EncodeOptions, VideoDetails};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use v_frame::frame::Frame;
use v_frame::pixel::Pixel;
use zstd::{Decoder, Encoder};

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

#[derive(Serialize, Deserialize)]
pub struct CompressedRawFrameData {
    pub connection_id: Uuid,
    compressed_data: Vec<u8>,
}

impl CompressedRawFrameData {
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.compressed_data.len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.compressed_data.is_empty()
    }
}

/// Use this to send y4m frame data, loaded into the v_frame struct,
/// from the client to the server.
#[derive(Serialize, Deserialize)]
pub struct RawFrameData<T: Pixel + Default> {
    pub connection_id: Uuid,
    pub frames: Vec<Frame<T>>,
}

impl<T: Pixel + Default + DeserializeOwned> From<CompressedRawFrameData> for RawFrameData<T> {
    fn from(compressed: CompressedRawFrameData) -> Self {
        let decoder = Decoder::new(compressed.compressed_data.as_slice()).unwrap();
        let frames = bincode::deserialize_from(decoder).unwrap();

        RawFrameData {
            connection_id: compressed.connection_id,
            frames,
        }
    }
}

impl<T: Pixel + Default + Serialize> From<RawFrameData<T>> for CompressedRawFrameData {
    fn from(data: RawFrameData<T>) -> Self {
        let mut compressed_data = Vec::new();
        let mut encoder = Encoder::new(&mut compressed_data, 0).unwrap();
        bincode::serialize_into(&mut encoder, &data.frames).unwrap();
        encoder.finish().unwrap();

        CompressedRawFrameData {
            connection_id: data.connection_id,
            compressed_data,
        }
    }
}
