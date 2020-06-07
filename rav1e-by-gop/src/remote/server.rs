use rav1e::Packet;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use v_frame::pixel::Pixel;

/// The server sends this in response to the handshake message,
/// then disconnects the handshake connection.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct WorkerQueryResponse {
    pub worker_count: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum EncoderMessage<T: Pixel + Default> {
    /// Indicates that the server has allocated a slot for the client
    /// and is ready to receive data.
    ///
    /// Includes the connection ID; the worker will need to send this back
    /// with each raw frame message.
    SlotAllocated(Uuid),
    /// Sends an encoded frame packet from the server to the client.
    /// Within a segment, these will always be sent in order.
    SendEncodedPacket(Box<Packet<T>>),
    /// Indicates that the server is finished encoding the segment,
    /// and that the slot has been released on the server.
    /// A new `RequestSlot` message will need to be sent before
    /// encoding another segment.
    SegmentFinished,
    /// We don't want this to happen... It also closes the connection.
    EncodeFailed(String),
}
