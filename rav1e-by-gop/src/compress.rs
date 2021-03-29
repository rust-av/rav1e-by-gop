//! Utilities for losslessly compressing frames in memory.
//! The goal here is to reduce memory usage,
//! because uncompressed Y4M frames are quite large.

use serde::{de::DeserializeOwned, Serialize};
use v_frame::{frame::Frame, pixel::Pixel};

pub fn compress_frame<T: Pixel + Serialize>(frame: &Frame<T>) -> Vec<u8> {
    let mut compressed_frame = Vec::new();
    let mut encoder = zstd::Encoder::new(&mut compressed_frame, 0).unwrap();
    bincode::serialize_into(&mut encoder, frame).unwrap();
    encoder.finish().unwrap();
    compressed_frame
}

pub fn decompress_frame<T: Pixel + DeserializeOwned>(compressed_frame: &[u8]) -> Frame<T> {
    let decoder = zstd::Decoder::new(compressed_frame).unwrap();
    bincode::deserialize_from(decoder).unwrap()
}
