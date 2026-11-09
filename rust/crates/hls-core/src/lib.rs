//! Packet-copy TS HLS segmentation ported from Dispatcharr's original segmenter.
//! Original project authors retain full credit; see ../../../ATTRIBUTION.md.
pub mod parser;
mod playlist;
mod segmenter;
pub use playlist::{render_media_playlist, SegmentDescriptor};
pub use segmenter::{Error, Segment, SegmenterConfig, TSSegmenter};

pub const TS_PACKET_SIZE: usize = 188;
pub const PTS_WRAP_SECONDS: f64 = (1u64 << 33) as f64 / 90_000.0;

#[cfg(test)]
mod tests;
