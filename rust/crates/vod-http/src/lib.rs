//! Native VOD slice of the derivative Dispatcharr Rust backend port.
//! Full original application credit: upstream Dispatcharr authors and contributors.
mod engine;
mod events;
mod resolver;
mod sessions;
mod statistics;

pub use engine::{VodConfig, VodEngine};
pub use resolver::{ContentKind, SqlVodResolver, VodCandidate, VodRequest, VodResolver, VodSource};
