//! Bounded realtime transport derived from original Dispatcharr's consumer and
//! ChannelsRedis protocol. Full original application authorship is retained.
//! Router integration and transport parity gates are not yet complete.
mod bus;
pub mod codec;
pub use bus::{BusConfig, Incoming, Membership, RealtimeBus};
pub mod policy;
