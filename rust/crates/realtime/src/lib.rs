//! Bounded realtime transport derived from original Dispatcharr's consumer and
//! ChannelsRedis protocol. Full original application authorship is retained.
//! Router integration and transport parity gates are not yet complete.
mod bus;
pub mod codec;
pub use bus::{BusConfig, Incoming, Membership, RealtimeBus};
mod gateway;
mod hub;
pub use gateway::{GatewayConfig, PreviewHandler, RealtimeGateway};
pub mod policy;

mod statistics;
pub use statistics::{LiveStatistics, StatisticsConfig};

#[cfg(test)]
mod core_special_peer;
