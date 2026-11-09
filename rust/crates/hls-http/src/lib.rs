//! Native HLS capability transport for the derivative Dispatcharr Rust port.
//! Original application credit belongs to the upstream authors and contributors.
mod gateway;
mod routes;
pub use gateway::{GatewayConfig, HlsGateway};
