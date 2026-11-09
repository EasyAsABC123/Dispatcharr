//! Native live events. Original Dispatcharr authors retain full credit.
use dispatcharr_realtime::{BusConfig, GatewayConfig, RealtimeBus, RealtimeGateway};
use dispatcharr_store::AuthService;
use std::env;

pub async fn bus() -> Result<Option<RealtimeBus>, String> {
    let Ok(url) = env::var("REDIS_URL") else {
        return Ok(None);
    };
    let mut config = BusConfig::default();
    config.max_connections = crate::positive("RUST_WS_MAX_CONNECTIONS", config.max_connections)?;
    config.max_group_members =
        crate::positive("RUST_WS_MAX_GROUP_MEMBERS", config.max_group_members)?;
    config.max_message_bytes = crate::positive("RUST_WS_EVENT_BYTES", config.max_message_bytes)?;
    config.max_publish_bytes = crate::positive("RUST_WS_PUBLISH_BYTES", config.max_publish_bytes)?;
    config.max_read_bytes = crate::positive("RUST_WS_READ_BYTES", config.max_read_bytes)?;
    RealtimeBus::connect(&url, config)
        .await
        .map(Some)
        .map_err(|_| "Realtime Redis initialization failed (credentials omitted)".into())
}

pub fn gateway(
    auth: AuthService,
    bus: Option<RealtimeBus>,
) -> Result<Option<RealtimeGateway>, String> {
    let Some(bus) = bus else { return Ok(None) };
    let mut config = GatewayConfig::default();
    config.max_buffered_events =
        crate::positive("RUST_WS_BUFFERED_EVENTS", config.max_buffered_events)?;
    config.max_buffered_bytes =
        crate::positive("RUST_WS_BUFFERED_BYTES", config.max_buffered_bytes)?;
    config.max_shared_wire_bytes =
        crate::positive("RUST_WS_SHARED_BYTES", config.max_shared_wire_bytes)?;
    config.max_private_wire_bytes =
        crate::positive("RUST_WS_PRIVATE_BYTES", config.max_private_wire_bytes)?;
    config.max_socket_output_bytes = crate::positive(
        "RUST_WS_SOCKET_OUTPUT_BYTES",
        config.max_socket_output_bytes,
    )?;
    RealtimeGateway::new(auth, bus, config)
        .map(Some)
        .map_err(|_| "Invalid realtime configuration".into())
}
