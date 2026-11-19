//! Native live events. Original Dispatcharr authors retain full credit.
use dispatcharr_contracts::{BoxFuture, UpdateEventError};
use dispatcharr_realtime::{
    BusConfig, GatewayConfig, PreviewHandler, PreviewResponse, RealtimeBus, RealtimeGateway,
};
use dispatcharr_store::{AuthService, StreamRegexPreview};
use std::sync::Arc;

struct ProfilePreview(StreamRegexPreview);

impl PreviewHandler for ProfilePreview {
    fn render(&self, request: String) -> BoxFuture<'_, Result<PreviewResponse, UpdateEventError>> {
        Box::pin(async move {
            let (fields, owner) = self.0.m3u_profile_test(request).await?;
            let preview = fields["search_preview"]
                .as_str()
                .ok_or(UpdateEventError::InvalidMessage)?;
            let result = fields["result"]
                .as_str()
                .ok_or(UpdateEventError::InvalidMessage)?;
            // The dependency returns JSON-escaped scalar fragments. Appending
            // them directly preserves lone UTF-16 surrogates on the JSON wire.
            let wire = format!(
                r#"{{"data":{{"type":"m3u_profile_test","search_preview":{preview},"result":{result}}}}}"#
            );
            Ok(PreviewResponse {
                bytes: wire.into_bytes().into_boxed_slice(),
                owner,
            })
        })
    }
}

pub async fn bus(url: Option<&str>) -> Result<Option<RealtimeBus>, String> {
    let Some(url) = url else {
        return Ok(None);
    };
    let mut config = BusConfig::default();
    config.max_connections = crate::positive("RUST_WS_MAX_CONNECTIONS", config.max_connections)?;
    config.max_group_members =
        crate::positive("RUST_WS_MAX_GROUP_MEMBERS", config.max_group_members)?;
    config.max_message_bytes = crate::positive("RUST_WS_EVENT_BYTES", config.max_message_bytes)?;
    config.max_publish_bytes = crate::positive("RUST_WS_PUBLISH_BYTES", config.max_publish_bytes)?;
    config.max_read_bytes = crate::positive("RUST_WS_READ_BYTES", config.max_read_bytes)?;
    RealtimeBus::connect(url, config)
        .await
        .map(Some)
        .map_err(|_| "Realtime Redis initialization failed (credentials omitted)".into())
}

pub fn gateway(
    auth: AuthService,
    bus: Option<RealtimeBus>,
    stream_regex: StreamRegexPreview,
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
        .map(|gateway| Some(gateway.with_preview_handler(Arc::new(ProfilePreview(stream_regex)))))
        .map_err(|_| "Invalid realtime configuration".into())
}
