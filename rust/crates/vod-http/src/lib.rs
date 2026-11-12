//! Native VOD slice of the derivative Dispatcharr Rust backend port.
//! Full original application credit: upstream Dispatcharr authors and contributors.
mod engine;
mod events;
mod resolver;
// Private opt-in surface; component-only methods remain outside default routes.
#[allow(dead_code)]
mod recovery_generation;
mod sessions;
mod statistics;

pub use engine::{VodConfig, VodEngine};
pub use resolver::{ContentKind, SqlVodResolver, VodCandidate, VodRequest, VodResolver, VodSource};

/// Same full Source VOD projection as transition publication. Caller admission
/// survives actual Redis replies; its owning supervisor retains SQL work.
/// Original Dispatcharr authors retain the projection and fallback contract.
pub async fn source_statistics_retained(
    store: &dispatcharr_store::Store,
    transport: &dispatcharr_output_store::transport::RedisTransport,
    now: f64,
    caller: std::sync::Arc<dyn Send + Sync>,
) -> serde_json::Value {
    statistics::snapshot(store, transport, now, caller)
        .await
        .unwrap_or_else(
            |_| serde_json::json!({"vod_connections":[],"total_connections":0,"timestamp":now}),
        )
}
