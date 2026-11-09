//! Original global live statistics, shared across status requests and callbacks.
//! Original Dispatcharr authors retain full application credit.
use dispatcharr_clients::ClientStore;
use dispatcharr_contracts::{
    BoxFuture, LiveStatsSnapshotReader, LiveStatsSnapshotSource, LiveStatsTrigger, PlaybackError,
};
use dispatcharr_realtime::{LiveStatistics, RealtimeBus, StatisticsConfig};
use dispatcharr_stream_http::StreamEngine;
use serde_json::Value;
use std::sync::Arc;
struct Source(StreamEngine);
impl LiveStatsSnapshotSource for Source {
    fn build(&self) -> BoxFuture<'_, Result<Value, PlaybackError>> {
        Box::pin(self.0.global_live_statistics())
    }
}
pub fn start(
    engine: &StreamEngine,
    clients: Option<&ClientStore>,
    bus: Option<&RealtimeBus>,
) -> Result<Option<Arc<LiveStatistics>>, String> {
    let (Some(clients), Some(bus)) = (clients, bus) else {
        return Ok(None);
    };
    let mut config = StatisticsConfig::default();
    config.max_snapshot_bytes =
        crate::positive("RUST_STATS_SNAPSHOT_BYTES", config.max_snapshot_bytes)?;
    config.max_live_bytes = crate::positive("RUST_STATS_LIVE_BYTES", config.max_live_bytes)?;
    config.max_pending = crate::positive("RUST_STATS_PENDING", config.max_pending)?;
    let statistics = Arc::new(
        LiveStatistics::new(
            Arc::new(Source(engine.clone())),
            Arc::new(bus.clone()),
            config,
        )
        .map_err(|_| "Invalid live statistics configuration")?,
    );
    let reader: Arc<dyn LiveStatsSnapshotReader> = statistics.clone();
    engine.set_live_statistics(Arc::downgrade(&reader));
    let trigger: Arc<dyn LiveStatsTrigger> = statistics.clone();
    clients.set_live_stats_trigger(Arc::downgrade(&trigger));
    Ok(Some(statistics))
}
