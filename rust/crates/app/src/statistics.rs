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
/// Periodic Celery task source uses the real live scanner already owned by Main.
/// It deliberately returns live channels/count, not the combined HTTP payload.
struct LiveTaskSource(StreamEngine);
impl dispatcharr_job_transport::stats_worker::Source for LiveTaskSource {
    fn build(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Value, dispatcharr_job_transport::stats_worker::Error>> {
        Box::pin(async move {
            self.0
                .global_live_statistics_retained(owner)
                .await
                .map_err(|_| dispatcharr_job_transport::stats_worker::Error::Build)
        })
    }
}
/// Attach this adapter to the same Main engine and real realtime event sink.
/// The queue handler owns task admission/drain; this creates no pools or worker.
pub fn task_source(
    engine: &StreamEngine,
) -> Arc<dyn dispatcharr_job_transport::stats_worker::Source> {
    Arc::new(LiveTaskSource(engine.clone()))
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
