//! Retained guide status/progress/audit effects ported from original Dispatcharr.
//! Original authors retain all credit. Job scheduling/app wiring is separate.
use crate::{
    cache_policy::{CachedSwapPolicy, PolicyClock},
    ImportError, SwapPolicyContext,
};
use chrono::{DateTime, Utc};
use dispatcharr_contracts::UpdateEventSink;
use dispatcharr_ingest_core::source_integer_literal;
use dispatcharr_output_store::transport::{RedisTransport, TransportConfig};
use dispatcharr_store::{Store, StoreError};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

#[derive(Clone, Debug)]
pub struct CompletedImport {
    pub source_id: i64,
    pub source_name: String,
    pub accepted: u64,
    pub channels_with_programmes: usize,
    pub skipped_unmapped: u64,
    pub total_epg_entries: i64,
    pub mapped_tvg_ids: usize,
}
#[derive(Clone, Debug)]
pub struct FailedImport {
    pub source_id: i64,
    pub source_name: String,
    pub last_message: String,
    /// Source's ws_error overrides stored last_message for publication/audit.
    pub publication_error: String,
}
#[derive(Clone, Debug)]
pub enum ReportAction {
    Progress {
        source_id: i64,
        progress: u8,
    },
    Staging {
        source_id: i64,
        processed: u64,
        mapped_tvg_ids: usize,
    },
    NoMappings {
        source_id: i64,
        total_epg_entries: u64,
    },
    Committed(CompletedImport),
    Failed(FailedImport),
    ChannelProgress {
        source_id: i64,
        progress: u8,
        processed: Option<u64>,
        total: Option<u64>,
        total_channels: Option<u64>,
    },
    ChannelsComplete {
        source_id: i64,
        processed: u64,
    },
    ChannelFailed(FailedImport),
    RefreshFailed(FailedImport),
    DownloadFailed(FailedImport),
    LocalDownloadComplete {
        source_id: i64,
    },
}
impl ReportAction {
    fn charge(&self) -> usize {
        match self {
            Self::Committed(v) => v.source_name.capacity(),
            Self::Failed(v)
            | Self::ChannelFailed(v)
            | Self::RefreshFailed(v)
            | Self::DownloadFailed(v) => v
                .source_name
                .capacity()
                .saturating_add(v.last_message.capacity())
                .saturating_add(v.publication_error.capacity()),
            _ => 0,
        }
    }
}
struct WorkState {
    admission: Arc<Semaphore>,
    changed: Notify,
    stopping: AtomicBool,
    max: usize,
}
struct Work {
    permit: Option<OwnedSemaphorePermit>,
    caller: Option<Arc<dyn Send + Sync>>,
    state: Arc<WorkState>,
}
impl Drop for Work {
    fn drop(&mut self) {
        drop(self.caller.take());
        drop(self.permit.take());
        self.state.changed.notify_waiters();
    }
}
struct Inner {
    store: Store,
    transport: RedisTransport,
    settings: CachedSwapPolicy,
    sink: Arc<dyn UpdateEventSink>,
    clock: PolicyClock,
    state: Arc<WorkState>,
}
/// Physical observation for the guarded failure-status helper. Existing
/// execute() still discards this observation, preserving Source sideeffects.
#[derive(Debug)]
pub enum ReportObservation {
    Ordinary,
    FailureStatusSaved,
    FailureStatusUnmatched,
    FailureStatusSqlCaught(sqlx::Error),
}
#[derive(Clone)]
pub struct GuideReporter {
    inner: Arc<Inner>,
}
impl GuideReporter {
    pub async fn connect(
        store: Store,
        redis: &str,
        sink: Arc<dyn UpdateEventSink>,
        max: usize,
    ) -> Result<Self, ImportError> {
        Self::with_clock(store, redis, sink, max, Arc::new(Utc::now)).await
    }
    pub async fn with_clock(
        store: Store,
        redis: &str,
        sink: Arc<dyn UpdateEventSink>,
        max: usize,
        clock: PolicyClock,
    ) -> Result<Self, ImportError> {
        if !(1..=4).contains(&max) {
            return Err(ImportError::Capacity("reporter jobs"));
        }
        let transport = RedisTransport::connect(
            redis,
            TransportConfig {
                max_operations: 2,
                max_queued_operations: 4,
                command_timeout: Duration::from_secs(2),
            },
        )
        .await
        .map_err(|_| ImportError::Worker)?;
        Ok(Self {
            inner: Arc::new(Inner {
                settings: CachedSwapPolicy::new(store.clone(), Some(redis.into())),
                store,
                transport,
                sink,
                clock,
                state: Arc::new(WorkState {
                    admission: Arc::new(Semaphore::new(max)),
                    changed: Notify::new(),
                    stopping: AtomicBool::new(false),
                    max,
                }),
            }),
        })
    }
    pub fn physical_operations(&self) -> usize {
        self.inner.state.max - self.inner.state.admission.available_permits()
    }
    /// Ownership passes to a supervised task. Dropping the waiter cannot drop
    /// SQL/publication work, its admission, or the caller's retained job guard.
    pub async fn execute(
        &self,
        action: ReportAction,
        context: SwapPolicyContext,
    ) -> Result<(), ImportError> {
        self.execute_observed(action, context).await.map(|_| ())
    }
    /// The owner, admission and physical work are identical to execute(). A
    /// caught error is returned as data only after its actual SQL reply settles.
    pub async fn execute_observed(
        &self,
        action: ReportAction,
        context: SwapPolicyContext,
    ) -> Result<ReportObservation, ImportError> {
        match &action {
            ReportAction::Committed(v)
                if v.total_epg_entries < 0
                    || v.mapped_tvg_ids > 100_000
                    || v.accepted > 5_000_000 =>
            {
                return Err(ImportError::Capacity("reporter counts"))
            }
            ReportAction::Staging {
                processed,
                mapped_tvg_ids,
                ..
            } if *processed > 5_000_000 || *mapped_tvg_ids > 100_000 => {
                return Err(ImportError::Capacity("reporter counts"))
            }
            ReportAction::NoMappings {
                total_epg_entries, ..
            } if *total_epg_entries > i64::MAX as u64 => {
                return Err(ImportError::Capacity("reporter counts"))
            }
            _ => {}
        }
        if action.charge() > 64 * 1024 {
            return Err(ImportError::Capacity("reporter text"));
        }
        if self.inner.state.stopping.load(Ordering::Acquire) {
            return Err(ImportError::Stopping);
        }
        let permit = self
            .inner
            .state
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| ImportError::Busy)?;
        if self.inner.state.stopping.load(Ordering::Acquire) {
            return Err(ImportError::Stopping);
        }
        let work = Arc::new(Work {
            permit: Some(permit),
            caller: Some(context.retained),
            state: self.inner.state.clone(),
        });
        let context = SwapPolicyContext {
            cancelled: context.cancelled,
            retained: work,
        };
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let result = inner.process(action, &context).await;
            drop(context);
            result
        })
        .await
        .map_err(|_| ImportError::Worker)?
    }
    /// A false result leaves physical tasks/guards intact; never abort SQL or a
    /// queued Redis command just to report clean shutdown.
    pub async fn shutdown(&self, timeout: Duration) -> bool {
        self.inner.state.stopping.store(true, Ordering::Release);
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.inner.state.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.physical_operations() == 0 {
                return true;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return self.physical_operations() == 0;
            }
        }
    }
}
impl Inner {
    async fn send(&self, data: Value, context: &SwapPolicyContext) {
        // The sink's retained entrypoint must move this guard through physical
        // publication commands when its logical command deadline elapses.
        let _ = self
            .sink
            .send_retained(
                "updates",
                json!({"type":"update","data":data}),
                context.retained.clone(),
            )
            .await;
    }
    async fn process(
        &self,
        action: ReportAction,
        context: &SwapPolicyContext,
    ) -> Result<ReportObservation, ImportError> {
        match action {
            ReportAction::Progress {
                source_id,
                progress,
            } => self.send(progress_data(source_id, progress), context).await,
            ReportAction::Staging {
                source_id,
                processed,
                mapped_tvg_ids,
            } => {
                let mut data = progress_data(source_id, staging_progress(processed));
                data["processed"] = json!(processed);
                data["channels"] = json!(mapped_tvg_ids);
                data["message"] = json!(format!("Staging programs... {}", commas(processed)));
                self.send(data, context).await;
            }
            ReportAction::NoMappings {
                source_id,
                total_epg_entries,
            } => {
                let message = format!(
                    "No channels mapped to this EPG source ({total_epg_entries} entries available)"
                );
                let saved = sqlx::query(
                    "UPDATE epg_epgsource SET status='success',last_message=$2 WHERE id=$1",
                )
                .bind(source_id)
                .bind(message)
                .execute(self.store.pool())
                .await?;
                saved_source(saved.rows_affected())?;
                let mut data = progress_data(source_id, 100);
                data["status"] = json!("success");
                self.send(data, context).await;
            }
            ReportAction::Committed(value) => {
                // Original invalidation swallows backend/server errors. Retired
                // chunk keys expire naturally; never delete an in-flight reader.
                let _: Result<(i64, _), _> = self
                    .transport
                    .command(
                        "return redis.call('INCR',KEYS[1])",
                        &["epg_content_generation".into()],
                        &[],
                        context.retained.clone(),
                    )
                    .await;
                let now = source_clock(&self.clock);
                let message = success_message(&value);
                let saved=sqlx::query("UPDATE epg_epgsource SET status='success',last_message=$2,updated_at=$3 WHERE id=$1").bind(value.source_id).bind(&message).bind(now).execute(self.store.pool()).await?;
                saved_source(saved.rows_affected())?;
                self.audit("epg_refresh",json!({"source_name":value.source_name,"programs":value.accepted,"channels":value.channels_with_programmes,"skipped_programs":value.skipped_unmapped,"unmapped_channels":value.total_epg_entries-value.mapped_tvg_ids as i64}),context).await;
                let mut data = progress_data(value.source_id, 100);
                data["status"] = json!("success");
                data["message"] = json!(message);
                data["updated_at"] = json!(source_timestamp(now));
                self.send(data, context).await;
            }
            ReportAction::Failed(value) => {
                return Ok(self.failure(value, "parsing_programs", context).await)
            }
            ReportAction::ChannelFailed(value) => {
                return Ok(self.failure(value, "parsing_channels", context).await)
            }
            ReportAction::RefreshFailed(value) => {
                return Ok(self.failure(value, "refresh", context).await)
            }
            ReportAction::DownloadFailed(value) => {
                return Ok(self.failure(value, "downloading", context).await)
            }
            ReportAction::LocalDownloadComplete { source_id } => {
                let saved = sqlx::query("UPDATE epg_epgsource SET status='success' WHERE id=$1")
                    .bind(source_id)
                    .execute(self.store.pool())
                    .await?;
                saved_source(saved.rows_affected())?;
                let mut data = stage_progress_data(source_id, "downloading", 100);
                data["status"] = json!("success");
                self.send(data, context).await;
            }
            ReportAction::ChannelProgress {
                source_id,
                progress,
                processed,
                total,
                total_channels,
            } => {
                let mut data = stage_progress_data(source_id, "parsing_channels", progress);
                if let Some(value) = processed {
                    data["processed"] = json!(value);
                }
                if let Some(value) = total {
                    data["total"] = json!(value);
                }
                if let Some(value) = total_channels {
                    data["total_channels"] = json!(value);
                }
                self.send(data, context).await;
            }
            ReportAction::ChannelsComplete {
                source_id,
                processed,
            } => {
                let saved = sqlx::query(
                    "UPDATE epg_epgsource SET status='success',last_message=$2 WHERE id=$1",
                )
                .bind(source_id)
                .bind(format!("Successfully parsed {processed} channels"))
                .execute(self.store.pool())
                .await?;
                saved_source(saved.rows_affected())?;
                let mut data = stage_progress_data(source_id, "parsing_channels", 100);
                data["status"] = json!("success");
                data["channels_count"] = json!(processed);
                self.send(data, context).await;
            }
        }
        Ok(ReportObservation::Ordinary)
    }
    async fn failure(
        &self,
        value: FailedImport,
        action: &str,
        context: &SwapPolicyContext,
    ) -> ReportObservation {
        // Pooled SQL is separate from the private staging session, so a
        // failed/rolled-back staging transaction cannot poison status.
        let saved = match sqlx::query(
            "UPDATE epg_epgsource SET status='error',last_message=$2 WHERE id=$1",
        )
        .bind(value.source_id)
        .bind(value.last_message)
        .execute(self.store.pool())
        .await
        {
            Ok(saved) => saved,
            Err(cause) => return ReportObservation::FailureStatusSqlCaught(cause),
        };
        {
            let mut data = stage_progress_data(value.source_id, action, 100);
            data["status"] = json!("error");
            data["error"] = json!(value.publication_error);
            self.send(data, context).await;
            self.audit(
                "epg_error",
                json!({"source_name":value.source_name,"message":value.publication_error}),
                context,
            )
            .await;
        }
        if saved.rows_affected() == 1 {
            ReportObservation::FailureStatusSaved
        } else {
            ReportObservation::FailureStatusUnmatched
        }
    }
    async fn audit(&self, kind: &str, details: Value, context: &SwapPolicyContext) {
        if sqlx::query("INSERT INTO core_systemevent(event_type,timestamp,channel_id,channel_name,details)VALUES($1,$3,NULL,NULL,$2)").bind(kind).bind(details).bind(source_clock(&self.clock)).execute(self.store.pool()).await.is_err(){return;}
        // External Connect/plugin event dispatch remains a separate M8 bridge.
        let settings = self
            .settings
            .retained_system_settings(context.clone())
            .await
            .unwrap_or(Value::Null);
        let Some(max) = max_system_events(settings.get("max_system_events")) else {
            return;
        };
        if max < 0 {
            return;
        }
        let _=async {
            let count:i64=sqlx::query_scalar("SELECT count(*) FROM core_systemevent").fetch_one(self.store.pool()).await?;
            if count>max {
                let cutoff:Option<i64>=sqlx::query_scalar("SELECT id::bigint FROM core_systemevent ORDER BY timestamp DESC LIMIT 1 OFFSET $1").bind(max).fetch_optional(self.store.pool()).await?;
                if let Some(id)=cutoff{sqlx::query("DELETE FROM core_systemevent WHERE id<$1").bind(id).execute(self.store.pool()).await?;}
            }
            Ok::<_,sqlx::Error>(())
        }.await;
    }
}
// Python timezone.now() has microsecond precision; bind the same truncated
// value that is serialized so PostgreSQL rounding cannot diverge from the event.
fn source_clock(clock: &PolicyClock) -> DateTime<Utc> {
    DateTime::from_timestamp_micros(clock().timestamp_micros()).unwrap()
}
fn saved_source(affected: u64) -> Result<(), ImportError> {
    if affected == 0 {
        return Err(
            StoreError::Unsupported("Save with update_fields did not affect any rows.").into(),
        );
    }
    Ok(())
}
pub fn staging_progress(processed: u64) -> u8 {
    85.min(10 + ((processed as f64 / (processed as f64 + 10000.0)) * 75.0) as u8)
}
fn stage_progress_data(source_id: i64, action: &str, progress: u8) -> Value {
    json!({"progress":progress,"type":"epg_refresh","source":source_id,"action":action})
}
fn progress_data(source_id: i64, progress: u8) -> Value {
    let mut data = stage_progress_data(source_id, "parsing_programs", progress);
    if progress == 10 {
        data["message"] = json!("Parsing programs...");
    }
    if progress == 90 {
        data["message"] = json!("Updating database...");
    }
    data
}
fn commas(value: u64) -> String {
    let raw = value.to_string();
    let mut result = String::new();
    for (index, ch) in raw.chars().enumerate() {
        if index > 0 && (raw.len() - index).is_multiple_of(3) {
            result.push(',');
        }
        result.push(ch);
    }
    result
}
fn success_message(value: &CompletedImport) -> String {
    format!(
        "Parsed {} programs for {} channels (skipped {} programs for {} unmapped channels)",
        commas(value.accepted),
        value.channels_with_programmes,
        commas(value.skipped_unmapped),
        value.total_epg_entries - value.mapped_tvg_ids as i64
    )
}
fn source_timestamp(value: DateTime<Utc>) -> String {
    let mut result = value.format("%Y-%m-%dT%H:%M:%S").to_string();
    let micros = value.timestamp_subsec_micros();
    if micros != 0 {
        result.push_str(&format!(".{micros:06}"));
    }
    result.push_str("+00:00");
    result
}
fn max_system_events(value: Option<&Value>) -> Option<i64> {
    match value {
        None | Some(Value::Null) | Some(Value::Bool(false)) => Some(100),
        Some(Value::Bool(true)) => Some(1),
        Some(Value::String(value)) if value.is_empty() => Some(100),
        Some(Value::String(value)) => {
            source_integer_literal(value).map_or(Some(100), |v| v.parse().ok())
        }
        Some(Value::Number(value)) => {
            if value.as_i64() == Some(0) || value.as_f64() == Some(0.0) {
                return Some(100);
            }
            if value.is_i64() {
                return value.as_i64();
            }
            if value.is_u64() {
                return value.as_i64();
            }
            value
                .as_f64()
                .filter(|v| v.is_finite() && *v >= i64::MIN as f64 && *v < i64::MAX as f64)
                .map(|v| v.trunc() as i64)
        }
        _ => Some(100),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_progress_message_and_timestamp_boundaries() {
        assert_eq!(staging_progress(5000), 35);
        assert_eq!(staging_progress(10000), 47);
        assert_eq!(staging_progress(5_000_000), 84);
        assert_eq!(commas(0), "0");
        assert_eq!(commas(5001), "5,001");
        assert_eq!(commas(1_000_000), "1,000,000");
        assert_eq!(
            source_timestamp("2026-11-09T12:00:00.123Z".parse().unwrap()),
            "2026-11-09T12:00:00.123000+00:00"
        );
        assert!(progress_data(1, 0).get("success").is_none());
        let clock: PolicyClock = Arc::new(|| "2026-11-09T12:00:00.123456789Z".parse().unwrap());
        assert_eq!(
            source_timestamp(source_clock(&clock)),
            "2026-11-09T12:00:00.123456+00:00"
        );
    }
    #[test]
    fn source_retention_truthiness_and_integer_spelling() {
        for value in [
            json!(null),
            json!(false),
            json!(0),
            json!(""),
            json!([]),
            json!({}),
            json!("invalid"),
        ] {
            assert_eq!(max_system_events(Some(&value)), Some(100));
        }
        for (value, expected) in [
            (json!(true), 1),
            (json!("0"), 0),
            (json!(0.3), 0),
            (json!(" １_０ "), 10),
            (json!(-1), -1),
        ] {
            assert_eq!(max_system_events(Some(&value)), Some(expected));
        }
    }
}
