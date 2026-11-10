//! Experimental, app-unwired orphan task cleanup from original Dispatcharr.
//! Original authors retain full application/algorithm credit (AGPL); Django and
//! django-celery-beat authors retain collector/model/signal credit/licenses.
//! Authority: Source orphan79. Only observed nonconcurrent collector shapes and
//! settled PostgreSQL P0001 faults are supported; other faults stay operational.
use crate::cache_policy::PolicyClock;
use sqlx::{pool::PoolConnection, PgPool, Postgres, Row};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Phase {
    Lookup,
    TaskCollector,
    TaskModelMarker,
    IntervalCollector,
}
#[derive(Debug)]
pub struct CleanupReport {
    pub found: bool,
    pub helper_return: bool,
    pub task_committed: bool,
    pub model_marker_committed: bool,
    pub interval_committed: bool,
    pub settled_failure: Option<(Phase, sqlx::Error)>,
}
impl CleanupReport {
    fn missing() -> Self {
        Self {
            found: false,
            helper_return: false,
            task_committed: false,
            model_marker_committed: false,
            interval_committed: false,
            settled_failure: None,
        }
    }
}
#[derive(Debug)]
pub enum CleanupError {
    Busy,
    Stopping,
    InvalidConfig,
    InvalidSource,
    Capacity,
    Worker,
    /// A raw cause is retained; no claim of a Source caught-failure result.
    Operational {
        phase: Phase,
        cause: sqlx::Error,
    },
    /// Source concurrent graph changes require a separately observed collector.
    ConcurrentGraph,
}
impl std::fmt::Display for CleanupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Operational { phase, cause } => write!(f, "orphan cleanup {phase:?}: {cause}"),
            other => write!(f, "orphan cleanup {other:?}"),
        }
    }
}
impl std::error::Error for CleanupError {}
#[derive(Clone, Copy)]
pub struct CleanupConfig {
    pub max_jobs: usize,
    pub max_related_rows: i64,
}
impl Default for CleanupConfig {
    fn default() -> Self {
        Self {
            max_jobs: 1,
            max_related_rows: 1000,
        }
    }
}
struct Inner {
    pool: PgPool,
    clock: PolicyClock,
    config: CleanupConfig,
    slots: Arc<Semaphore>,
    stopping: AtomicBool,
    changed: Notify,
}
#[derive(Clone)]
pub struct OrphanCleanup {
    inner: Arc<Inner>,
}
struct Retained {
    inner: Arc<Inner>,
    _permit: Option<OwnedSemaphorePermit>,
    _caller: Option<Arc<dyn Send + Sync>>,
}
impl Drop for Retained {
    fn drop(&mut self) {
        drop(self._caller.take());
        drop(self._permit.take());
        self.inner.changed.notify_waiters();
    }
}
impl OrphanCleanup {
    pub fn new(
        pool: PgPool,
        clock: PolicyClock,
        config: CleanupConfig,
    ) -> Result<Self, CleanupError> {
        if !(1..=4).contains(&config.max_jobs) || !(1..=100_000).contains(&config.max_related_rows)
        {
            return Err(CleanupError::InvalidConfig);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                pool,
                clock,
                config,
                slots: Arc::new(Semaphore::new(config.max_jobs)),
                stopping: AtomicBool::new(false),
                changed: Notify::new(),
            }),
        })
    }
    pub fn physical_jobs(&self) -> usize {
        self.inner.config.max_jobs - self.inner.slots.available_permits()
    }
    /// The spawned owner retains caller, connection and admission through actual
    /// queries/commit/rollback/close after logical waiter abort. No statement is
    /// retried. Shutdown closes intake; it does not cancel accepted SQL midway.
    pub async fn cleanup(
        &self,
        source_id: i64,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<CleanupReport, CleanupError> {
        if source_id <= 0 {
            return Err(CleanupError::InvalidSource);
        }
        if self.inner.stopping.load(Ordering::Acquire) {
            return Err(CleanupError::Stopping);
        }
        let permit = self.inner.slots.clone().try_acquire_owned().map_err(|_| {
            if self.inner.stopping.load(Ordering::Acquire) {
                CleanupError::Stopping
            } else {
                CleanupError::Busy
            }
        })?;
        let retained = Retained {
            inner: self.inner.clone(),
            _permit: Some(permit),
            _caller: Some(caller),
        };
        tokio::spawn(async move {
            let result = retained.inner.run(source_id).await;
            drop(retained);
            result
        })
        .await
        .map_err(|_| CleanupError::Worker)?
    }
    pub async fn shutdown(&self, timeout: Duration) -> bool {
        self.inner.stopping.store(true, Ordering::Release);
        self.inner.slots.close();
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.physical_jobs() == 0 {
                return true;
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return self.physical_jobs() == 0;
            }
        }
    }
}
impl Inner {
    async fn run(&self, source_id: i64) -> Result<CleanupReport, CleanupError> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(|cause| CleanupError::Operational {
                phase: Phase::Lookup,
                cause,
            })?;
        let row = sqlx::query(
            "SELECT id, interval_id FROM django_celery_beat_periodictask WHERE name=$1",
        )
        .bind(format!("epg_source-refresh-{source_id}"))
        .fetch_optional(&mut *conn)
        .await
        .map_err(|cause| CleanupError::Operational {
            phase: Phase::Lookup,
            cause,
        })?;
        let Some(row) = row else {
            return Ok(CleanupReport::missing());
        };
        let task: i32 = row.get("id");
        let interval: Option<i32> = row.get("interval_id");
        // Source obtains the relation and reference count BEFORE deleting task.
        let interval = if let Some(id) = interval {
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM django_celery_beat_intervalschedule WHERE id=$1)",
            )
            .bind(id)
            .fetch_one(&mut *conn)
            .await
            .map_err(|cause| CleanupError::Operational {
                phase: Phase::Lookup,
                cause,
            })?;
            if !exists {
                return Err(CleanupError::ConcurrentGraph);
            }
            Some(id)
        } else {
            None
        };
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM django_celery_beat_periodictask WHERE interval_id=$1",
        )
        .bind(interval)
        .fetch_one(&mut *conn)
        .await
        .map_err(|cause| CleanupError::Operational {
            phase: Phase::Lookup,
            cause,
        })?;
        let related: i64 = sqlx::query_scalar("SELECT (SELECT COUNT(*) FROM epg_epgsource WHERE refresh_task_id=$1)+(SELECT COUNT(*) FROM m3u_m3uaccount WHERE refresh_task_id=$1)")
            .bind(task).fetch_one(&mut *conn).await.map_err(|cause| CleanupError::Operational { phase: Phase::Lookup, cause })?;
        if count > self.config.max_related_rows || related > self.config.max_related_rows {
            return Err(CleanupError::Capacity);
        }
        let mut report = CleanupReport {
            found: true,
            ..CleanupReport::missing()
        };
        for phase in [
            Phase::TaskCollector,
            Phase::TaskModelMarker,
            Phase::IntervalCollector,
        ] {
            if phase == Phase::IntervalCollector && (interval.is_none() || count != 1) {
                break;
            }
            if phase == Phase::IntervalCollector {
                // Source skips a disappeared interval. New relationships would
                // require cascading additional tasks; that graph is unaccepted.
                let remaining: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM django_celery_beat_periodictask WHERE interval_id=$1",
                )
                .bind(interval)
                .fetch_one(&mut *conn)
                .await
                .map_err(|cause| CleanupError::Operational { phase, cause })?;
                if remaining != 0 {
                    return Err(CleanupError::ConcurrentGraph);
                }
                let exists: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM django_celery_beat_intervalschedule WHERE id=$1)",
                )
                .bind(interval)
                .fetch_one(&mut *conn)
                .await
                .map_err(|cause| CleanupError::Operational { phase, cause })?;
                if !exists {
                    break;
                }
            }
            let (next, result) = self.phase(conn, phase, task, interval).await;
            match next {
                Some(value) => conn = value,
                None => {
                    let cause = match result {
                        Err(PhaseFailure::Operational(cause))
                        | Err(PhaseFailure::Settled(cause)) => cause,
                        Ok(()) => unreachable!(),
                    };
                    return Err(CleanupError::Operational { phase, cause });
                }
            }
            match result {
                Ok(()) => match phase {
                    Phase::TaskCollector => report.task_committed = true,
                    Phase::TaskModelMarker => report.model_marker_committed = true,
                    Phase::IntervalCollector => report.interval_committed = true,
                    Phase::Lookup => unreachable!(),
                },
                Err(failure) => {
                    // Only the actually observed, definitely settled P0001
                    // path maps helper False. No generic SQLSTATE equivalence.
                    match failure {
                        PhaseFailure::Settled(cause)
                            if cause.as_database_error().and_then(|e| e.code()).as_deref()
                                == Some("P0001") =>
                        {
                            report.settled_failure = Some((phase, cause));
                            return Ok(report);
                        }
                        PhaseFailure::Settled(cause) | PhaseFailure::Operational(cause) => {
                            return Err(CleanupError::Operational { phase, cause })
                        }
                    }
                }
            }
        }
        report.helper_return = true;
        Ok(report)
    }
    async fn marker(&self, conn: &mut PoolConnection<Postgres>) -> Result<(), sqlx::Error> {
        // Mirror Source's existing-row lock + UPDATE, or INSERT when absent.
        // In particular an UPSERT would fire INSERT and UPDATE triggers on an
        // existing row, changing the observed marker fault ordering.
        let existing: Option<i16> = sqlx::query_scalar(
            "SELECT ident FROM django_celery_beat_periodictasks WHERE ident=1 FOR UPDATE",
        )
        .fetch_optional(&mut **conn)
        .await?;
        if existing.is_some() {
            sqlx::query("UPDATE django_celery_beat_periodictasks SET last_update=$1 WHERE ident=1")
                .bind((self.clock)())
                .execute(&mut **conn)
                .await?;
        } else {
            sqlx::query(
                "INSERT INTO django_celery_beat_periodictasks (ident,last_update) VALUES (1,$1)",
            )
            .bind((self.clock)())
            .execute(&mut **conn)
            .await?;
        }
        Ok(())
    }
    async fn statements(
        &self,
        conn: &mut PoolConnection<Postgres>,
        phase: Phase,
        task: i32,
        interval: Option<i32>,
    ) -> Result<(), sqlx::Error> {
        self.marker(conn).await?;
        match phase {
            Phase::TaskCollector => {
                sqlx::query(
                    "UPDATE epg_epgsource SET refresh_task_id=NULL WHERE refresh_task_id=$1",
                )
                .bind(task)
                .execute(&mut **conn)
                .await?;
                sqlx::query(
                    "UPDATE m3u_m3uaccount SET refresh_task_id=NULL WHERE refresh_task_id=$1",
                )
                .bind(task)
                .execute(&mut **conn)
                .await?;
                sqlx::query("DELETE FROM django_celery_beat_periodictask WHERE id=$1")
                    .bind(task)
                    .execute(&mut **conn)
                    .await?;
            }
            Phase::IntervalCollector => {
                sqlx::query("DELETE FROM django_celery_beat_intervalschedule WHERE id=$1")
                    .bind(interval)
                    .execute(&mut **conn)
                    .await?;
            }
            Phase::TaskModelMarker => (),
            Phase::Lookup => unreachable!(),
        }
        Ok(())
    }
    async fn phase(
        &self,
        mut conn: PoolConnection<Postgres>,
        phase: Phase,
        task: i32,
        interval: Option<i32>,
    ) -> (Option<PoolConnection<Postgres>>, Result<(), PhaseFailure>) {
        if let Err(cause) = sqlx::query("BEGIN").execute(&mut *conn).await {
            let _ = conn.close().await;
            return (None, Err(PhaseFailure::Operational(cause)));
        }
        let result = match self.statements(&mut conn, phase, task, interval).await {
            Ok(()) => sqlx::query("COMMIT").execute(&mut *conn).await.map(|_| ()),
            Err(cause) => Err(cause),
        };
        match result {
            Ok(()) => (Some(conn), Ok(())),
            Err(cause) => {
                let definite = cause.as_database_error().is_some();
                if sqlx::query("ROLLBACK").execute(&mut *conn).await.is_err() {
                    // Explicitly await the physical close before releasing
                    // admission/caller. Never return a dirty pool connection.
                    let _ = conn.close().await;
                    return (None, Err(PhaseFailure::Operational(cause)));
                }
                (
                    Some(conn),
                    if definite {
                        Err(PhaseFailure::Settled(cause))
                    } else {
                        Err(PhaseFailure::Operational(cause))
                    },
                )
            }
        }
    }
}
enum PhaseFailure {
    Settled(sqlx::Error),
    Operational(sqlx::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn missing_is_false_without_commits() {
        let value = CleanupReport::missing();
        assert!(
            !value.found
                && !value.helper_return
                && !value.task_committed
                && !value.interval_committed
        );
    }
    #[tokio::test]
    async fn config_bounds_do_not_overflow() {
        let pool = PgPool::connect_lazy("postgres://fixture@127.0.0.1/fixture").unwrap();
        let clock = Arc::new(|| chrono::DateTime::from_timestamp(0, 0).unwrap());
        assert!(matches!(
            OrphanCleanup::new(
                pool.clone(),
                clock.clone(),
                CleanupConfig {
                    max_jobs: usize::MAX,
                    max_related_rows: 1
                }
            ),
            Err(CleanupError::InvalidConfig)
        ));
        assert!(matches!(
            OrphanCleanup::new(
                pool,
                clock,
                CleanupConfig {
                    max_jobs: 1,
                    max_related_rows: i64::MAX
                }
            ),
            Err(CleanupError::InvalidConfig)
        ));
    }
}

/// Fixture peer only: copied database, fixed missing-source range, no worker or
/// application route authority. The Python controller owns setup/fault triggers.
#[tokio::test]
#[ignore = "requires copied Original orphan79 PostgreSQL fixture"]
async fn native_orphan_cleanup_peer() {
    use std::io::Read;
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let database = std::env::var("POSTGRES_DB").unwrap();
    assert!(
        database.starts_with("rust_probe_backup_jobs_")
            && database.len() <= 63
            && database
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    );
    let mut input = Vec::new();
    std::io::stdin().take(4097).read_to_end(&mut input).unwrap();
    assert!(input.len() <= 4096);
    let request: serde_json::Value = serde_json::from_slice(&input).unwrap();
    let source = request["source_id"].as_i64().unwrap();
    assert!((9_000_000..9_000_100).contains(&source));
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(
            sqlx::postgres::PgConnectOptions::new()
                .host("127.0.0.1")
                .port(5432)
                .username("dispatch")
                .password("playback-fixture-only")
                .database(&database),
        )
        .await
        .unwrap();
    let actual: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(actual, database);
    let cleanup = OrphanCleanup::new(
        pool.clone(),
        Arc::new(|| chrono::DateTime::from_timestamp(1_609_459_200, 0).unwrap()),
        CleanupConfig::default(),
    )
    .unwrap();
    if request["action"] == "resources" {
        use std::sync::atomic::AtomicUsize;
        struct CountDrop(Arc<AtomicUsize>);
        impl Drop for CountDrop {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let drops = Arc::new(AtomicUsize::new(0));
        let control = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect_with(
                sqlx::postgres::PgConnectOptions::new()
                    .host("127.0.0.1")
                    .port(5432)
                    .username("dispatch")
                    .password("playback-fixture-only")
                    .database(&database),
            )
            .await
            .unwrap();
        let mut lock = control.begin().await.unwrap();
        sqlx::query("SELECT ident FROM django_celery_beat_periodictasks WHERE ident=1 FOR UPDATE")
            .execute(&mut *lock)
            .await
            .unwrap();
        let owned = cleanup.clone();
        let caller = Arc::new(CountDrop(drops.clone()));
        let waiting = tokio::spawn(async move { owned.cleanup(source, caller).await });
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            sqlx::query("SELECT pg_stat_clear_snapshot()")
                .execute(&mut *lock)
                .await
                .unwrap();
            let blocked: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'SELECT ident FROM django_celery_beat_periodictasks%'")
                .fetch_one(&mut *lock).await.unwrap();
            if blocked == 1 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "actual row-lock waiter required"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        waiting.abort();
        let _ = waiting.await;
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(cleanup.physical_jobs(), 1);
        assert!(matches!(
            cleanup.cleanup(source, Arc::new(())).await,
            Err(CleanupError::Busy)
        ));
        assert!(!cleanup.shutdown(Duration::from_millis(20)).await);
        lock.commit().await.unwrap();
        assert!(cleanup.shutdown(Duration::from_secs(3)).await);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        // The owned task DELETE trigger raises P0001 after the marker wait;
        // settled rollback must preserve task and marker, and return pool1 clean.
        let still_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM django_celery_beat_periodictask WHERE name=$1)",
        )
        .bind(format!("epg_source-refresh-{source}"))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(still_exists);
        let assigned: Option<String> =
            sqlx::query_scalar("SELECT txid_current_if_assigned()::text")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(assigned.is_none());
        let second = OrphanCleanup::new(
            pool.clone(),
            Arc::new(|| chrono::DateTime::from_timestamp(1_609_459_200, 0).unwrap()),
            CleanupConfig::default(),
        )
        .unwrap();
        let held = pool.acquire().await.unwrap();
        let owned = second.clone();
        let caller = Arc::new(CountDrop(drops.clone()));
        let waiting = tokio::spawn(async move { owned.cleanup(source, caller).await });
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while second.physical_jobs() == 0 {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::task::yield_now().await;
        }
        waiting.abort();
        let _ = waiting.await;
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(!second.shutdown(Duration::from_millis(20)).await);
        drop(held);
        assert!(second.shutdown(Duration::from_secs(3)).await);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
        assert_eq!(second.physical_jobs(), 0);
        assert!(matches!(
            second.cleanup(source, Arc::new(())).await,
            Err(CleanupError::Stopping)
        ));
        control.close().await;
        pool.close().await;
        println!(
            "NATIVE_ORPHAN_JSON={}",
            serde_json::json!({"resource_cases":2,"row_lock_abort_retained":true,"pool_acquire_abort_retained":true,"p0001_rollback_settled":true,"physical_jobs":0,"caller_drops":2})
        );
        return;
    }
    let report = cleanup.cleanup(source, Arc::new(())).await.unwrap();
    let failure = report.settled_failure.as_ref().map(|(phase,cause)| serde_json::json!({"phase":format!("{phase:?}"),"code":cause.as_database_error().and_then(|e|e.code()).map(|e|e.to_string())}));
    assert!(cleanup.shutdown(Duration::from_secs(2)).await);
    assert_eq!(cleanup.physical_jobs(), 0);
    pool.close().await;
    println!(
        "NATIVE_ORPHAN_JSON={}",
        serde_json::json!({"helper_return":report.helper_return,"found":report.found,"task_committed":report.task_committed,"model_marker_committed":report.model_marker_committed,"interval_committed":report.interval_committed,"failure":failure,"physical_jobs":0})
    );
}
