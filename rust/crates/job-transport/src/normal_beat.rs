//! Normal configured Beat: one owned SQL session/cache/heap for clocked,
//! interval and crontab rows. Original Dispatcharr/Celery/django-celery-beat
//! authors retain all application/algorithm credit. No constructor starts it.
use crate::{
    database_schedule::{self, Entry},
    interval_schedule::PersistenceEffect,
    transport::{self, Jobs},
};
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{DateTime, Datelike, Timelike, Utc};
use dispatcharr_store::ProducerIdentitySource;
use serde_json::{json, Value};
use sqlx::{Connection, PgConnection, PgPool, Row};
use std::{
    collections::{BTreeMap, HashSet},
    sync::{Arc, Mutex as StateMutex, OnceLock},
    time::Duration,
};
use tokio::sync::{watch, Mutex, OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;
#[derive(Debug)]
pub enum Error {
    Closed,
    Capacity,
    Configuration(&'static str),
    Database(Box<sqlx::Error>),
    Settlement {
        primary: Option<Box<Error>>,
        ready: Option<Box<sqlx::Error>>,
        close: Option<Box<sqlx::Error>>,
    },
    Transport(transport::Error),
    Supervisor,
    Drain,
}
impl From<sqlx::Error> for Error {
    fn from(v: sqlx::Error) -> Self {
        Self::Database(Box::new(v))
    }
}
#[derive(Clone, Debug, Default)]
pub struct Tick {
    pub reloaded: bool,
    pub published: Option<Uuid>,
    pub publication_error: Option<transport::Error>,
    pub disabled: usize,
    pub synchronized: usize,
    pub missing: usize,
    pub invalid_rows: usize,
    pub delay_seconds: f64,
}
#[derive(Clone, Debug, Default)]
pub struct Setup {
    pub installed: Vec<String>,
    pub failed: Vec<(String, Option<String>)>,
}
#[derive(Clone, Debug)]
pub enum Status {
    Prepared,
    Running,
    Observed(Tick),
    Fault(Arc<Error>),
    Stopped,
}
struct Cached {
    entry: Entry,
    clocked: Option<DateTime<Utc>>,
    when: f64,
    identity: Arc<()>,
}
#[derive(Default)]
struct Cache {
    entries: BTreeMap<String, Cached>,
    dirty: HashSet<String>,
    loaded: bool,
    marker: Option<DateTime<Utc>>,
    last_full: Option<std::time::Instant>,
    last_sync: Option<std::time::Instant>,
    invalid_rows: usize,
}
struct Inner {
    pool: PgPool,
    jobs: Jobs,
    identity: ProducerIdentitySource,
    cache: Mutex<Cache>,
    lifecycle: Lifecycle,
    maximum_sleep: Duration,
    scope: ScheduleScope,
    backup_worker: Option<crate::normal_queue::CoreScheduledWorker>,
}
const BACKUP_PERIODIC_NAME: &str = "backup-scheduled-task";
const BACKUP_PERIODIC_TASK: &str = "apps.backups.tasks.scheduled_backup_task";
const CLEANUP_PERIODIC_TASK: &str = "celery.backend_cleanup";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScheduleScope {
    All,
    Backup,
    Cleanup,
}
impl ScheduleScope {
    fn row_identity(self) -> (Option<&'static str>, Option<&'static str>) {
        match self {
            Self::All => (None, None),
            Self::Backup => (Some(BACKUP_PERIODIC_NAME), Some(BACKUP_PERIODIC_TASK)),
            Self::Cleanup => (Some(CLEANUP_PERIODIC_TASK), Some(CLEANUP_PERIODIC_TASK)),
        }
    }
    fn admits(self, entry: &Entry) -> bool {
        if self == Self::All {
            return true;
        }
        if self == Self::Cleanup {
            return entry.name == CLEANUP_PERIODIC_TASK
                && entry.task == CLEANUP_PERIODIC_TASK
                && entry.interval.is_none()
                && entry.crontab.is_some()
                && !entry.one_off
                && entry.start_time.is_none()
                && entry.expires.is_none()
                && entry.expire_seconds == Some(43_200)
                && entry.queue.is_none()
                && entry.exchange.is_none()
                && entry.routing_key.is_none()
                && entry.priority.is_none()
                && entry
                    .headers
                    .as_object()
                    .is_some_and(|value| value.is_empty())
                && entry
                    .kwargs
                    .as_object()
                    .is_some_and(|value| value.is_empty())
                && entry.args.as_array().is_some_and(|value| value.is_empty());
        }
        entry.name == BACKUP_PERIODIC_NAME
            && entry.task == BACKUP_PERIODIC_TASK
            && entry.interval.is_none()
            && entry.crontab.is_some()
            && !entry.one_off
            && entry.start_time.is_none()
            && entry.expires.is_none()
            && entry.expire_seconds.is_none()
            && entry.queue.is_none()
            && entry.exchange.is_none()
            && entry.routing_key.is_none()
            && entry.priority.is_none()
            && entry
                .headers
                .as_object()
                .is_some_and(|header| header.is_empty())
            && entry.args.as_array().is_some_and(|args| args.is_empty())
            && entry.kwargs.as_object().is_some_and(|kwargs| {
                kwargs.len() == 1
                    && kwargs
                        .get("retention_count")
                        .and_then(Value::as_i64)
                        .is_some_and(|count| count >= 0)
            })
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Admission {
    Prepared,
    Started,
    Closed,
}
enum Operation {
    Initialize,
    Tick,
}
struct Lifecycle {
    admission: StateMutex<Admission>,
    slots: Arc<Semaphore>,
    stop: watch::Sender<bool>,
    done: watch::Sender<bool>,
    status: watch::Sender<Status>,
}
impl Lifecycle {
    fn new() -> Self {
        Self {
            admission: StateMutex::new(Admission::Prepared),
            slots: Arc::new(Semaphore::new(1)),
            stop: watch::channel(false).0,
            done: watch::channel(true).0,
            status: watch::channel(Status::Prepared).0,
        }
    }

    fn reserve(&self, operation: Operation) -> Result<OwnedSemaphorePermit, Error> {
        let admission = self.admission.lock().map_err(|_| Error::Supervisor)?;
        if *admission == Admission::Closed
            || (*admission == Admission::Started && matches!(operation, Operation::Initialize))
        {
            return Err(Error::Closed);
        }
        self.slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)
    }

    fn start(&self) -> Result<(), Error> {
        let mut admission = self.admission.lock().map_err(|_| Error::Supervisor)?;
        if *admission != Admission::Prepared {
            return Err(Error::Closed);
        }
        // Do not start the timer while setup or a manually admitted tick
        // owns the SQL/broker slot.
        let _slot = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        *admission = Admission::Started;
        self.done.send_replace(false);
        self.status.send_replace(Status::Running);
        Ok(())
    }

    fn close(&self) {
        let mut admission = self
            .admission
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        *admission = Admission::Closed;
        self.stop.send_replace(true);
    }

    fn finish(&self, status: Status) {
        self.status.send_replace(status);
        self.done.send_replace(true);
    }
}
struct Owner {
    _caller: Arc<dyn Send + Sync>,
    _slot: OwnedSemaphorePermit,
    _inner: Arc<Inner>,
}
#[derive(Clone)]
pub struct Scheduler {
    inner: Arc<Inner>,
}
impl Scheduler {
    /// The shared Main pool and shared normal Jobs connection are reused. Root
    /// replaces its clocked scheduler with this single producer; never run both.
    pub fn prepare(
        pool: PgPool,
        jobs: Jobs,
        identity: ProducerIdentitySource,
        maximum_sleep: Duration,
    ) -> Result<Self, Error> {
        Self::prepare_scoped(pool, jobs, identity, maximum_sleep, None)
    }

    /// Route a backup-only periodic owner through the same model/reload and
    /// confirmed broker publisher. Callers must replace their Beat loop, and
    /// own the matching normal backup queue adapter before starting this one.
    pub fn prepare_core_schedules(
        pool: PgPool,
        identity: ProducerIdentitySource,
        maximum_sleep: Duration,
        worker: crate::normal_queue::CoreScheduledWorker,
    ) -> Result<Self, Error> {
        Self::prepare_scoped(pool, worker.jobs(), identity, maximum_sleep, Some(worker))
    }

    fn prepare_scoped(
        pool: PgPool,
        jobs: Jobs,
        identity: ProducerIdentitySource,
        maximum_sleep: Duration,
        backup_worker: Option<crate::normal_queue::CoreScheduledWorker>,
    ) -> Result<Self, Error> {
        if !jobs.allows_defaults()
            || maximum_sleep.is_zero()
            || maximum_sleep > Duration::from_secs(5)
        {
            return Err(Error::Configuration("normal Beat ports/poll"));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                pool,
                jobs,
                identity,
                cache: Mutex::new(Cache::default()),
                lifecycle: Lifecycle::new(),
                maximum_sleep,
                scope: if backup_worker.is_some() {
                    ScheduleScope::Cleanup
                } else {
                    ScheduleScope::All
                },
                backup_worker,
            }),
        })
    }
    /// Source DatabaseScheduler installs backend cleanup plus the pinned app
    /// defaults before timer start. Existing disabled rows stay disabled except
    /// the explicitly disabled legacy stats row, which is always disabled.
    pub async fn initialize(&self, caller: Arc<dyn Send + Sync>) -> Result<Setup, Error> {
        let slot = self.inner.lifecycle.reserve(Operation::Initialize)?;
        let owner = Arc::new(Owner {
            _caller: caller,
            _slot: slot,
            _inner: self.inner.clone(),
        });
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let _owner = owner;
            let mut connection = inner.pool.acquire().await?.detach();
            let observed = if inner.scope == ScheduleScope::Cleanup {
                install_default(
                    &mut connection,
                    CLEANUP_PERIODIC_TASK,
                    CLEANUP_PERIODIC_TASK,
                    None,
                )
                .await
                .map(|()| Setup {
                    installed: vec![CLEANUP_PERIODIC_TASK.into()],
                    failed: Vec::new(),
                })
            } else {
                install_defaults(&mut connection).await
            };
            settle(connection, observed).await
        })
        .await
        .map_err(|_| Error::Supervisor)?
    }
    pub fn state(&self) -> Status {
        self.status()
    }
    pub fn status(&self) -> Status {
        self.inner.lifecycle.status.borrow().clone()
    }
    pub fn start(&self, caller: Arc<dyn Send + Sync>) -> Result<(), Error> {
        if self
            .inner
            .backup_worker
            .as_ref()
            .is_some_and(|worker| !worker.ready())
        {
            return Err(Error::Configuration(
                "backup schedule consumer is unavailable",
            ));
        }
        self.inner.lifecycle.start()?;
        let this = self.clone();
        let mut stop = self.inner.lifecycle.stop.subscribe();
        tokio::spawn(async move {
            while !*stop.borrow() {
                match this.tick_once(caller.clone()).await {
                    Ok(tick) => {
                        let delay = tick.delay_seconds;
                        this.inner
                            .lifecycle
                            .status
                            .send_replace(Status::Observed(tick));
                        if delay > 0.0 {
                            let duration =
                                Duration::from_secs_f64(delay).min(this.inner.maximum_sleep);
                            tokio::select! {_=tokio::time::sleep(duration)=>{},_=stop.changed()=>{}}
                        }
                    }
                    Err(Error::Closed) => break,
                    Err(error) => {
                        this.inner.lifecycle.finish(Status::Fault(Arc::new(error)));
                        return;
                    }
                }
            }
            this.inner.lifecycle.finish(Status::Stopped);
        });
        Ok(())
    }
    pub async fn tick_once(&self, caller: Arc<dyn Send + Sync>) -> Result<Tick, Error> {
        let slot = self.inner.lifecycle.reserve(Operation::Tick)?;
        if self
            .inner
            .backup_worker
            .as_ref()
            .is_some_and(|worker| !worker.ready())
        {
            return Ok(Tick {
                delay_seconds: self.inner.maximum_sleep.as_secs_f64(),
                ..Tick::default()
            });
        }
        let owner = Arc::new(Owner {
            _caller: caller,
            _slot: slot,
            _inner: self.inner.clone(),
        });
        let inner = self.inner.clone();
        // Detached on observer loss, but never aborted: one actual connection remains
        // owned through all statements, broker reply, Ready and physical close.
        tokio::spawn(async move {
            let mut connection = inner.pool.acquire().await?.detach();
            let observed = async {
                if inner.scope == ScheduleScope::Cleanup
                    && crate::normal_backup_schedule::try_lock_due_occurrence(&mut connection)
                        .await?
                {
                    let backup = tick_backup(&inner, &mut connection, owner.clone()).await?;
                    if backup.synchronized != 0 {
                        return Ok(backup);
                    }
                    if !crate::normal_backup_schedule::release_due_occurrence(&mut connection)
                        .await?
                    {
                        return Err(Error::Configuration("backup schedule lock settlement"));
                    }
                }
                tick(&inner, &mut connection, owner).await
            }
            .await;
            settle(connection, observed).await
        })
        .await
        .map_err(|_| Error::Supervisor)?
    }
    pub fn core_schedule_ready(&self) -> bool {
        !*self.inner.lifecycle.stop.borrow()
            && matches!(self.status(), Status::Observed(_))
            && self
                .inner
                .backup_worker
                .as_ref()
                .is_some_and(|worker| worker.ready())
    }

    pub fn stop_admission(&self) {
        self.inner.lifecycle.close();
    }
    pub async fn shutdown(&self, budget: Duration) -> Result<(), Error> {
        self.stop_admission();
        let deadline = tokio::time::Instant::now() + budget;
        let mut done = self.inner.lifecycle.done.subscribe();
        tokio::time::timeout_at(deadline, async {
            while !*done.borrow_and_update() {
                done.changed().await.map_err(|_| Error::Drain)?;
            }
            Ok::<(), Error>(())
        })
        .await
        .map_err(|_| Error::Drain)??;
        let slot =
            tokio::time::timeout_at(deadline, self.inner.lifecycle.slots.clone().acquire_owned())
                .await
                .map_err(|_| Error::Drain)?
                .map_err(|_| Error::Drain)?;
        if self.inner.cache.lock().await.dirty.is_empty() {
            drop(slot);
            return Ok(());
        }
        let inner = self.inner.clone();
        // Final dirty sync itself is physically retained when its observer times out.
        let task = tokio::spawn(async move {
            let _slot = slot;
            let mut connection = inner.pool.acquire().await?.detach();
            let mut cache = inner.cache.lock().await;
            let observed = sync(&mut connection, &mut cache).await.map(|_| ());
            settle(connection, observed).await
        });
        tokio::time::timeout_at(deadline, task)
            .await
            .map_err(|_| Error::Drain)?
            .map_err(|_| Error::Supervisor)?
    }
}

impl dispatcharr_store::BackupScheduleEnableApi for Scheduler {
    fn enable(
        &self,
        data: Value,
        caller: Arc<dyn Send + Sync>,
    ) -> dispatcharr_contracts::BoxFuture<'_, Result<Value, dispatcharr_store::StoreError>> {
        Box::pin(async move {
            let ready = self.core_schedule_ready()
                && data.get("enabled").and_then(Value::as_bool) == Some(true);
            let Some(worker) = self.inner.backup_worker.as_ref().filter(|_| ready) else {
                return Err(dispatcharr_store::StoreError::StatusBody(
                    501,
                    json!({"detail":"Backup schedule consumer is not prepared"}),
                ));
            };
            worker.enable_schedule(data, caller).await
        })
    }
}
async fn settle<T>(mut connection: PgConnection, observed: Result<T, Error>) -> Result<T, Error> {
    let ready = connection.ping().await.err().map(Box::new);
    let close = connection.close().await.err().map(Box::new);
    if ready.is_some() || close.is_some() {
        return Err(Error::Settlement {
            primary: observed.err().map(Box::new),
            ready,
            close,
        });
    }
    observed
}

fn due_matches_config(entry: &Entry, expression: &str, retention: i64) -> bool {
    let Some((minute, hour, weekday, day, month, _)) = &entry.crontab else {
        return false;
    };
    let current = [minute, hour, day, month, weekday];
    let expected: Vec<_> = expression.split_whitespace().map(cron_names).collect();
    current.len() == expected.len()
        && current
            .into_iter()
            .zip(expected)
            .all(|(live, wanted)| live == &wanted)
        && entry.kwargs["retention_count"].as_i64() == Some(retention)
}

async fn tick_backup(
    inner: &Inner,
    connection: &mut PgConnection,
    owner: Arc<Owner>,
) -> Result<Tick, Error> {
    let mut result = Tick {
        delay_seconds: 5.0,
        ..Tick::default()
    };
    // The detached session keeps its lock through the SQL reservation and
    // the real Redis result. The outer tick settles the same connection.
    let now = Utc::now();
    let (mut entries, invalid) = load(connection, now, ScheduleScope::Backup).await?;
    result.reloaded = true;
    result.invalid_rows = invalid;
    let Some(candidate) = entries.remove(BACKUP_PERIODIC_NAME) else {
        return Ok(result);
    };
    let due = decision(&candidate, now)?;
    if !due.due || due.persistence != PersistenceEffect::Unchanged {
        return Ok(result);
    }
    let Some((expression, retention)) =
        crate::normal_backup_schedule::live_due_configuration(connection)
            .await
            .map_err(|_| Error::Configuration("live backup schedule settings"))?
    else {
        return Ok(result);
    };
    let entry = candidate.entry;
    if !due_matches_config(&entry, &expression, retention)
        || !inner
            .backup_worker
            .as_ref()
            .is_some_and(|worker| worker.ready())
    {
        return Ok(result);
    }
    let count = entry
        .total_run_count
        .checked_add(1)
        .ok_or(Error::Capacity)?;
    let reserved = sqlx::query(concat!(
        "UPDATE django_celery_beat_periodictask ",
        "SET last_run_at=$1,total_run_count=$2,date_changed=$1 ",
        "WHERE enabled AND id=$3 AND name=$4 AND task=$5 AND total_run_count=$6"
    ))
    .bind(now)
    .bind(count)
    .bind(entry.id)
    .bind(BACKUP_PERIODIC_NAME)
    .bind(BACKUP_PERIODIC_TASK)
    .bind(entry.total_run_count)
    .execute(&mut *connection)
    .await?;
    if reserved.rows_affected() != 1 {
        return Ok(result);
    }
    result.synchronized = 1;
    let published = match prepare_publication(&entry, Uuid::new_v4(), &inner.identity) {
        Ok(publication) => inner.jobs.publish_normal_beat(&publication, owner).await,
        Err(_) => Err(transport::Error::Protocol),
    };
    match published {
        Ok(publication) => result.published = Some(publication.task_id),
        Err(error @ transport::Error::AmbiguousPublication(_)) => {
            return Err(Error::Transport(error))
        }
        Err(error) => result.publication_error = Some(error),
    }
    Ok(result)
}
fn adjusted(delay: f64) -> f64 {
    if delay > 0.0 {
        delay - 0.010
    } else {
        delay
    }
}
fn when(delay: f64) -> Result<f64, Error> {
    let now = Utc::now();
    let value = now.timestamp() as f64
        + f64::from(now.timestamp_subsec_micros()) / 1_000_000.0
        + adjusted(delay);
    if value.is_finite() {
        Ok(value)
    } else {
        Err(Error::Capacity)
    }
}
fn decision(
    entry: &Cached,
    now: DateTime<Utc>,
) -> Result<crate::interval_schedule::DueDecision, Error> {
    if let Some(clocked) = entry.clocked {
        let mut guard = entry.entry.clone();
        guard.interval = Some((0, "seconds".into()));
        guard.crontab = None;
        let mut due = database_schedule::tick::decision(&guard, now)
            .map_err(|_| Error::Configuration("clocked guard"))?;
        if due.due && due.persistence == PersistenceEffect::Unchanged {
            let remaining =
                (clocked - now).num_microseconds().ok_or(Error::Capacity)? as f64 / 1_000_000.0;
            due.due = remaining <= 0.0;
            due.next_seconds = if due.due { 100_000_000.0 } else { remaining };
        }
        Ok(due)
    } else {
        database_schedule::tick::decision(&entry.entry, now).map_err(|_| {
            Error::Configuration(
                "interval/crontab decision outside existing Source-qualified arithmetic/TZ profile",
            )
        })
    }
}
async fn disable(
    connection: &mut PgConnection,
    entry: &mut Cached,
    completed: bool,
    now: DateTime<Utc>,
) -> Result<(), Error> {
    let count = if completed {
        0
    } else {
        entry.entry.total_run_count
    };
    sqlx::query("UPDATE django_celery_beat_periodictask SET enabled=false,last_run_at=NULL,total_run_count=$2,date_changed=$3 WHERE id=$1").bind(entry.entry.id).bind(count).bind(now).execute(&mut *connection).await?;
    if completed {
        sqlx::query("INSERT INTO django_celery_beat_periodictasks(ident,last_update) VALUES(1,$1) ON CONFLICT(ident) DO UPDATE SET last_update=EXCLUDED.last_update").bind(now).execute(&mut *connection).await?;
    }
    entry.entry.enabled = false;
    entry.entry.total_run_count = count;
    Ok(())
}
async fn sync(connection: &mut PgConnection, cache: &mut Cache) -> Result<(usize, usize), Error> {
    let mut saved = 0;
    let mut missing = HashSet::new();
    while let Some(name) = cache.dirty.iter().next().cloned() {
        cache.dirty.remove(&name);
        let Some(entry) = cache.entries.get(&name) else {
            missing.insert(name);
            continue;
        };
        let result=sqlx::query("UPDATE django_celery_beat_periodictask SET last_run_at=CASE WHEN enabled THEN $2 ELSE NULL END,total_run_count=$3,date_changed=$4 WHERE id=$1")
            .bind(entry.entry.id).bind(entry.entry.last_run_at).bind(entry.entry.total_run_count).bind(Utc::now()).execute(&mut *connection).await;
        match result {
            Ok(rows) if rows.rows_affected() > 0 => saved += 1,
            Ok(_) => {
                missing.insert(name);
            }
            Err(sqlx::Error::Database(error)) => {
                // Source pops the attempted name before save. A DatabaseError does
                // not restore that name; missing names and not-yet-tried names stay.
                eprintln!(
                    "Periodic SQL sync rejected: {}",
                    error
                        .code()
                        .unwrap_or_default()
                        .chars()
                        .take(64)
                        .collect::<String>()
                );
                break;
            }
            Err(error) => {
                cache.dirty.extend(missing);
                return Err(error.into());
            }
        }
    }
    let count = missing.len();
    cache.dirty.extend(missing);
    Ok((saved, count))
}
async fn tick(
    inner: &Inner,
    connection: &mut PgConnection,
    owner: Arc<Owner>,
) -> Result<Tick, Error> {
    let now = Utc::now();
    let mut cache = inner.cache.lock().await;
    let mut result = Tick {
        delay_seconds: 5.0,
        ..Tick::default()
    };
    let marker: Option<DateTime<Utc>> = sqlx::query_scalar(
        "SELECT last_update FROM django_celery_beat_periodictasks WHERE ident=1",
    )
    .fetch_optional(&mut *connection)
    .await?;
    let changed = match (cache.marker, marker) {
        (Some(old), Some(new)) => new > old,
        _ => false,
    };
    let reload = !cache.loaded
        || changed
        || cache
            .last_full
            .is_none_or(|last| last.elapsed() >= Duration::from_secs(300));
    cache.marker = marker;
    if reload {
        let (saved, missing) = sync(connection, &mut cache).await?;
        result.synchronized += saved;
        result.missing += missing;
        let (entries, invalid) = load(connection, now, inner.scope).await?;
        cache.entries = entries;
        cache.loaded = true;
        cache.last_full = Some(std::time::Instant::now());
        result.reloaded = true;
        cache.invalid_rows = invalid;
    }
    result.invalid_rows = cache.invalid_rows;
    let selected = cache
        .entries
        .iter()
        .min_by(|(_, a), (_, b)| {
            a.when
                .total_cmp(&b.when)
                .then((Arc::as_ptr(&a.identity) as usize).cmp(&(Arc::as_ptr(&b.identity) as usize)))
        })
        .map(|(name, _)| name.clone());
    let Some(name) = selected else {
        return Ok(result);
    };
    let entry = cache.entries.get_mut(&name).ok_or(Error::Supervisor)?;
    let sampled = Utc::now();
    let due = decision(entry, sampled)?;
    if due.persistence != PersistenceEffect::Unchanged {
        disable(
            connection,
            entry,
            due.persistence == PersistenceEffect::DisableCompletedOneOff,
            sampled,
        )
        .await?;
        result.disabled += 1;
    }
    if !due.due {
        result.delay_seconds = adjusted(due.next_seconds).min(5.0);
        return Ok(result);
    }
    // Reserve BEFORE serialization/publication, retaining the advanced cached
    // count even on a known rejection. An ambiguous actual reply is never retried.
    entry.entry.last_run_at = Utc::now();
    entry.entry.total_run_count = entry
        .entry
        .total_run_count
        .checked_add(1)
        .ok_or(Error::Capacity)?;
    let snapshot = entry.entry.clone();
    cache.dirty.insert(name.clone());
    let id = Uuid::new_v4();
    let publication = prepare_publication(&snapshot, id, &inner.identity);
    let published = match publication {
        Ok(publication) => inner.jobs.publish_normal_beat(&publication, owner).await,
        Err(_) => Err(transport::Error::Protocol),
    };
    if cache
        .last_sync
        .is_none_or(|last| last.elapsed() > Duration::from_secs(180))
    {
        let (saved, missing) = sync(connection, &mut cache).await?;
        result.synchronized += saved;
        result.missing += missing;
        cache.last_sync = Some(std::time::Instant::now());
    }
    let entry = cache.entries.get_mut(&name).ok_or(Error::Supervisor)?;
    entry.when = when(due.next_seconds)?;
    result.delay_seconds = 0.0;
    match published {
        Ok(publication) => result.published = Some(publication.task_id),
        Err(error @ transport::Error::AmbiguousPublication(_)) => {
            return Err(Error::Transport(error))
        }
        Err(error) => result.publication_error = Some(error),
    };
    Ok(result)
}
fn text(value: &Value, key: &str) -> Result<String, Error> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or(Error::Configuration("periodic text field"))
}
fn optional_text(value: &Value, key: &str) -> Result<Option<String>, Error> {
    match value.get(key) {
        Some(Value::Null) => Ok(None),
        Some(Value::String(v)) => Ok(Some(v.clone())),
        _ => Err(Error::Configuration("periodic optional text")),
    }
}
fn integer(value: &Value, key: &str) -> Result<i64, Error> {
    value
        .get(key)
        .and_then(Value::as_i64)
        .ok_or(Error::Configuration("periodic integer field"))
}
fn optional_integer(value: &Value, key: &str) -> Result<Option<i32>, Error> {
    match value.get(key) {
        Some(Value::Null) => Ok(None),
        Some(v) => v
            .as_i64()
            .and_then(|v| i32::try_from(v).ok())
            .map(Some)
            .ok_or(Error::Configuration("periodic integer range")),
        None => Err(Error::Configuration("periodic integer absent")),
    }
}
fn date(value: &Value, key: &str) -> Result<Option<DateTime<Utc>>, Error> {
    optional_text(value, key)?
        .map(|v| {
            DateTime::parse_from_rfc3339(&v)
                .map(|v| v.with_timezone(&Utc))
                .map_err(|_| Error::Configuration("periodic date"))
        })
        .transpose()
}
// Remaining capacity is intentionally unchanged: >256 ELIGIBLE models or a
// >1MiB aggregate eligible reply refuses. Generic pagination is still open.
const LOAD_QUERY: &str = r#"WITH zone_offsets AS (SELECT * FROM unnest($4::text[],$5::integer[]) AS z(zone_name,hour_offset)),
candidates AS MATERIALIZED (
 SELECT to_jsonb(p)::text AS raw,
        (SELECT to_jsonb(i)::text FROM django_celery_beat_intervalschedule i WHERE i.id=p.interval_id) AS interval_spec,
        to_jsonb(cr)::text AS crontab_spec,
        cl.clocked_time
 FROM django_celery_beat_periodictask p
 LEFT JOIN django_celery_beat_clockedschedule cl ON cl.id=p.clocked_id
 LEFT JOIN django_celery_beat_crontabschedule cr ON cr.id=p.crontab_id
 LEFT JOIN zone_offsets z ON z.zone_name=cr.timezone
 WHERE p.enabled
   AND ($6::text IS NULL OR (p.name=$6 AND p.task=$7::text
        AND p.interval_id IS NULL AND p.solar_id IS NULL AND p.clocked_id IS NULL AND p.crontab_id IS NOT NULL))
   AND (p.clocked_id IS NULL OR cl.clocked_time IS NULL OR cl.clocked_time <= $1::timestamptz)
   AND (p.crontab_id IS NULL OR cr.id IS NULL OR NOT (cr.hour=ANY($3::text[])) OR z.hour_offset IS NULL
        OR CASE WHEN cr.hour=ANY($3::text[]) THEN (cr.hour::integer+z.hour_offset+24)%24 END=ANY($2::integer[]))
 LIMIT 257
),budget AS (
 SELECT count(*)::bigint AS rows,
 COALESCE(sum(octet_length(raw)+COALESCE(octet_length(interval_spec),0)+COALESCE(octet_length(crontab_spec),0)),0)::bigint AS bytes
 FROM candidates
)
SELECT b.rows,b.bytes,
 CASE WHEN b.rows<=256 AND b.bytes<=1048576 THEN c.raw END AS raw,
 CASE WHEN b.rows<=256 AND b.bytes<=1048576 THEN c.interval_spec END AS interval_spec,
 CASE WHEN b.rows<=256 AND b.bytes<=1048576 THEN c.crontab_spec END AS crontab_spec,
 CASE WHEN b.rows<=256 AND b.bytes<=1048576 THEN c.clocked_time END AS clocked_time
FROM budget b LEFT JOIN candidates c ON b.rows<=256 AND b.bytes<=1048576"#;
struct ZoneHours {
    names: Vec<&'static str>,
    offsets: Vec<i32>,
}
static ZONE_HOURS: OnceLock<ZoneHours> = OnceLock::new();

struct QueryFilters {
    cutoff: DateTime<Utc>,
    hours: Vec<i32>,
    numeric_hours: Vec<String>,
    zones: &'static [&'static str],
    offsets: &'static [i32],
}
impl QueryFilters {
    fn new(now: DateTime<Utc>) -> Result<Self, Error> {
        let cutoff = now
            .checked_add_signed(chrono::Duration::seconds(300))
            .ok_or(Error::Capacity)?;
        let mut hours = (0..5)
            .map(|n| (now.hour() as i32 + 22 + n) % 24)
            .collect::<Vec<_>>();
        hours.push(4);
        let numeric_hours = (0..24)
            .map(|h| h.to_string())
            .chain((0..10).map(|h| format!("{h:02}")))
            .collect();
        let ZoneHours { names, offsets } = ZONE_HOURS.get_or_init(|| {
            let (names, offsets) = database_schedule::normal_query_offsets().unzip();
            ZoneHours { names, offsets }
        });
        Ok(Self {
            cutoff,
            hours,
            numeric_hours,
            zones: names,
            offsets,
        })
    }
}
async fn load(
    connection: &mut PgConnection,
    now: DateTime<Utc>,
    scope: ScheduleScope,
) -> Result<(BTreeMap<String, Cached>, usize), Error> {
    // Source enabled_models_qs excludes far-future clocked and simple numeric
    // calendar hours BEFORE model loading. Apply those exact profile predicates
    // before LIMIT/serialization budget, not after an unrelated enabled-row cap.
    let filters = QueryFilters::new(now)?;
    let (name, task) = scope.row_identity();
    let rows = sqlx::query(LOAD_QUERY)
        .bind(filters.cutoff)
        .bind(&filters.hours)
        .bind(&filters.numeric_hours)
        .bind(filters.zones)
        .bind(filters.offsets)
        .bind(name)
        .bind(task)
        .fetch_all(&mut *connection)
        .await?;
    let budget = rows.first().ok_or(Error::Capacity)?;
    if budget.try_get::<i64, _>("rows")? > 256 || budget.try_get::<i64, _>("bytes")? > 1048576 {
        return Err(Error::Capacity);
    }
    let mut entries = BTreeMap::new();
    let mut invalid = 0;
    for row in rows {
        let Some(raw) = row.try_get::<Option<String>, _>("raw")? else {
            continue;
        };
        let value: Value =
            serde_json::from_str(&raw).map_err(|_| Error::Configuration("periodic row JSON"))?;
        let id = integer(&value, "id")?;
        let interval: Option<String> = row.try_get("interval_spec")?;
        let crontab: Option<String> = row.try_get("crontab_spec")?;
        let mut clocked: Option<DateTime<Utc>> = row.try_get("clocked_time")?;
        let interval = interval
            .map(|raw| {
                let v: Value =
                    serde_json::from_str(&raw).map_err(|_| Error::Configuration("interval row"))?;
                Ok::<_, Error>((
                    i32::try_from(integer(&v, "every")?).map_err(|_| Error::Capacity)?,
                    text(&v, "period")?,
                ))
            })
            .transpose()?;
        let crontab = crontab
            .map(|raw| {
                let v: Value =
                    serde_json::from_str(&raw).map_err(|_| Error::Configuration("crontab row"))?;
                Ok::<_, Error>((
                    text(&v, "minute")?,
                    text(&v, "hour")?,
                    text(&v, "day_of_week")?,
                    text(&v, "day_of_month")?,
                    text(&v, "month_of_year")?,
                    text(&v, "timezone")?,
                ))
            })
            .transpose()?;
        if clocked.is_some_and(|time| time > now + chrono::Duration::seconds(300)) {
            continue;
        }
        if let Some((_, hour, _, _, _, zone)) = &crontab {
            let offset = database_schedule::normal_query_offset(zone)
                .map_err(|_| Error::Configuration("crontab timezone"))?;
            if (0..24).any(|h| hour == &h.to_string() || (h < 10 && hour == &format!("{h:02}"))) {
                let h: i32 = hour
                    .parse()
                    .map_err(|_| Error::Configuration("crontab hour"))?;
                let shifted = (h + offset + 24) % 24;
                let current = now.hour() as i32;
                if shifted != 4 && !(0..5).any(|n| (current + 22 + n) % 24 == shifted) {
                    continue;
                }
            }
        }
        // Source's model property chooses interval before crontab before clocked.
        if interval.is_some() || crontab.is_some() {
            clocked = None;
        }
        if interval.is_none() && crontab.is_none() && clocked.is_none() {
            return Err(Error::Configuration(
                "missing/solar schedule outside this Beat profile",
            ));
        }
        let args = text(&value, "args")?;
        let kwargs = text(&value, "kwargs")?;
        let headers = text(&value, "headers")?;
        if [args.len(), kwargs.len(), headers.len()]
            .iter()
            .any(|v| *v > 65536)
        {
            return Err(Error::Capacity);
        }
        let args = serde_json::from_str::<Value>(if args.is_empty() { "[]" } else { &args });
        let kwargs = serde_json::from_str::<Value>(if kwargs.is_empty() { "{}" } else { &kwargs });
        let mut enabled = true;
        if args.is_err() || kwargs.is_err() {
            if scope != ScheduleScope::All {
                return Err(Error::Configuration("stored Core periodic arguments"));
            }
            sqlx::query("UPDATE django_celery_beat_periodictask SET enabled=false,last_run_at=NULL,date_changed=$2 WHERE id=$1").bind(id).bind(now).execute(&mut *connection).await?;
            enabled = false;
        }
        let headers =
            match serde_json::from_str::<Value>(if headers.is_empty() { "{}" } else { &headers }) {
                Ok(v) if v.is_object() => v,
                _ => {
                    invalid += 1;
                    continue;
                }
            };
        let start = date(&value, "start_time")?;
        let mut last = date(&value, "last_run_at")?
            .or(date(&value, "date_changed")?)
            .unwrap_or(now);
        if value["last_run_at"].is_null() && start.is_some() {
            last = last
                .checked_sub_signed(chrono::Duration::days(365 * 30))
                .ok_or(Error::Capacity)?;
        }
        let model = Entry {
            id,
            name: text(&value, "name")?,
            task: text(&value, "task")?,
            enabled,
            args: args.unwrap_or(Value::Null),
            kwargs: kwargs.unwrap_or(Value::Null),
            last_run_at: last,
            start_time: start,
            expires: date(&value, "expires")?,
            one_off: value["one_off"]
                .as_bool()
                .ok_or(Error::Configuration("one_off boolean"))?,
            headers,
            queue: optional_text(&value, "queue")?,
            exchange: optional_text(&value, "exchange")?,
            routing_key: optional_text(&value, "routing_key")?,
            priority: optional_integer(&value, "priority")?,
            expire_seconds: optional_integer(&value, "expire_seconds")?,
            total_run_count: integer(&value, "total_run_count")?,
            interval,
            crontab: crontab.map(|(minute, hour, weekday, day, month, zone)| {
                (
                    cron_names(&minute),
                    cron_names(&hour),
                    cron_names(&weekday),
                    cron_names(&day),
                    cron_names(&month),
                    zone,
                )
            }),
        };
        if !scope.admits(&model) {
            return Err(Error::Configuration("stored backup schedule admission"));
        }
        if model.crontab.is_some()
            && (!(1970..=2037).contains(&model.last_run_at.year())
                || !(1970..=2037).contains(&now.year()))
        {
            return Err(Error::Configuration(
                "calendar date outside pinned Source TZif horizon",
            ));
        }
        let mut cached = Cached {
            entry: model,
            clocked,
            when: 0.0,
            identity: Arc::new(()),
        };
        let due = match decision(&cached, now) {
            Ok(v) => v,
            Err(Error::Configuration(_)) => {
                invalid += 1;
                continue;
            }
            Err(error) => return Err(error),
        };
        if due.persistence != PersistenceEffect::Unchanged {
            disable(
                connection,
                &mut cached,
                due.persistence == PersistenceEffect::DisableCompletedOneOff,
                now,
            )
            .await?;
        }
        cached.when = when(if due.due { 0.0 } else { due.next_seconds })?;
        if entries.insert(cached.entry.name.clone(), cached).is_some() {
            return Err(Error::Configuration("duplicate periodic name"));
        }
    }
    Ok((entries, invalid))
}
/// Sealed by the actual model-entry reserve path; no caller-provided raw frame
/// is granted a generic publication hook. Unknown task names are still literal.
pub(crate) struct PreparedPublication {
    id: Uuid,
    bytes: Vec<u8>,
    queue: String,
    exchange_key: String,
    binding: String,
}
impl PreparedPublication {
    pub(crate) fn id(&self) -> Uuid {
        self.id
    }
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub(crate) fn redis_queue(&self) -> &str {
        &self.queue
    }
    pub(crate) fn exchange_key(&self) -> &str {
        &self.exchange_key
    }
    pub(crate) fn binding(&self) -> &str {
        &self.binding
    }
}
fn prepare_publication(
    entry: &Entry,
    id: Uuid,
    identity: &ProducerIdentitySource,
) -> Result<PreparedPublication, Error> {
    let identity_value = identity
        .current()
        .map_err(|_| Error::Configuration("actual producer identity"))?;
    let (argsrepr, kwargsrepr) = identity
        .periodic_representations(&entry.args, &entry.kwargs)
        .map_err(|_| Error::Configuration("periodic repr capacity/types"))?;
    let default_queue = if entry.task == "apps.channels.tasks.run_recording" {
        "dvr"
    } else {
        "celery"
    };
    let queue = entry.queue.as_deref().unwrap_or(default_queue);
    let routing = entry.routing_key.as_deref().unwrap_or(queue);
    let exchange = entry.exchange.as_deref().unwrap_or("");
    if queue.is_empty() || queue.len() > 4096 || routing.len() > 4096 || exchange.len() > 4096 {
        return Err(Error::Capacity);
    }
    let priority = entry.priority.unwrap_or(0);
    if !(0..=255).contains(&priority) {
        return Err(Error::Configuration("priority range"));
    }
    let expires = entry.expires.or_else(|| {
        entry
            .expire_seconds
            .filter(|v| *v != 0)
            .and_then(|v| Utc::now().checked_add_signed(chrono::Duration::seconds(i64::from(v))))
    });
    let base = crate::protocol::encode(
        &crate::protocol::Task::CreateBackup,
        id,
        identity_value.reply_to,
        Uuid::new_v4(),
        &identity_value.origin,
    )
    .map_err(|_| Error::Configuration("periodic base envelope"))?;
    let mut frame: Value =
        serde_json::from_slice(&base).map_err(|_| Error::Configuration("periodic base JSON"))?;
    frame["body"]=json!(STANDARD.encode(serde_json::to_vec(&json!([entry.args,entry.kwargs,{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])).map_err(|_|Error::Configuration("periodic body"))?));
    frame["headers"]["task"] = json!(entry.task);
    if entry.task == crate::normal_xc_live::SINGLE_TASK_NAME {
        frame["headers"]["timelimit"] = json!(crate::normal_xc_live::SINGLE_TASK_TIMELIMIT);
    } else if entry.task == crate::refresh_protocol::NAME {
        frame["headers"]["timelimit"] =
            json!([crate::refresh_protocol::HARD_TIME_LIMIT_SECONDS, null]);
    }
    frame["headers"]["argsrepr"] = json!(argsrepr);
    frame["headers"]["kwargsrepr"] = json!(kwargsrepr);
    frame["headers"]["expires"] = expires
        .map(|v| json!(v.to_rfc3339_opts(chrono::SecondsFormat::Micros, false)))
        .unwrap_or(Value::Null);
    let supplied = entry
        .headers
        .as_object()
        .ok_or(Error::Configuration("periodic headers"))?;
    for (key, value) in supplied {
        frame["headers"][key] = value.clone();
    }
    frame["headers"]["periodic_task_name"] = json!(entry.name);
    frame["properties"]["delivery_info"] = json!({"exchange":exchange,"routing_key":routing});
    frame["properties"]["priority"] = json!(priority);
    if let Some(expires) = expires {
        let milliseconds = (expires - Utc::now()).num_milliseconds().max(0);
        frame["properties"]["expiration"] = json!(milliseconds.to_string());
    }
    let bytes = serde_json::to_vec(&frame).map_err(|_| Error::Configuration("periodic frame"))?;
    if bytes.len() > 65536 {
        return Err(Error::Capacity);
    }
    let bucket = if priority >= 9 {
        9
    } else if priority >= 6 {
        6
    } else if priority >= 3 {
        3
    } else {
        0
    };
    let redis_queue = if bucket == 0 {
        queue.to_owned()
    } else {
        format!("{queue}\u{6}\u{16}{bucket}")
    };
    Ok(PreparedPublication {
        id,
        bytes,
        queue: redis_queue,
        exchange_key: if exchange.is_empty() {
            queue.into()
        } else {
            exchange.into()
        },
        binding: format!("{routing}\u{6}\u{16}\u{6}\u{16}{queue}"),
    })
}
async fn install_defaults(connection: &mut PgConnection) -> Result<Setup, Error> {
    let specifications = [
        ("celery.backend_cleanup", "celery.backend_cleanup", None),
        (
            "fetch-channel-statuses",
            "apps.proxy.tasks.fetch_channel_stats",
            Some(2),
        ),
        ("scan-files", "core.tasks.scan_and_process_files", Some(20)),
        (
            "maintain-recurring-recordings",
            "apps.channels.tasks.maintain_recurring_recordings",
            Some(3600),
        ),
        (
            "check-version-updates",
            "core.tasks.check_for_version_update",
            Some(86400),
        ),
        (
            "check-account-expirations",
            "apps.m3u.tasks.check_account_expirations",
            Some(86400),
        ),
    ];
    let mut result = Setup::default();
    for (name, task, every) in specifications {
        let observed = install_default(connection, name, task, every).await;
        match observed {
            Ok(()) => result.installed.push(name.into()),
            Err(Error::Database(error)) => {
                let code = error
                    .as_database_error()
                    .and_then(|v| v.code())
                    .map(|v| v.chars().take(64).collect());
                result.failed.push((name.into(), code));
            }
            Err(error) => return Err(error),
        }
    }
    Ok(result)
}
async fn install_default(
    connection: &mut PgConnection,
    name: &str,
    task: &str,
    every: Option<i32>,
) -> Result<(), Error> {
    let (mut interval, mut cron) = (None, None);
    if let Some(every) = every {
        let existing:Option<i64>=sqlx::query_scalar("SELECT id::bigint FROM django_celery_beat_intervalschedule WHERE every=$1 AND period='seconds' ORDER BY id LIMIT 1").bind(every).fetch_optional(&mut *connection).await?;
        interval=Some(match existing{Some(id)=>id,None=>sqlx::query_scalar("INSERT INTO django_celery_beat_intervalschedule(every,period) VALUES($1,'seconds') RETURNING id::bigint").bind(every).fetch_one(&mut *connection).await?});
    } else {
        let existing:Option<i64>=sqlx::query_scalar("SELECT id::bigint FROM django_celery_beat_crontabschedule WHERE minute='0' AND hour='4' AND day_of_week='*' AND day_of_month='*' AND month_of_year='*' AND timezone='UTC' ORDER BY id LIMIT 1").fetch_optional(&mut *connection).await?;
        cron=Some(match existing{Some(id)=>id,None=>sqlx::query_scalar("INSERT INTO django_celery_beat_crontabschedule(minute,hour,day_of_week,day_of_month,month_of_year,timezone) VALUES('0','4','*','*','*','UTC') RETURNING id::bigint").fetch_one(&mut *connection).await?});
    }
    // Model update_or_create/save + tracking signal share this inner transaction;
    // creating its schedule FK above is Source's separate earlier effect boundary.
    let mut transaction = connection.begin().await?;
    let update=sqlx::query("INSERT INTO django_celery_beat_periodictask(name,task,interval_id,crontab_id,args,kwargs,headers,enabled,total_run_count,date_changed,description,one_off,expire_seconds) VALUES($1,$2,$3,$4,'[]','{}','{}',$5,0,$6,'',false,$7) ON CONFLICT(name) DO UPDATE SET task=EXCLUDED.task,interval_id=EXCLUDED.interval_id,crontab_id=EXCLUDED.crontab_id,solar_id=NULL,clocked_id=NULL,args='[]',kwargs='{}',headers='{}',queue=NULL,exchange=NULL,routing_key=NULL,priority=NULL,expire_seconds=EXCLUDED.expire_seconds,enabled=CASE WHEN EXCLUDED.name='fetch-channel-statuses' THEN false ELSE django_celery_beat_periodictask.enabled END,last_run_at=CASE WHEN NOT django_celery_beat_periodictask.enabled OR EXCLUDED.name='fetch-channel-statuses' THEN NULL ELSE django_celery_beat_periodictask.last_run_at END,date_changed=EXCLUDED.date_changed").bind(name).bind(task).bind(interval).bind(cron).bind(name!="fetch-channel-statuses").bind(Utc::now()).bind(if every.is_none(){Some(43200i32)}else{None}).execute(&mut *transaction).await;
    if let Err(error) = update {
        transaction.rollback().await?;
        return Err(error.into());
    }
    let marker=sqlx::query("INSERT INTO django_celery_beat_periodictasks(ident,last_update) VALUES(1,$1) ON CONFLICT(ident) DO UPDATE SET last_update=EXCLUDED.last_update").bind(Utc::now()).execute(&mut *transaction).await;
    if let Err(error) = marker {
        transaction.rollback().await?;
        return Err(error.into());
    }
    transaction.commit().await?;
    Ok(())
}

fn cron_names(raw: &str) -> String {
    let months = [
        "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
    ];
    let days = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"];
    let token = |word: &str, step: bool| -> String {
        if let Some(number) = dispatcharr_ingest_core::source_integer_literal(word) {
            if step {
                return number
                    .parse::<usize>()
                    .map(|n| n.min(4096).to_string())
                    .unwrap_or_else(|_| "4096".into());
            }
            return number;
        }
        if !step {
            let abbreviation = word
                .chars()
                .take(3)
                .flat_map(char::to_lowercase)
                .collect::<String>();
            if let Some(index) = months.iter().position(|v| *v == abbreviation) {
                return (index + 1).to_string();
            }
            if let Some(index) = days.iter().position(|v| *v == abbreviation) {
                return index.to_string();
            }
        }
        word.to_owned()
    };
    let (mut out, mut word, mut step) = (String::new(), String::new(), false);
    for c in raw.chars().chain(std::iter::once('\0')) {
        if c.is_alphanumeric() || c == '_' {
            word.push(c);
        } else {
            if !word.is_empty() {
                out.push_str(&token(&word, step));
                word.clear();
            }
            if c == '\0' {
                break;
            }
            out.push(c);
            if c == '/' {
                step = true;
            }
            if c == ',' {
                step = false;
            }
        }
    }
    out
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheduler_lifecycle_keeps_dormant_stop_closed() {
        let lifecycle = Lifecycle::new();
        lifecycle.close();
        assert!(matches!(*lifecycle.status.borrow(), Status::Prepared));
        assert!(*lifecycle.done.borrow());
        assert!(*lifecycle.stop.borrow());
        assert!(matches!(lifecycle.start(), Err(Error::Closed)));
        assert!(matches!(
            lifecycle.reserve(Operation::Initialize),
            Err(Error::Closed)
        ));
        assert!(matches!(
            lifecycle.reserve(Operation::Tick),
            Err(Error::Closed)
        ));
    }

    #[test]
    fn scheduler_lifecycle_owns_setup_before_start_and_timer_before_drain() {
        let lifecycle = Lifecycle::new();
        let setup = lifecycle.reserve(Operation::Initialize).unwrap();
        assert!(matches!(lifecycle.start(), Err(Error::Capacity)));
        assert!(matches!(*lifecycle.status.borrow(), Status::Prepared));
        assert!(*lifecycle.done.borrow());
        drop(setup);
        lifecycle.start().unwrap();
        assert!(!*lifecycle.done.borrow());
        assert!(matches!(*lifecycle.status.borrow(), Status::Running));
        assert!(matches!(lifecycle.start(), Err(Error::Closed)));
        assert!(matches!(
            lifecycle.reserve(Operation::Initialize),
            Err(Error::Closed)
        ));
        let active_tick = lifecycle.reserve(Operation::Tick).unwrap();
        lifecycle.close();
        assert!(*lifecycle.stop.borrow());
        assert!(!*lifecycle.done.borrow());
        assert!(lifecycle.slots.try_acquire().is_err());
        assert!(matches!(
            lifecycle.reserve(Operation::Tick),
            Err(Error::Closed)
        ));
        drop(active_tick);
        lifecycle.finish(Status::Stopped);
        assert!(*lifecycle.done.borrow());
        assert!(matches!(*lifecycle.status.borrow(), Status::Stopped));
        assert!(lifecycle.slots.try_acquire().is_ok());
    }
    fn model(task: &str) -> Entry {
        let now = DateTime::parse_from_rfc3339("2026-01-05T12:00:00+00:00")
            .unwrap()
            .with_timezone(&Utc);
        Entry {
            id: 7,
            name: "configured-periodic".into(),
            task: task.into(),
            enabled: true,
            args: json!([]),
            kwargs: json!({}),
            last_run_at: now,
            start_time: None,
            expires: None,
            one_off: false,
            headers: json!({}),
            queue: None,
            exchange: None,
            routing_key: None,
            priority: None,
            expire_seconds: None,
            total_run_count: 0,
            interval: Some((5, "seconds".into())),
            crontab: None,
        }
    }
    #[test]
    fn backup_scheduler_selects_only_the_owned_standard_periodic_model() {
        let mut entry = model(BACKUP_PERIODIC_TASK);
        entry.name = BACKUP_PERIODIC_NAME.into();
        entry.interval = None;
        entry.crontab = Some((
            "10".into(),
            "14".into(),
            "2".into(),
            "*".into(),
            "*".into(),
            "UTC".into(),
        ));
        entry.kwargs = json!({"retention_count": 3});
        assert_eq!(
            ScheduleScope::Backup.row_identity(),
            (Some(BACKUP_PERIODIC_NAME), Some(BACKUP_PERIODIC_TASK))
        );
        assert_eq!(ScheduleScope::All.row_identity(), (None, None));
        assert!(ScheduleScope::Backup.admits(&entry));
        assert!(due_matches_config(&entry, "10 14 * * 2", 3));
        assert!(!due_matches_config(&entry, "11 14 * * 2", 3));
        assert!(!due_matches_config(&entry, "10 14 * * 2", 2));
        assert!(!due_matches_config(&entry, "10 14 * *", 3));
        let mut other = entry.clone();
        other.name = "other-periodic-backup".into();
        assert!(!ScheduleScope::Backup.admits(&other));
        assert!(ScheduleScope::All.admits(&other));
        other = entry.clone();
        other.task = "apps.backups.tasks.create_backup_task".into();
        assert!(!ScheduleScope::Backup.admits(&other));
        other = entry.clone();
        other.queue = Some("custom".into());
        assert!(!ScheduleScope::Backup.admits(&other));
        other = entry;
        other.kwargs = json!({"retention_count": -1});
        assert!(!ScheduleScope::Backup.admits(&other));
    }
    #[test]
    fn core_cleanup_scope_keeps_the_ordinary_same_named_backend_task() {
        let mut entry = model(CLEANUP_PERIODIC_TASK);
        entry.name = CLEANUP_PERIODIC_TASK.into();
        entry.interval = None;
        entry.crontab = Some((
            "0".into(),
            "4".into(),
            "*".into(),
            "*".into(),
            "*".into(),
            "UTC".into(),
        ));
        entry.expire_seconds = Some(43_200);
        assert_eq!(
            ScheduleScope::Cleanup.row_identity(),
            (Some(CLEANUP_PERIODIC_TASK), Some(CLEANUP_PERIODIC_TASK))
        );
        assert!(ScheduleScope::Cleanup.admits(&entry));
        assert!(!ScheduleScope::Backup.admits(&entry));
        entry.name = BACKUP_PERIODIC_NAME.into();
        assert!(!ScheduleScope::Cleanup.admits(&entry));
    }
    #[test]
    fn cached_backup_timezone_change_reaches_the_next_honolulu_cron_boundary() {
        // Actual batch11: UTC backup ran05:12; API changed the cached timezone
        // then installed19:13 Honolulu, which is05:13 UTC on the following date.
        let now = DateTime::parse_from_rfc3339("2026-11-15T05:13:00+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let filters = QueryFilters::new(now).unwrap();
        let index = filters
            .zones
            .iter()
            .position(|zone| *zone == "Pacific/Honolulu")
            .unwrap();
        assert_eq!(filters.offsets[index], 10);
        assert!(filters
            .hours
            .contains(&((19 + filters.offsets[index]) % 24)));
        let mut entry = model("apps.backups.tasks.scheduled_backup_task");
        entry.last_run_at = now - chrono::Duration::minutes(1);
        entry.interval = None;
        entry.crontab = Some((
            "13".into(),
            "19".into(),
            "*".into(),
            "*".into(),
            "*".into(),
            "Pacific/Honolulu".into(),
        ));
        let mut cached = Cached {
            entry,
            clocked: None,
            when: 0.0,
            identity: Arc::new(()),
        };
        let actual = decision(&cached, now).unwrap();
        assert!(actual.due);
        cached.entry.last_run_at = now;
        assert!(!decision(&cached, now).unwrap().due);
        assert!(
            !decision(&cached, now - chrono::Duration::seconds(1))
                .unwrap()
                .due
        );
    }
    #[test]
    fn saved_timezone_catalogue_drives_existing_sql_window_and_reload() {
        let winter = DateTime::parse_from_rfc3339("2026-01-05T10:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let summer = DateTime::parse_from_rfc3339("2026-07-06T09:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let first = QueryFilters::new(winter).unwrap();
        let second = QueryFilters::new(summer).unwrap();
        assert!(std::ptr::eq(first.zones, second.zones));
        assert!(std::ptr::eq(first.offsets, second.offsets));
        for (name, known) in [
            ("America/Chicago", 6),
            ("America/St_Johns", 3),
            ("Asia/Kathmandu", -5),
            ("Pacific/Chatham", -13),
            ("US/Eastern", 5),
            ("Africa/Casablanca", -1),
        ] {
            let index = first.zones.binary_search(&name).unwrap();
            assert_eq!(first.offsets[index], known);
            assert_eq!(database_schedule::normal_query_offset(name).unwrap(), known);
        }
        for (now, filters) in [(winter, first), (summer, second)] {
            let chicago = filters.zones.binary_search(&"America/Chicago").unwrap();
            assert!(filters
                .hours
                .contains(&((4 + filters.offsets[chicago]) % 24)));
            assert!(!filters
                .hours
                .contains(&((18 + filters.offsets[chicago]) % 24)));
            let mut entry = model("celery.backend_cleanup");
            entry.last_run_at = now - chrono::Duration::minutes(1);
            entry.interval = None;
            entry.expire_seconds = Some(43200);
            entry.crontab = Some((
                "30".into(),
                "4".into(),
                "*".into(),
                "*".into(),
                "*".into(),
                "America/Chicago".into(),
            ));
            let mut cached = Cached {
                entry,
                clocked: None,
                when: 0.0,
                identity: Arc::new(()),
            };
            assert!(decision(&cached, now).unwrap().due);
            assert!(
                !decision(&cached, now - chrono::Duration::seconds(1))
                    .unwrap()
                    .due
            );
            cached.entry.last_run_at = now;
            assert!(!decision(&cached, now).unwrap().due);
        }
    }
    #[test]
    fn named_fields_keep_the_existing_numeric_calendar_constructor_unchanged() {
        assert_eq!(cron_names("Monday-Friday"), "1-5");
        assert_eq!(cron_names("JAN,March-December/2"), "1,3-12/2");
        assert_eq!(cron_names("*/1_0"), "*/10");
        assert_eq!(cron_names("*/Monday"), "*/Monday");
        assert_eq!(cron_names("5-1"), "5-1");
    }
    #[test]
    fn all_interval_units_and_clocked_use_the_same_start_expiry_one_off_guards() {
        let now = DateTime::parse_from_rfc3339("2026-01-05T12:00:00+00:00")
            .unwrap()
            .with_timezone(&Utc);
        for (unit, micros) in [
            ("days", 86_400_000_000i64),
            ("hours", 3_600_000_000),
            ("minutes", 60_000_000),
            ("seconds", 1_000_000),
            ("microseconds", 1),
        ] {
            let mut entry = model("unregistered.literal.task");
            entry.interval = Some((1, unit.into()));
            entry.last_run_at = now - chrono::Duration::microseconds(micros);
            let mut cached = Cached {
                entry,
                clocked: None,
                when: 0.0,
                identity: Arc::new(()),
            };
            assert!(decision(&cached, now).unwrap().due);
            cached.entry.expires = Some(now);
            assert_eq!(
                decision(&cached, now).unwrap().persistence,
                PersistenceEffect::DisableExpired
            );
        }
        let mut cached = Cached {
            entry: model("apps.channels.tasks.run_recording"),
            clocked: Some(now),
            when: 0.0,
            identity: Arc::new(()),
        };
        cached.entry.interval = None;
        cached.entry.one_off = true;
        assert!(decision(&cached, now).unwrap().due);
        cached.entry.total_run_count = 1;
        assert_eq!(
            decision(&cached, now).unwrap().persistence,
            PersistenceEffect::DisableCompletedOneOff
        );
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn unknown_configured_name_is_literal_and_uses_real_identity_list_repr_and_queue() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(8)
            .connect_lazy("postgresql://127.0.0.1/unused_metadata_only")
            .unwrap();
        let auth = dispatcharr_store::AuthService::new(
            dispatcharr_store::Store::from_pool(pool),
            dispatcharr_store::AuthConfig::new("unused-metadata-only".into()).unwrap(),
        );
        let identity = auth.producer_identity_source();
        let id = Uuid::new_v4();
        let mut entry = model("not.installed.literal.task");
        entry.args = json!([1, "café"]);
        entry.kwargs = json!({"flag":true});
        entry.headers =
            json!({"owned_header":"kept","periodic_task_name":"must_not_replace_model_name"});
        let publication = prepare_publication(&entry, id, &identity).unwrap();
        let frame: Value = serde_json::from_slice(publication.bytes()).unwrap();
        let body: Value =
            serde_json::from_slice(&STANDARD.decode(frame["body"].as_str().unwrap()).unwrap())
                .unwrap();
        assert_eq!(frame["headers"]["task"], entry.task);
        assert_eq!(frame["headers"]["periodic_task_name"], entry.name);
        assert_eq!(frame["headers"]["argsrepr"], "[1, 'café']");
        assert_eq!(frame["headers"]["kwargsrepr"], "{'flag': True}");
        assert_eq!(body[0], entry.args);
        assert_eq!(body[1], entry.kwargs);
        assert_eq!(
            Uuid::parse_str(frame["properties"]["reply_to"].as_str().unwrap())
                .unwrap()
                .get_version_num(),
            3
        );
        assert!(frame["headers"]["origin"]
            .as_str()
            .unwrap()
            .starts_with(&format!("gen{}@", std::process::id())));
        assert_eq!(publication.redis_queue(), "celery");
        entry.task = "apps.channels.tasks.run_recording".into();
        entry.args = json!([]);
        entry.kwargs = json!({});
        let publication = prepare_publication(&entry, id, &identity).unwrap();
        let frame: Value = serde_json::from_slice(publication.bytes()).unwrap();
        assert_eq!(publication.redis_queue(), "dvr");
        assert_eq!(frame["headers"]["argsrepr"], "()");
        entry.priority = Some(7);
        assert_eq!(
            prepare_publication(&entry, id, &identity)
                .unwrap()
                .redis_queue(),
            "dvr\u{6}\u{16}6"
        );
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn scheduled_live_xc_frame_reaches_normal_consumer_with_source_time_limits() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(8)
            .connect_lazy("postgresql://127.0.0.1/unused_metadata_only")
            .unwrap();
        let auth = dispatcharr_store::AuthService::new(
            dispatcharr_store::Store::from_pool(pool),
            dispatcharr_store::AuthConfig::new("unused-metadata-only".into()).unwrap(),
        );
        let mut entry = model(crate::normal_xc_live::SINGLE_TASK_NAME);
        entry.name = "m3u_account-refresh-5".into();
        entry.args = json!([]);
        entry.kwargs = json!({"account_id": 5});
        let id = Uuid::new_v4();
        let publication =
            prepare_publication(&entry, id, &auth.producer_identity_source()).unwrap();
        assert_eq!(publication.redis_queue(), "celery");
        let mut frame: Value = serde_json::from_slice(publication.bytes()).unwrap();
        assert_eq!(frame["headers"]["periodic_task_name"], entry.name);
        assert_eq!(frame["headers"]["timelimit"], json!([3600, 3500]));
        let parsed = crate::normal_xc_live::parse(publication.bytes()).unwrap();
        assert_eq!(parsed.id, id);
        assert_eq!(parsed.root, id);
        assert_eq!(parsed.account, 5);
        assert!(parsed.parent.is_none());
        frame["headers"]["timelimit"] = json!([null, null]);
        assert!(crate::normal_xc_live::parse(&serde_json::to_vec(&frame).unwrap()).is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn scheduled_xmltv_frame_reaches_existing_refresh_parser() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(8)
            .connect_lazy("postgresql://127.0.0.1/unused_metadata_only")
            .unwrap();
        let auth = dispatcharr_store::AuthService::new(
            dispatcharr_store::Store::from_pool(pool),
            dispatcharr_store::AuthConfig::new("unused-metadata-only".into()).unwrap(),
        );
        let mut entry = model(crate::refresh_protocol::NAME);
        entry.name = "epg_source-refresh-5".into();
        entry.args = json!([]);
        entry.kwargs = json!({"source_id": 5});
        let id = Uuid::new_v4();
        let publication =
            prepare_publication(&entry, id, &auth.producer_identity_source()).unwrap();
        assert_eq!(publication.redis_queue(), "celery");
        let mut frame: Value = serde_json::from_slice(publication.bytes()).unwrap();
        assert_eq!(frame["headers"]["periodic_task_name"], entry.name);
        assert_eq!(frame["headers"]["timelimit"], json!([14400, null]));
        let parsed = crate::refresh_protocol::parse(publication.bytes()).unwrap();
        assert_eq!(parsed.task_id, id);
        assert_eq!(parsed.root, id);
        assert_eq!(parsed.source_id, 5);
        assert!(parsed.parent.is_none() && parsed.eta.is_none());
        assert!(!parsed.force);
        assert_eq!(parsed.retry, 0);
        frame["headers"]["timelimit"] = json!([null, null]);
        assert!(crate::refresh_protocol::parse(&serde_json::to_vec(&frame).unwrap()).is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn scheduled_cleanup_frame_reaches_existing_normal_consumer_parser() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(8)
            .connect_lazy("postgresql://127.0.0.1/unused_metadata_only")
            .unwrap();
        let auth = dispatcharr_store::AuthService::new(
            dispatcharr_store::Store::from_pool(pool),
            dispatcharr_store::AuthConfig::new("unused-metadata-only".into()).unwrap(),
        );
        let mut entry = model("celery.backend_cleanup");
        entry.name = "celery.backend_cleanup".into();
        entry.expire_seconds = Some(43200);
        let id = Uuid::new_v4();
        let publication =
            prepare_publication(&entry, id, &auth.producer_identity_source()).unwrap();
        let frame: Value = serde_json::from_slice(publication.bytes()).unwrap();
        assert_eq!(publication.redis_queue(), "celery");
        assert_eq!(frame["headers"]["ignore_result"], false);
        assert_eq!(frame["headers"]["argsrepr"], "()");
        assert_eq!(frame["headers"]["periodic_task_name"], entry.name);
        let parsed = crate::default_protocol::parse(publication.bytes()).unwrap();
        assert_eq!(parsed.task_id, id);
        assert_eq!(
            parsed.kind,
            crate::default_protocol::Kind::RedisBackendCleanup
        );
        assert!(parsed.expires.is_some());
        assert_eq!(
            crate::default_protocol::Normal::Null
                .value(parsed.kind)
                .unwrap(),
            Value::Null
        );
        // No silent ignore-result adaptation or arbitrary header authority.
        let mut ignored = frame.clone();
        ignored["headers"]["ignore_result"] = json!(true);
        assert!(crate::default_protocol::parse(&serde_json::to_vec(&ignored).unwrap()).is_err());
        let mut foreign = frame;
        foreign["headers"]["foreign_authority"] = json!(true);
        assert!(crate::default_protocol::parse(&serde_json::to_vec(&foreign).unwrap()).is_err());
    }
}

#[cfg(test)]
#[path = "normal_beat_query_tests.rs"]
mod query_tests;

#[path = "plugin_startup_schedule.rs"]
mod plugin_startup_schedule;
pub use plugin_startup_schedule::PluginRepositoryScheduleAdapter;
