//! Normal configured Beat: one owned SQL session/cache/heap for clocked,
//! interval and crontab rows. Original Dispatcharr/Celery/django-celery-beat
//! authors retain all application/algorithm credit. No constructor starts it.
use crate::{
    database_schedule::{self, Entry},
    interval_schedule::PersistenceEffect,
    normal_queue::SelectedScheduleAvailability::{self, Ready, Terminal, Waiting},
    transport::{self, Jobs},
};
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{DateTime, Datelike, Timelike, Utc};
use dispatcharr_store::ProducerIdentitySource;
use serde_json::{json, Value};
use sqlx::{Connection, PgConnection, PgPool, Row};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex as StateMutex, OnceLock},
    time::Duration,
};
use tokio::sync::{oneshot, watch, Mutex, OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;
#[path = "normal_beat_restore_pause.rs"]
mod restore_pause;
pub(crate) use restore_pause::{RestorePause, RestorePauseClient};
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
    RoleDatabaseTimeout,
    SavedCleanupUncertain(Uuid),
    Drain,
}
impl From<sqlx::Error> for Error {
    fn from(v: sqlx::Error) -> Self {
        Self::Database(Box::new(v))
    }
}
/// Fixed vocabulary only. Read solely on failed, opt-in shutdown diagnostics.
/// No runtime errors, SQL, task names, IDs or values escape this observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShutdownDiagnostic {
    pub timer: &'static str,
    pub slot: &'static str,
    pub cache: &'static str,
    pub checkpoint: &'static str,
    pub state: &'static str,
    pub fault: &'static str,
}
fn shutdown_dirty_labels(dirty: bool, orphaned: bool) -> (&'static str, &'static str) {
    if !dirty {
        ("Clean", "None")
    } else if orphaned {
        ("Dirty", "Orphaned")
    } else {
        ("Dirty", "Present")
    }
}
fn shutdown_fault_label(error: &Error) -> &'static str {
    match error {
        Error::Drain => "Drain",
        Error::Database(_) => "Database",
        Error::Configuration(_) => "Configuration",
        Error::RoleDatabaseTimeout => "RoleDatabaseTimeout",
        Error::Supervisor => "Supervisor",
        Error::Settlement { primary, .. } => match primary.as_deref() {
            Some(Error::Drain) => "SettlementDrain",
            Some(Error::Database(_)) => "SettlementDatabase",
            _ => "SettlementOther",
        },
        _ => "Other",
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
    pub saved_cleanup: Option<SavedCleanupObservation>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SavedCleanupObservation {
    pub id: i64,
    pub total_run_count: i64,
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
    Standby,
    Contended,
    Acquired,
    Observed(Tick),
    ReceiverFault,
    SettlingFault,
    Fault(Arc<Error>),
    Stopped,
}
struct Cached {
    entry: Entry,
    clocked: Option<DateTime<Utc>>,
    saved_xc: bool,
    when: f64,
    identity: Arc<()>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Checkpoint {
    Required,
    PublishedClockedDvr(i64),
}

impl Checkpoint {
    fn after_publication(linked_clocked_dvr: bool, acknowledged: bool, periodic_id: i64) -> Self {
        if linked_clocked_dvr && acknowledged {
            Self::PublishedClockedDvr(periodic_id)
        } else {
            Self::Required
        }
    }

    fn can_confirm_retirement(self, periodic_id: i64) -> bool {
        self == Self::PublishedClockedDvr(periodic_id)
    }
}

#[derive(Default)]
struct Cache {
    entries: BTreeMap<String, Cached>,
    dirty: HashMap<String, Checkpoint>,
    loaded: bool,
    marker: Option<DateTime<Utc>>,
    last_full: Option<std::time::Instant>,
    last_sync: Option<std::time::Instant>,
    invalid_rows: usize,
    saved_cleanup: Option<SavedCleanupSnapshot>,
}
struct Inner {
    pool: PgPool,
    jobs: Jobs,
    identity: ProducerIdentitySource,
    cache: Mutex<Cache>,
    lifecycle: Lifecycle,
    restore_pause: restore_pause::Control,
    maximum_sleep: Duration,
    scope: ScheduleScope,
    saved_cleanup: Option<SavedCleanupSchedule>,
    backup_worker: Option<ScheduleConsumer>,
}

enum ScheduleConsumer {
    Core(crate::normal_queue::CoreScheduledWorker),
    Selected(crate::normal_queue::SelectedCoreSchedules),
}

impl ScheduleConsumer {
    fn ready(&self) -> bool {
        match self {
            Self::Core(worker) => worker.ready(),
            Self::Selected(worker) => worker.ready(),
        }
    }

    fn owns_plugin_refresh(&self) -> bool {
        match self {
            Self::Core(worker) => worker.owns_plugin_refresh(),
            Self::Selected(worker) => worker.owns_plugin_refresh(),
        }
    }

    fn enable_schedule(
        &self,
        data: Value,
        caller: Arc<dyn Send + Sync>,
    ) -> dispatcharr_contracts::BoxFuture<'_, Result<Value, dispatcharr_store::StoreError>> {
        match self {
            Self::Core(worker) => worker.enable_schedule(data, caller),
            Self::Selected(worker) => worker.enable_schedule(data, caller),
        }
    }
}

const BACKUP_PERIODIC_NAME: &str = "backup-scheduled-task";
const BACKUP_PERIODIC_TASK: &str = "apps.backups.tasks.scheduled_backup_task";
const CLEANUP_PERIODIC_TASK: &str = "celery.backend_cleanup";
const CLOCKED_DVR_TASK: &str = "apps.channels.tasks.run_recording";
const EXPIRATION_PERIODIC_NAME: &str = "check-account-expirations";
const EXPIRATION_PERIODIC_TASK: &str = "apps.m3u.tasks.check_account_expirations";
const VERSION_PERIODIC_NAME: &str = "check-version-updates";
const DAILY_SECONDS: i32 = 86_400;
const RESERVED_PERIODIC_NAMES: &[&str] = &[
    CLEANUP_PERIODIC_TASK,
    VERSION_PERIODIC_NAME,
    EXPIRATION_PERIODIC_NAME,
    SCAN_FILES.0,
    MAINTAIN_RECURRING.0,
    DISABLED_STATS_DEFAULT,
    BACKUP_PERIODIC_NAME,
    plugin_startup_schedule::NAME,
];
const DISABLED_STATS_DEFAULT: &str = "fetch-channel-statuses";
const STATS_TASK: &str = "apps.proxy.tasks.fetch_channel_stats";
const SCAN_FILES: (&str, &str, i32) = ("scan-files", "core.tasks.scan_and_process_files", 20);
const MAINTAIN_RECURRING: (&str, &str, i32) = (
    "maintain-recurring-recordings",
    "apps.channels.tasks.maintain_recurring_recordings",
    3_600,
);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScheduleScope {
    All,
    Backup,
    SavedCleanup,
    Core,
    CoreWithPluginRefresh,
    CoreWithXmltv {
        plugin_refresh: bool,
        saved_stats: bool,
        saved_xc: bool,
    },
}
impl ScheduleScope {
    fn core_with_xmltv(
        plugin_refresh: bool,
        xmltv_worker_available: bool,
        saved_stats: bool,
        saved_xc: bool,
    ) -> Result<Self, Error> {
        if !xmltv_worker_available {
            return Err(Error::Configuration(
                "Core linked XMLTV receiver unavailable",
            ));
        }
        Ok(Self::CoreWithXmltv {
            plugin_refresh,
            saved_stats,
            saved_xc,
        })
    }

    fn uses_selected_receivers(self) -> bool {
        matches!(self, Self::CoreWithXmltv { .. })
    }

    fn strict_checkpoint(self) -> bool {
        self.uses_selected_receivers() || self == Self::SavedCleanup
    }

    fn includes_saved_xc(self) -> bool {
        matches!(self, Self::CoreWithXmltv { saved_xc: true, .. })
    }

    fn selected_default_interval(self, name: &str, task: &str) -> Option<i32> {
        // Only this scope is bound to the complete Selected dispatcher.
        if !self.uses_selected_receivers() {
            return None;
        }
        [SCAN_FILES, MAINTAIN_RECURRING]
            .into_iter()
            .find(|row| (row.0, row.1) == (name, task))
            .map(|(_, _, seconds)| seconds)
    }

    fn is_linked_xmltv_task(self, task: &str) -> bool {
        self.uses_selected_receivers() && task == crate::refresh_protocol::NAME
    }

    fn xmltv_source_id(self, entry: &Entry) -> Option<i64> {
        if !self.is_linked_xmltv_task(&entry.task) {
            return None;
        }
        linked_id(entry, "source_id", "epg_source-refresh-")
    }

    fn is_linked_clocked_dvr(self, task: &str) -> bool {
        self.uses_selected_receivers() && task == CLOCKED_DVR_TASK
    }

    fn is_linked_local_m3u_task(self, task: &str) -> bool {
        self.uses_selected_receivers() && task == crate::normal_xc_live::SINGLE_TASK_NAME
    }

    fn local_m3u_account_id(self, entry: &Entry) -> Option<i64> {
        if !self.is_linked_local_m3u_task(&entry.task) {
            return None;
        }
        linked_id(entry, "account_id", "m3u_account-refresh-")
    }

    fn includes_plugin_refresh(self) -> bool {
        matches!(
            self,
            Self::CoreWithPluginRefresh
                | Self::CoreWithXmltv {
                    plugin_refresh: true,
                    ..
                }
        )
    }

    fn is_core(self) -> bool {
        matches!(
            self,
            Self::Core | Self::CoreWithPluginRefresh | Self::CoreWithXmltv { .. }
        )
    }

    fn includes_saved_stats(self) -> bool {
        matches!(
            self,
            Self::CoreWithXmltv {
                saved_stats: true,
                ..
            }
        )
    }

    fn admits_saved_stats(self, entry: &Entry) -> bool {
        if !self.includes_saved_stats()
            || entry.task != STATS_TASK
            || entry.name == DISABLED_STATS_DEFAULT
            || !entry.enabled
            || entry.expires.is_some()
            || entry.expire_seconds.is_some()
            || entry.queue.is_some()
            || entry.exchange.is_some()
            || entry.routing_key.is_some()
            || entry.priority.is_some()
            || !entry.args.as_array().is_some_and(Vec::is_empty)
            || !entry
                .kwargs
                .as_object()
                .is_some_and(serde_json::Map::is_empty)
            || !entry
                .headers
                .as_object()
                .is_some_and(serde_json::Map::is_empty)
        {
            return false;
        }

        match (&entry.interval, &entry.crontab) {
            (Some((every, period)), None) => *every > 0 && period == "seconds" && entry.one_off,
            (None, Some(_)) => {
                !entry.one_off && entry.start_time.is_none() && daily_utc_crontab(entry).is_some()
            }
            _ => false,
        }
    }

    fn is_named_builtin(self, name: &str, task: &str) -> bool {
        self.uses_selected_receivers()
            && !RESERVED_PERIODIC_NAMES.contains(&name)
            && matches!(
                task,
                CLEANUP_PERIODIC_TASK | EXPIRATION_PERIODIC_TASK | crate::version_protocol::TASK
            )
    }

    fn admits_named_builtin(self, entry: &Entry) -> bool {
        if !self.is_named_builtin(&entry.name, &entry.task)
            || entry.id <= 0
            || entry.total_run_count < 0
            || entry.name.is_empty()
            || entry.name.chars().count() > 200
            || !entry.enabled
        {
            return false;
        }
        match entry.task.as_str() {
            CLEANUP_PERIODIC_TASK => {
                entry.interval.is_none()
                    && entry.expire_seconds == Some(43_200)
                    && daily_utc_crontab(entry).is_some()
            }
            EXPIRATION_PERIODIC_TASK | crate::version_protocol::TASK => {
                entry.crontab.is_none()
                    && entry.expire_seconds.is_none()
                    && entry.interval.as_ref().is_some_and(|(every, period)| {
                        *every == DAILY_SECONDS && period == "seconds"
                    })
            }
            _ => false,
        }
    }

    fn row_identity(self) -> (Option<&'static str>, Option<&'static str>) {
        match self {
            Self::All => (None, None),
            Self::Backup => (Some(BACKUP_PERIODIC_NAME), Some(BACKUP_PERIODIC_TASK)),
            Self::SavedCleanup => (None, None),
            Self::Core | Self::CoreWithPluginRefresh | Self::CoreWithXmltv { .. } => {
                (Some(CLEANUP_PERIODIC_TASK), Some(CLEANUP_PERIODIC_TASK))
            }
        }
    }
    fn admits(self, entry: &Entry) -> bool {
        if self == Self::All {
            return true;
        }
        if self == Self::SavedCleanup {
            return false;
        }
        if self.is_core() {
            if self.admits_saved_stats(entry) {
                return true;
            }
            let cleanup = entry.name == CLEANUP_PERIODIC_TASK
                && entry.task == CLEANUP_PERIODIC_TASK
                && entry.interval.is_none()
                && entry.crontab.is_some()
                && entry.expire_seconds == Some(43_200);
            let daily = matches!(
                (entry.name.as_str(), entry.task.as_str()),
                (EXPIRATION_PERIODIC_NAME, EXPIRATION_PERIODIC_TASK)
                    | (VERSION_PERIODIC_NAME, crate::version_protocol::TASK)
            ) && entry.crontab.is_none()
                && entry
                    .interval
                    .as_ref()
                    .is_some_and(|(every, period)| *every == DAILY_SECONDS && period == "seconds")
                && entry.expire_seconds.is_none();
            let plugin_refresh = self.includes_plugin_refresh()
                && entry.name == plugin_startup_schedule::NAME
                && entry.task == crate::normal_plugin_repos::TASK
                && entry.crontab.is_none()
                && entry
                    .interval
                    .as_ref()
                    .is_some_and(|(every, period)| *every > 0 && period == "hours")
                && entry.expire_seconds.is_none();
            let linked_refresh = (self.xmltv_source_id(entry).is_some()
                || self.local_m3u_account_id(entry).is_some())
                && entry.enabled
                && entry.crontab.is_none()
                && entry
                    .interval
                    .as_ref()
                    .is_some_and(|(every, period)| *every > 0 && period == "hours")
                && entry.expire_seconds.is_none();
            let linked_xmltv_cron = self.xmltv_source_id(entry).is_some()
                && entry.enabled
                && entry.interval.is_none()
                && entry.expire_seconds.is_none()
                && daily_utc_crontab(entry).is_some();
            let selected_default =
                self.selected_default_interval(&entry.name, &entry.task)
                    .is_some_and(|seconds| {
                        entry.enabled
                            && entry.crontab.is_none()
                            && entry.expire_seconds.is_none()
                            && entry.interval.as_ref().is_some_and(|(every, period)| {
                                *every == seconds && period == "seconds"
                            })
                    });
            let ordinary = (cleanup
                || daily
                || plugin_refresh
                || selected_default
                || self.admits_named_builtin(entry))
                && entry
                    .kwargs
                    .as_object()
                    .is_some_and(|kwargs| kwargs.is_empty());
            return (ordinary || linked_refresh || linked_xmltv_cron)
                && !entry.one_off
                && entry.start_time.is_none()
                && entry.expires.is_none()
                && entry.queue.is_none()
                && entry.exchange.is_none()
                && entry.routing_key.is_none()
                && entry.priority.is_none()
                && entry
                    .headers
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
fn daily_utc_crontab(entry: &Entry) -> Option<(&str, &str)> {
    fn clock_value(value: &str, upper_bound: u8) -> bool {
        value.len() <= 2
            && value.bytes().all(|digit| digit.is_ascii_digit())
            && value.parse::<u8>().is_ok_and(|value| value < upper_bound)
    }

    let (minute, hour, weekday, day, month, zone) = entry.crontab.as_ref()?;
    (zone == "UTC"
        && clock_value(minute, 60)
        && clock_value(hour, 24)
        && weekday == "*"
        && day == "*"
        && month == "*")
        .then_some((minute.as_str(), hour.as_str()))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Admission {
    Prepared,
    Started,
    Closed,
}
enum Operation {
    Initialize,
    TimerTick,
    ManualTick,
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
            || (*admission == Admission::Started && !matches!(operation, Operation::TimerTick))
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

struct BeatTickOwner {
    _caller: Arc<dyn Send + Sync>,
    _inner: Arc<Inner>,
    _physical_permit: PhysicalPermit,
}

/// Return the permit to the waiter only after every retained transport owner
/// has dropped. This keeps both the scheduler slot and its PG fence while a
/// logically timed-out Redis command may still be physically running.
struct PhysicalPermit {
    return_on_drop: Option<(OwnedSemaphorePermit, oneshot::Sender<OwnedSemaphorePermit>)>,
}
impl PhysicalPermit {
    fn new(permit: OwnedSemaphorePermit) -> (Self, oneshot::Receiver<OwnedSemaphorePermit>) {
        let (release, completed) = oneshot::channel();
        (
            Self {
                return_on_drop: Some((permit, release)),
            },
            completed,
        )
    }
}
impl Drop for PhysicalPermit {
    fn drop(&mut self) {
        if let Some((permit, release)) = self.return_on_drop.take() {
            let _ = release.send(permit);
        }
    }
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
        Self::prepare_scoped(pool, jobs, identity, maximum_sleep, None, None, None)
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
        Self::prepare_scoped(
            pool,
            worker.jobs(),
            identity,
            maximum_sleep,
            Some(ScheduleConsumer::Core(worker)),
            None,
            None,
        )
    }

    /// Use the one prepared Selected receiver for bounded Core schedules and
    /// individually linked saved XMLTV schedules. This starts neither owner.
    pub fn prepare_core_schedules_for_selected(
        worker: crate::normal_queue::SelectedCoreSchedules,
        maximum_sleep: Duration,
    ) -> Result<Self, Error> {
        let scope = ScheduleScope::core_with_xmltv(
            worker.owns_plugin_refresh(),
            worker.owns_xmltv_refresh(),
            worker.owns_saved_stats(),
            worker.owns_saved_xc(),
        )?;
        Self::prepare_scoped(
            worker.pool(),
            worker.jobs(),
            worker.identity(),
            maximum_sleep,
            Some(ScheduleConsumer::Selected(worker)),
            Some(scope),
            None,
        )
    }

    /// Prepare one explicitly selected saved cleanup row without a receiver.
    /// The producer's only tick path uses the same normal Beat session fence.
    pub fn prepare_saved_cleanup(
        pool: PgPool,
        jobs: Jobs,
        identity: ProducerIdentitySource,
        name: &str,
    ) -> Result<Self, Error> {
        let saved_cleanup = SavedCleanupSchedule::new(name)?;
        Self::prepare_scoped(
            pool,
            jobs,
            identity,
            Duration::from_secs(5),
            None,
            Some(ScheduleScope::SavedCleanup),
            Some(saved_cleanup),
        )
    }

    fn prepare_scoped(
        pool: PgPool,
        jobs: Jobs,
        identity: ProducerIdentitySource,
        maximum_sleep: Duration,
        backup_worker: Option<ScheduleConsumer>,
        scope: Option<ScheduleScope>,
        saved_cleanup: Option<SavedCleanupSchedule>,
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
                restore_pause: restore_pause::Control::new(),
                maximum_sleep,
                scope: scope.unwrap_or_else(|| match &backup_worker {
                    Some(worker) if worker.owns_plugin_refresh() => {
                        ScheduleScope::CoreWithPluginRefresh
                    }
                    Some(_) => ScheduleScope::Core,
                    None => ScheduleScope::All,
                }),
                saved_cleanup,
                backup_worker,
            }),
        })
    }
    /// Source DatabaseScheduler installs backend cleanup plus the pinned app
    /// defaults before timer start. Existing disabled rows stay disabled except
    /// the explicitly disabled legacy stats row, which is always disabled.
    pub async fn initialize(&self, caller: Arc<dyn Send + Sync>) -> Result<Setup, Error> {
        if self.inner.saved_cleanup.is_some() {
            return Err(Error::Configuration("saved cleanup has no default setup"));
        }
        let slot = self.inner.lifecycle.reserve(Operation::Initialize)?;
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let _owner = (caller, slot);
            let Some(mut session) = BeatSession::connect(&inner.pool).await?.try_claim().await?
            else {
                return if inner.scope.uses_selected_receivers() {
                    Ok(Setup::default())
                } else {
                    Err(Error::Configuration(
                        "another configured Beat owner is active",
                    ))
                };
            };
            let observed = if inner.scope.is_core() && !inner.scope.uses_selected_receivers() {
                install_default(
                    &mut session.connection,
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
                install_defaults(&mut session.connection).await
            };
            session.settle(observed).await
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
        if self.inner.saved_cleanup.is_some() {
            return Err(Error::Configuration(
                "saved cleanup requires a supervised task",
            ));
        }
        self.spawn_timer(caller).map(drop)
    }

    pub fn start_saved_cleanup(
        &self,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<tokio::task::JoinHandle<()>, Error> {
        if self.inner.saved_cleanup.is_none() {
            return Err(Error::Configuration(
                "saved cleanup task requires its own scope",
            ));
        }
        self.spawn_timer(caller)
    }

    fn spawn_timer(
        &self,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<tokio::task::JoinHandle<()>, Error> {
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
        Ok(tokio::spawn(async move {
            let status = match this.serve_owned(&mut stop, caller).await {
                Ok(status) => status,
                Err(error) => Status::Fault(Arc::new(error)),
            };
            this.inner.lifecycle.finish(status);
        }))
    }

    fn receiver_availability(&self) -> SelectedScheduleAvailability {
        match self.inner.backup_worker.as_ref() {
            Some(ScheduleConsumer::Selected(worker)) => worker.availability(),
            Some(ScheduleConsumer::Core(worker)) => worker.schedule_availability(),
            None => Ready,
        }
    }

    async fn serve_owned(
        &self,
        stop: &mut watch::Receiver<bool>,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Status, Error> {
        while !*stop.borrow() {
            match self.receiver_availability() {
                Terminal => return Ok(Status::ReceiverFault),
                Waiting => {
                    tokio::select! {
                        _ = tokio::time::sleep(self.inner.maximum_sleep) => {},
                        _ = stop.changed() => {},
                    }
                    continue;
                }
                Ready => {}
            }
            let session = tokio::select! {
                biased;
                _ = stop.changed() => return Ok(Status::Stopped),
                session = BeatSession::connect(&self.inner.pool) => session?,
            };
            // Only connection setup may be cancelled by TERM. Once the SQL
            // claim starts, settle its reply/socket within the role budget.
            if let Some(mut session) = session.try_claim().await? {
                if self.inner.scope.strict_checkpoint() {
                    self.inner.lifecycle.status.send_replace(Status::Acquired);
                }
                *self.inner.cache.lock().await = Cache::default();
                self.inner.restore_pause.elected(true);
                let observed = self.run_owned(&mut session.connection, stop, caller).await;
                // An uncertain parked restore must not issue checkpoint SQL.
                let observed = if self.inner.restore_pause.blocked()
                    && !self.inner.restore_pause.settled_on_stop()
                {
                    Err(Error::Drain)
                } else if self.inner.restore_pause.settled_on_stop() {
                    observed
                } else {
                    self.finish_owned(&mut session.connection, observed).await
                };
                let settled = session.settle(observed).await;
                self.inner.restore_pause.elected(false);
                return settled;
            }
            if !self.inner.scope.uses_selected_receivers() {
                // Legacy and standalone Core still admit a single configured
                // owner. Only Selected/Core has a validated standby topology.
                return Ok(Status::Contended);
            }
            self.inner.lifecycle.status.send_replace(Status::Standby);
            tokio::select! {
                _ = tokio::time::sleep(self.inner.maximum_sleep) => {},
                _ = stop.changed() => {},
            }
        }
        Ok(Status::Stopped)
    }

    async fn run_owned(
        &self,
        connection: &mut PgConnection,
        stop: &mut watch::Receiver<bool>,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Status, Error> {
        let mut restore_pause = self.inner.restore_pause.changes.subscribe();
        loop {
            if *stop.borrow() || self.inner.restore_pause.stopped() {
                return Ok(Status::Stopped);
            }
            if self.inner.restore_pause.blocked() {
                if !self.park_for_restore(connection, stop).await? {
                    return Ok(Status::Stopped);
                }
                continue;
            }
            match self.receiver_availability() {
                Terminal => return Ok(Status::ReceiverFault),
                Waiting => {
                    self.inner.lifecycle.status.send_replace(Status::Acquired);
                    tokio::select! {
                        _ = tokio::time::sleep(self.inner.maximum_sleep) => {},
                        _ = stop.changed() => {},
                        _ = restore_pause.changed() => {},
                    }
                    continue;
                }
                Ready => {}
            }
            let slot = match self.inner.lifecycle.reserve(Operation::TimerTick) {
                Ok(slot) => slot,
                Err(Error::Closed) => return Ok(Status::Stopped),
                Err(error) => return Err(error),
            };
            let (physical_permit, completed) = PhysicalPermit::new(slot);
            let owner = Arc::new(BeatTickOwner {
                _caller: caller.clone(),
                _inner: self.inner.clone(),
                _physical_permit: physical_permit,
            });
            let tick = tick_owned(&self.inner, connection, owner);
            let observed = if let Some(saved) = self.inner.saved_cleanup.as_ref() {
                observe_saved_cleanup_tick(tick)
                    .await
                    .unwrap_or_else(|error| {
                        Err(saved.pending().map_or(error, Error::SavedCleanupUncertain))
                    })
            } else {
                tick.await
            };
            if observed.is_err() {
                self.inner
                    .lifecycle
                    .status
                    .send_replace(Status::SettlingFault);
            }
            let slot = completed.await.map_err(|_| Error::Supervisor)?;
            let observed = observed?;
            drop(slot);
            let delay = observed.delay_seconds;
            self.inner
                .lifecycle
                .status
                .send_replace(Status::Observed(observed));
            if delay > 0.0 {
                let duration = Duration::from_secs_f64(delay).min(self.inner.maximum_sleep);
                tokio::select! {
                    _ = tokio::time::sleep(duration) => {},
                    _ = stop.changed() => {},
                        _ = restore_pause.changed() => {},
                }
            }
        }
    }

    async fn finish_owned<T>(
        &self,
        connection: &mut PgConnection,
        observed: Result<T, Error>,
    ) -> Result<T, Error> {
        let strict = self.inner.scope.strict_checkpoint();
        // Selected/Core does not advance a possibly failed occurrence on
        // shutdown. Any attempted final sync must account for remaining dirty
        // state before this process can report a clean fence release.
        if !strict || observed.is_ok() {
            let mut cache = self.inner.cache.lock().await;
            sync(connection, &mut cache, true).await?;
            if !cache.dirty.is_empty() {
                return Err(Error::Drain);
            }
        }
        observed
    }

    pub async fn tick_once(&self, caller: Arc<dyn Send + Sync>) -> Result<Tick, Error> {
        if self.inner.saved_cleanup.is_some() {
            return Err(Error::Configuration(
                "saved cleanup ticks require the scheduler owner",
            ));
        }
        if self.inner.scope.uses_selected_receivers() {
            return Err(Error::Configuration(
                "Selected Core ticks require the scheduler owner",
            ));
        }
        let slot = self.inner.lifecycle.reserve(Operation::ManualTick)?;
        if self.receiver_availability() != Ready {
            return Ok(Tick {
                delay_seconds: self.inner.maximum_sleep.as_secs_f64(),
                ..Tick::default()
            });
        }
        let this = self.clone();
        // Physical work outlives its observer and cannot publish without the
        // same fence as the timer. Reload after each one-shot ownership claim.
        tokio::spawn(async move {
            let Some(mut session) = BeatSession::connect(&this.inner.pool)
                .await?
                .try_claim()
                .await?
            else {
                return Err(Error::Configuration(
                    "another configured Beat owner is active",
                ));
            };
            *this.inner.cache.lock().await = Cache::default();
            let (physical_permit, completed) = PhysicalPermit::new(slot);
            let owner = Arc::new(BeatTickOwner {
                _caller: caller,
                _inner: this.inner.clone(),
                _physical_permit: physical_permit,
            });
            let observed = tick_owned(&this.inner, &mut session.connection, owner).await;
            let _slot = completed.await.map_err(|_| Error::Supervisor)?;
            let observed = this.finish_owned(&mut session.connection, observed).await;
            session.settle(observed).await
        })
        .await
        .map_err(|_| Error::Supervisor)?
    }

    pub fn subscribe_status(&self) -> watch::Receiver<Status> {
        self.inner.lifecycle.status.subscribe()
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
        self.inner.restore_pause.stop();
        self.inner.lifecycle.close();
    }
    /// Nonblocking, allocation-free observation; never changes drain acceptance.
    /// The caller must select the existing bounded shutdown diagnostic control.
    pub fn shutdown_diagnostic(&self) -> ShutdownDiagnostic {
        let (cache, checkpoint) = match self.inner.cache.try_lock() {
            Ok(cache) => shutdown_dirty_labels(
                !cache.dirty.is_empty(),
                cache
                    .dirty
                    .keys()
                    .any(|name| !cache.entries.contains_key(name)),
            ),
            Err(_) => ("Busy", "Unknown"),
        };
        let (state, fault) = match self.status() {
            Status::Prepared => ("Prepared", "None"),
            Status::Running => ("Running", "None"),
            Status::Standby => ("Standby", "None"),
            Status::Contended => ("Contended", "None"),
            Status::Acquired => ("Acquired", "None"),
            Status::Observed(_) => ("Observed", "None"),
            Status::ReceiverFault => ("ReceiverFault", "None"),
            Status::SettlingFault => ("SettlingFault", "None"),
            Status::Fault(error) => ("Fault", shutdown_fault_label(error.as_ref())),
            Status::Stopped => ("Stopped", "None"),
        };
        ShutdownDiagnostic {
            timer: if *self.inner.lifecycle.done.borrow() {
                "Done"
            } else {
                "Pending"
            },
            slot: if self.inner.lifecycle.slots.is_closed() {
                "Closed"
            } else if self.inner.lifecycle.slots.available_permits() > 0 {
                "Available"
            } else {
                "Busy"
            },
            cache,
            checkpoint,
            state,
            fault,
        }
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
        let _slot =
            tokio::time::timeout_at(deadline, self.inner.lifecycle.slots.clone().acquire_owned())
                .await
                .map_err(|_| Error::Drain)?
                .map_err(|_| Error::Drain)?;
        if self.inner.restore_pause.blocked() && !self.inner.restore_pause.settled_on_stop() {
            return Err(Error::Drain);
        }
        shutdown_result(
            &self.status(),
            !self.inner.cache.lock().await.dirty.is_empty(),
        )
    }
}

fn shutdown_result(status: &Status, dirty: bool) -> Result<(), Error> {
    if dirty || matches!(status, Status::SettlingFault | Status::Fault(_)) {
        Err(Error::Drain)
    } else {
        Ok(())
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
/// An owned, unpooled session. Dropping it on any error or cancellation
/// cannot return a still-locked connection to the Main pool.
struct BeatSession {
    connection: PgConnection,
    timeout: Duration,
}

impl BeatSession {
    async fn connect(pool: &PgPool) -> Result<Self, Error> {
        let timeout = pool.options().get_acquire_timeout();
        let options = pool.connect_options();
        let connection =
            tokio::time::timeout(timeout, PgConnection::connect_with(options.as_ref()))
                .await
                .map_err(|_| Error::RoleDatabaseTimeout)??;
        Ok(Self {
            connection,
            timeout,
        })
    }

    async fn try_claim(mut self) -> Result<Option<Self>, Error> {
        let claim = sqlx::query_scalar(
            "SELECT pg_try_advisory_lock(hashtext('dispatcharr:selected-core-beat'), 0)",
        )
        .fetch_one(&mut self.connection);
        let observed = tokio::time::timeout(self.timeout, claim)
            .await
            .map_err(|_| Error::RoleDatabaseTimeout)
            .and_then(|observed| observed.map_err(Error::from));
        match observed {
            Ok(true) => Ok(Some(self)),
            outcome => self.settle(outcome.map(|_| None)).await,
        }
    }

    async fn settle<T>(mut self, observed: Result<T, Error>) -> Result<T, Error> {
        let deadline = tokio::time::Instant::now() + self.timeout;
        let timed_out = || sqlx::Error::Io(std::io::ErrorKind::TimedOut.into());
        let ready = tokio::time::timeout_at(deadline, self.connection.ping())
            .await
            .unwrap_or_else(|_| Err(timed_out()))
            .err()
            .map(Box::new);
        let close = tokio::time::timeout_at(deadline, self.connection.close())
            .await
            .unwrap_or_else(|_| Err(timed_out()))
            .err()
            .map(Box::new);
        settlement(observed, ready, close)
    }
}

async fn tick_owned(
    inner: &Inner,
    connection: &mut PgConnection,
    owner: Arc<BeatTickOwner>,
) -> Result<Tick, Error> {
    if inner.scope.is_core()
        && crate::normal_backup_schedule::try_lock_due_occurrence(connection).await?
    {
        let backup = tick_backup(inner, connection, owner.clone()).await?;
        if !crate::normal_backup_schedule::release_due_occurrence(connection).await? {
            return Err(Error::Configuration("backup schedule lock settlement"));
        }
        if backup.synchronized != 0 {
            return Ok(backup);
        }
    }
    tick(inner, connection, owner).await
}

fn settlement<T>(
    observed: Result<T, Error>,
    ready: Option<Box<sqlx::Error>>,
    close: Option<Box<sqlx::Error>>,
) -> Result<T, Error> {
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
    owner: Arc<BeatTickOwner>,
) -> Result<Tick, Error> {
    let mut result = Tick {
        delay_seconds: 5.0,
        ..Tick::default()
    };
    // The detached session keeps its advisory lock from the due check through
    // broker settlement and the durable SQL tick.
    let now = Utc::now();
    let (mut entries, invalid, _) = load(connection, now, ScheduleScope::Backup, None).await?;
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
    // Keep the verified row reserved during publication. An abrupt exit can
    // leave the previous durable tick for restart in both observed windows.
    let mut transaction = connection.begin().await?;
    let stored = sqlx::query(concat!(
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
    .execute(&mut *transaction)
    .await?;
    if stored.rows_affected() != 1 {
        transaction.rollback().await?;
        return Ok(result);
    }
    let published = match prepare_publication(&entry, Uuid::new_v4(), &inner.identity) {
        Ok(publication) => inner.jobs.publish_normal_beat(&publication, owner).await,
        Err(_) => Err(transport::Error::Protocol),
    };
    let settled = match published {
        Err(error @ transport::Error::AmbiguousPublication(_)) => {
            transaction.rollback().await?;
            return Err(Error::Transport(error));
        }
        outcome => outcome,
    };
    transaction.commit().await?;
    result.synchronized = 1;
    match settled {
        Ok(publication) => result.published = Some(publication.task_id),
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
enum CheckpointResult {
    Saved,
    Missing,
    Retired,
}

async fn save_checkpoint(
    connection: &mut PgConnection,
    entry: &Entry,
    checkpoint: Checkpoint,
) -> Result<CheckpointResult, sqlx::Error> {
    let rows = sqlx::query("UPDATE django_celery_beat_periodictask SET last_run_at=CASE WHEN enabled THEN $2 ELSE NULL END,total_run_count=$3,date_changed=$4 WHERE id=$1")
        .bind(entry.id).bind(entry.last_run_at).bind(entry.total_run_count).bind(Utc::now()).execute(&mut *connection).await?;
    if rows.rows_affected() > 0 {
        return Ok(CheckpointResult::Saved);
    }
    // The DVR receiver deletes its one-off row before capturing media. An
    // acknowledged occurrence whose row is gone has no run count left to save.
    if checkpoint.can_confirm_retirement(entry.id) {
        let present: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM django_celery_beat_periodictask WHERE id=$1 OR name=$2)",
        )
        .bind(entry.id)
        .bind(&entry.name)
        .fetch_one(&mut *connection)
        .await?;
        if !present {
            return Ok(CheckpointResult::Retired);
        }
    }
    Ok(CheckpointResult::Missing)
}

async fn sync(
    connection: &mut PgConnection,
    cache: &mut Cache,
    required: bool,
) -> Result<(usize, usize), Error> {
    let mut saved = 0;
    let mut missing = HashMap::new();
    while let Some((name, checkpoint)) = cache
        .dirty
        .iter()
        .next()
        .map(|(name, checkpoint)| (name.clone(), *checkpoint))
    {
        cache.dirty.remove(&name);
        let Some(entry) = cache.entries.get(&name) else {
            missing.insert(name, checkpoint);
            continue;
        };
        if matches!(checkpoint, Checkpoint::PublishedClockedDvr(id) if id != entry.entry.id) {
            missing.insert(name, checkpoint);
            continue;
        }
        match save_checkpoint(connection, &entry.entry, checkpoint).await {
            Ok(CheckpointResult::Saved) => saved += 1,
            Ok(CheckpointResult::Retired) => {}
            Ok(CheckpointResult::Missing) => {
                missing.insert(name, checkpoint);
            }
            Err(sqlx::Error::Database(error)) if !required => {
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
                if required {
                    cache.dirty.insert(name, checkpoint);
                }
                cache.dirty.extend(missing);
                return Err(error.into());
            }
        }
    }
    let count = missing.len();
    cache.dirty.extend(missing);
    Ok((saved, count))
}
// Close the read/claim window against normal source, task, and interval edits.
// Source is locked before the task join, matching the app's EPG write order.
// PostgreSQL locks cover local reservation only and retire before Redis work.
const LOCK_CURRENT_XMLTV: &str = r#"
WITH source AS MATERIALIZED (
    SELECT refresh_task_id FROM epg_epgsource
    WHERE id=$1::bigint AND is_active AND source_type='xmltv'
    FOR SHARE NOWAIT
)
SELECT p.args,p.kwargs,p.headers
FROM source s
JOIN django_celery_beat_periodictask p ON p.id=s.refresh_task_id
JOIN django_celery_beat_intervalschedule i ON i.id=p.interval_id
WHERE p.id=$2::bigint AND p.name=$3::text AND p.task=$4::text
  AND p.enabled AND NOT p.one_off AND p.solar_id IS NULL
  AND p.clocked_id IS NULL AND p.crontab_id IS NULL
  AND p.start_time IS NULL AND p.expires IS NULL AND p.expire_seconds IS NULL
  AND p.queue IS NULL AND p.exchange IS NULL AND p.routing_key IS NULL AND p.priority IS NULL
  AND i.every=$5::integer AND i.every>0 AND i.period='hours'
  AND p.total_run_count=$6::bigint
  AND COALESCE(p.last_run_at,p.date_changed)=$7::timestamptz
  AND octet_length(p.args)<=65536 AND octet_length(p.kwargs)<=65536
  AND octet_length(p.headers)<=65536
FOR SHARE OF p,i NOWAIT"#;

// Lock the source first, as for the interval path. Require the identical
// current numeric UTC fields before reserving the cached occurrence.
const LOCK_CURRENT_XMLTV_CRON: &str = r#"
WITH source AS MATERIALIZED (
    SELECT refresh_task_id FROM epg_epgsource
    WHERE id=$1::bigint AND is_active AND source_type='xmltv' AND refresh_interval=0
    FOR SHARE NOWAIT
)
SELECT p.args,p.kwargs,p.headers
FROM source s
JOIN django_celery_beat_periodictask p ON p.id=s.refresh_task_id
JOIN django_celery_beat_crontabschedule c ON c.id=p.crontab_id
WHERE p.id=$2::bigint AND p.name=$3::text AND p.task=$4::text
  AND p.enabled AND NOT p.one_off AND p.solar_id IS NULL
  AND p.clocked_id IS NULL AND p.interval_id IS NULL
  AND p.start_time IS NULL AND p.expires IS NULL AND p.expire_seconds IS NULL
  AND p.queue IS NULL AND p.exchange IS NULL AND p.routing_key IS NULL AND p.priority IS NULL
  AND c.minute=$5::text AND c.hour=$6::text AND c.timezone='UTC'
  AND c.day_of_week='*' AND c.day_of_month='*' AND c.month_of_year='*'
  AND p.total_run_count=$7::bigint
  AND COALESCE(p.last_run_at,p.date_changed)=$8::timestamptz
  AND octet_length(p.args)<=65536 AND octet_length(p.kwargs)<=65536
  AND octet_length(p.headers)<=65536
FOR SHARE OF p,c NOWAIT"#;

// Both SQL paths retain the same bounded account and default interval lane.
// A canonical saved URL override uses the local receiver's existing URL mode
// independently of the global settings. The worker honors explicit false
// in the same local preference shapes when creating group relationships.
// Other saved CSV forms remain outside this scheduler scope.
macro_rules! local_m3u_account {
    () => {
        r#"a.account_type='STD' AND a.is_active
           AND a.server_url IS NULL AND a.file_path <> ''
           AND a.user_agent_id IS NULL AND a.stream_profile_id IS NULL
           AND a.username IS NULL AND a.password IS NULL
           AND (CASE WHEN a.custom_properties->'hash_key'='"url"'::jsonb
                     THEN a.custom_properties - 'hash_key'
                     ELSE a.custom_properties END) IN (
             '{}'::jsonb,
             '{"auto_enable_new_groups_live":false}'::jsonb,
             '{"enable_vod":false,"auto_enable_new_groups_live":true,
               "auto_enable_new_groups_vod":true,"auto_enable_new_groups_series":true}'::jsonb,
             '{"enable_vod":false,"auto_enable_new_groups_live":false,
               "auto_enable_new_groups_vod":true,"auto_enable_new_groups_series":true}'::jsonb
           )
           AND octet_length(a.name)<=512 AND octet_length(a.file_path)<=4096
           AND COALESCE(octet_length(a.last_message),0)<=8192"#
    };
}
macro_rules! local_m3u_interval {
    () => {
        r#"NOT p.one_off AND p.start_time IS NULL
           AND p.expires IS NULL AND p.expire_seconds IS NULL
           AND p.queue IS NULL AND p.exchange IS NULL AND p.routing_key IS NULL AND p.priority IS NULL
           AND i.period='hours' AND i.every=a.refresh_interval AND i.every>0
           AND octet_length(p.args) BETWEEN 1 AND 64
           AND octet_length(p.kwargs) BETWEEN 1 AND 128
           AND octet_length(p.headers) BETWEEN 1 AND 64"#
    };
}

// The first saved XC scope is the same unparented live + VOD account shape
// already qualified by the ordinary receiver. Reuse its runtime preclaim too.
macro_rules! saved_xc_account {
    () => {
        r#"a.account_type='XC' AND a.is_active AND NOT a.locked
           AND COALESCE(octet_length(a.file_path),0)=0
           AND octet_length(a.server_url) BETWEEN 1 AND 4096
           AND octet_length(a.username) BETWEEN 1 AND 255
           AND octet_length(a.password) BETWEEN 1 AND 255
           AND a.user_agent_id IS NULL AND a.stream_profile_id IS NULL
           AND a.stale_stream_days=0
           AND a.custom_properties='{"enable_vod":true,"hash_key":"url","auto_enable_new_groups_live":true}'::jsonb
           AND octet_length(a.name)<=512 AND COALESCE(octet_length(a.last_message),0)<=8192"#
    };
}

// Match the normal Store write order and retire the locks before Redis work.
macro_rules! lock_current_m3u {
    ($account:expr) => {
        concat!(
            r#"WITH account AS MATERIALIZED (
    SELECT a.refresh_task_id,a.refresh_interval FROM m3u_m3uaccount a
    WHERE a.id=$1::bigint AND "#,
            $account,
            r#"
    FOR SHARE NOWAIT
)
SELECT p.args,p.kwargs,p.headers
FROM account a
JOIN django_celery_beat_periodictask p ON p.id=a.refresh_task_id
JOIN django_celery_beat_intervalschedule i ON i.id=p.interval_id
WHERE p.id=$2::bigint AND p.name=$3::text AND p.task=$4::text
  AND p.enabled AND p.solar_id IS NULL AND p.clocked_id IS NULL AND p.crontab_id IS NULL
  AND i.every=$5::integer
  AND p.total_run_count=$6::bigint
  AND COALESCE(p.last_run_at,p.date_changed)=$7::timestamptz
  AND "#,
            local_m3u_interval!(),
            " FOR SHARE OF p,i NOWAIT"
        )
    };
}

const LOCK_CURRENT_LOCAL_M3U: &str = lock_current_m3u!(local_m3u_account!());
const LOCK_CURRENT_SAVED_XC: &str = lock_current_m3u!(saved_xc_account!());

fn linked_arguments_current(args: &str, kwargs: &str, headers: &str, entry: &Entry) -> bool {
    fn matches(raw: &str, empty: &str, expected: &Value) -> bool {
        serde_json::from_str::<Value>(if raw.is_empty() { empty } else { raw })
            .is_ok_and(|actual| actual == *expected)
    }
    matches(args, "[]", &entry.args)
        && matches(kwargs, "{}", &entry.kwargs)
        && matches(headers, "{}", &entry.headers)
}

fn linked_id(entry: &Entry, key: &str, name_prefix: &str) -> Option<i64> {
    let kwargs = entry
        .kwargs
        .as_object()
        .filter(|kwargs| kwargs.len() == 1)?;
    let id = kwargs.get(key)?.as_i64().filter(|id| *id > 0)?;
    (entry.name == format!("{name_prefix}{id}")).then_some(id)
}

enum LinkedReservation {
    Reserved,
    Stale,
    Busy,
}

async fn reserve_linked_interval(
    connection: &mut PgConnection,
    query: &str,
    linked_id: i64,
    snapshot: &mut Entry,
) -> Result<LinkedReservation, Error> {
    let (every, _) = snapshot.interval.as_ref().ok_or(Error::Supervisor)?;
    let mut transaction = connection.begin().await?;
    let current = sqlx::query_as::<_, (String, String, String)>(query)
        .bind(linked_id)
        .bind(snapshot.id)
        .bind(&snapshot.name)
        .bind(&snapshot.task)
        .bind(every)
        .bind(snapshot.total_run_count)
        .bind(snapshot.last_run_at)
        .fetch_optional(&mut *transaction)
        .await;
    settle_linked_reservation(transaction, current, snapshot).await
}

async fn reserve_linked_xmltv_cron(
    connection: &mut PgConnection,
    source_id: i64,
    snapshot: &mut Entry,
) -> Result<LinkedReservation, Error> {
    let (minute, hour) = daily_utc_crontab(snapshot).ok_or(Error::Supervisor)?;
    let mut transaction = connection.begin().await?;
    let current = sqlx::query_as::<_, (String, String, String)>(LOCK_CURRENT_XMLTV_CRON)
        .bind(source_id)
        .bind(snapshot.id)
        .bind(&snapshot.name)
        .bind(&snapshot.task)
        .bind(minute)
        .bind(hour)
        .bind(snapshot.total_run_count)
        .bind(snapshot.last_run_at)
        .fetch_optional(&mut *transaction)
        .await;
    settle_linked_reservation(transaction, current, snapshot).await
}

async fn settle_linked_reservation(
    transaction: sqlx::Transaction<'_, sqlx::Postgres>,
    current: Result<Option<(String, String, String)>, sqlx::Error>,
    snapshot: &mut Entry,
) -> Result<LinkedReservation, Error> {
    let current = match current {
        Ok(current) => current,
        Err(error) => {
            transaction.rollback().await?;
            if error
                .as_database_error()
                .and_then(|error| error.code())
                .as_deref()
                == Some("55P03")
            {
                return Ok(LinkedReservation::Busy);
            }
            return Err(error.into());
        }
    };
    let outcome = if current.is_some_and(|(args, kwargs, headers)| {
        linked_arguments_current(&args, &kwargs, &headers, snapshot)
    }) {
        snapshot.total_run_count = snapshot
            .total_run_count
            .checked_add(1)
            .ok_or(Error::Capacity)?;
        snapshot.last_run_at = Utc::now();
        LinkedReservation::Reserved
    } else {
        LinkedReservation::Stale
    };
    transaction.commit().await?;
    Ok(outcome)
}

struct ClockedDvr {
    recording: i64,
    channel: i64,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
}

fn clocked_dvr(row: &Entry, clocked: Option<DateTime<Utc>>) -> Option<ClockedDvr> {
    if row.task != CLOCKED_DVR_TASK
        || !row.enabled
        || !row.one_off
        || row.interval.is_some()
        || row.crontab.is_some()
        || row.start_time.is_some()
        || row.expires.is_some()
        || row.expire_seconds.is_some()
        || row.priority.is_some()
        || row.queue.is_some()
        || row.exchange.is_some()
        || row.routing_key.is_some()
        || row.total_run_count < 0
        || !row
            .kwargs
            .as_object()
            .is_some_and(serde_json::Map::is_empty)
        || !row
            .headers
            .as_object()
            .is_some_and(serde_json::Map::is_empty)
    {
        return None;
    }
    let [recording, channel, start, end] = row.args.as_array()?.as_slice() else {
        return None;
    };
    let recording = recording.as_i64()?;
    let channel = channel.as_i64()?;
    if !(1..=i64::from(i32::MAX)).contains(&recording)
        || !(1..=i64::from(i32::MAX)).contains(&channel)
        || row.name != format!("dvr-recording-{recording}")
    {
        return None;
    }
    let date = crate::clocked_recording_envelope::normal_source_date;
    let start = date(start.as_str()?).ok()?;
    let end = date(end.as_str()?).ok()?;
    (clocked == Some(start) && end > start).then_some(ClockedDvr {
        recording,
        channel,
        start,
        end,
    })
}

// The saved row can be rescheduled or cancelled between the heap reload and its
// due tick. Reserve the still-linked model under the recording and schedule
// locks before offering the canonical envelope to the existing DVR receiver.
const LOCK_CURRENT_CLOCKED_DVR: &str = r#"
WITH recording AS MATERIALIZED (
    SELECT task_id FROM dispatcharr_channels_recording
    WHERE id=$1::bigint AND channel_id=$2::bigint
      AND start_time=$3::timestamptz AND end_time=$4::timestamptz
    FOR SHARE NOWAIT
)
SELECT p.args,p.kwargs,p.headers
FROM recording r
JOIN django_celery_beat_periodictask p ON p.name=r.task_id
JOIN django_celery_beat_clockedschedule c ON c.id=p.clocked_id
WHERE p.id=$5::bigint AND p.name=$6::text AND p.task=$7::text
  AND p.enabled AND p.one_off AND p.solar_id IS NULL
  AND p.interval_id IS NULL AND p.crontab_id IS NULL
  AND p.start_time IS NULL AND p.expires IS NULL AND p.expire_seconds IS NULL
  AND p.queue IS NULL AND p.exchange IS NULL AND p.routing_key IS NULL AND p.priority IS NULL
  AND p.total_run_count=$8::bigint
  AND COALESCE(p.last_run_at,p.date_changed)=$9::timestamptz
  AND c.clocked_time=$3::timestamptz
  AND octet_length(p.args)<=8192 AND octet_length(p.kwargs)<=8192 AND octet_length(p.headers)<=8192
FOR SHARE OF p,c NOWAIT"#;

async fn reserve_current_clocked_dvr(
    connection: &mut PgConnection,
    binding: ClockedDvr,
    snapshot: &mut Entry,
) -> Result<LinkedReservation, Error> {
    let mut transaction = connection.begin().await?;
    let current = sqlx::query_as::<_, (String, String, String)>(LOCK_CURRENT_CLOCKED_DVR)
        .bind(binding.recording)
        .bind(binding.channel)
        .bind(binding.start)
        .bind(binding.end)
        .bind(snapshot.id)
        .bind(&snapshot.name)
        .bind(CLOCKED_DVR_TASK)
        .bind(snapshot.total_run_count)
        .bind(snapshot.last_run_at)
        .fetch_optional(&mut *transaction)
        .await;
    let current = match current {
        Ok(current) => current,
        Err(error) => {
            transaction.rollback().await?;
            if error
                .as_database_error()
                .and_then(|error| error.code())
                .as_deref()
                == Some("55P03")
            {
                return Ok(LinkedReservation::Busy);
            }
            return Err(error.into());
        }
    };
    let outcome = if current.is_some_and(|(args, kwargs, headers)| {
        linked_arguments_current(&args, &kwargs, &headers, snapshot)
    }) {
        snapshot.total_run_count = snapshot
            .total_run_count
            .checked_add(1)
            .ok_or(Error::Capacity)?;
        snapshot.last_run_at = Utc::now();
        LinkedReservation::Reserved
    } else {
        LinkedReservation::Stale
    };
    transaction.commit().await?;
    Ok(outcome)
}

async fn tick(
    inner: &Inner,
    connection: &mut PgConnection,
    owner: Arc<BeatTickOwner>,
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
    let reload = inner.saved_cleanup.is_some()
        || !cache.loaded
        || changed
        || cache
            .last_full
            .is_none_or(|last| last.elapsed() >= Duration::from_secs(300));
    cache.marker = marker;
    if reload {
        let (saved, missing) =
            sync(connection, &mut cache, inner.scope.strict_checkpoint()).await?;
        result.synchronized += saved;
        result.missing += missing;
        let (entries, invalid, saved_cleanup) =
            load(connection, now, inner.scope, inner.saved_cleanup.as_ref()).await?;
        cache.entries = entries;
        cache.saved_cleanup = saved_cleanup;
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
    let cache = &mut *cache;
    let entry = cache.entries.get_mut(&name).ok_or(Error::Supervisor)?;
    if inner.saved_cleanup.is_some() {
        result.saved_cleanup = Some(SavedCleanupObservation {
            id: entry.entry.id,
            total_run_count: entry.entry.total_run_count,
        });
    }
    let sampled = Utc::now();
    let due = decision(entry, sampled)?;
    if due.persistence != PersistenceEffect::Unchanged {
        if inner.saved_cleanup.is_some() {
            return Err(Error::Supervisor);
        }
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
    if let Some(saved) = cache.saved_cleanup.as_ref() {
        if let Some(published) =
            publish_saved_cleanup(inner, connection, saved, &entry.entry, owner).await?
        {
            result.saved_cleanup = Some(SavedCleanupObservation {
                id: entry.entry.id,
                total_run_count: entry
                    .entry
                    .total_run_count
                    .checked_add(1)
                    .ok_or(Error::Capacity)?,
            });
            result.published = Some(published);
            result.synchronized = 1;
        } else {
            result.saved_cleanup = None;
        }
        return Ok(result);
    }
    if inner.saved_cleanup.is_some() {
        return Err(Error::Supervisor);
    }
    // Reserve BEFORE serialization/publication, retaining the advanced cached
    // count even on a known rejection. An ambiguous actual reply is never retried.
    let mut snapshot = entry.entry.clone();
    let linked_clocked_dvr = inner.scope.is_linked_clocked_dvr(&snapshot.task);
    let reservation = if inner.scope.is_linked_xmltv_task(&entry.entry.task) {
        let source_id = inner
            .scope
            .xmltv_source_id(&entry.entry)
            .ok_or(Error::Supervisor)?;
        if snapshot.crontab.is_some() {
            reserve_linked_xmltv_cron(connection, source_id, &mut snapshot).await?
        } else {
            reserve_linked_interval(connection, LOCK_CURRENT_XMLTV, source_id, &mut snapshot)
                .await?
        }
    } else if inner.scope.is_linked_local_m3u_task(&snapshot.task) {
        let account_id = inner
            .scope
            .local_m3u_account_id(&snapshot)
            .ok_or(Error::Supervisor)?;
        if entry.saved_xc {
            let Some(ScheduleConsumer::Selected(receiver)) = inner.backup_worker.as_ref() else {
                return Err(Error::Supervisor);
            };
            match receiver.admits_saved_xc(account_id, owner.clone()).await {
                Ok(true) => {
                    reserve_linked_interval(
                        connection,
                        LOCK_CURRENT_SAVED_XC,
                        account_id,
                        &mut snapshot,
                    )
                    .await?
                }
                // A provider outside the composed receiver domain may become
                // valid later. Allow other saved rows to run in the meantime.
                Ok(false) | Err(_) => LinkedReservation::Busy,
            }
        } else {
            reserve_linked_interval(
                connection,
                LOCK_CURRENT_LOCAL_M3U,
                account_id,
                &mut snapshot,
            )
            .await?
        }
    } else if linked_clocked_dvr {
        let binding = clocked_dvr(&snapshot, entry.clocked).ok_or(Error::Supervisor)?;
        reserve_current_clocked_dvr(connection, binding, &mut snapshot).await?
    } else {
        snapshot.last_run_at = Utc::now();
        snapshot.total_run_count = snapshot
            .total_run_count
            .checked_add(1)
            .ok_or(Error::Capacity)?;
        LinkedReservation::Reserved
    };
    match reservation {
        LinkedReservation::Reserved => entry.entry = snapshot.clone(),
        LinkedReservation::Stale => {
            cache.loaded = false;
            return Ok(result);
        }
        LinkedReservation::Busy => {
            // Defer only the locked saved row so other work can proceed.
            entry.when = when(inner.maximum_sleep.as_secs_f64())?;
            result.delay_seconds = 0.0;
            return Ok(result);
        }
    }
    cache.dirty.insert(name.clone(), Checkpoint::Required);
    let id = Uuid::new_v4();
    let publication = prepare_publication(&snapshot, id, &inner.identity);
    let published = match publication {
        Ok(publication) => inner.jobs.publish_normal_beat(&publication, owner).await,
        Err(_) => Err(transport::Error::Protocol),
    };
    cache.dirty.insert(
        name.clone(),
        Checkpoint::after_publication(linked_clocked_dvr, published.is_ok(), snapshot.id),
    );
    if cache
        .last_sync
        .is_none_or(|last| last.elapsed() > Duration::from_secs(180))
    {
        let (saved, missing) = sync(connection, cache, inner.scope.strict_checkpoint()).await?;
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
macro_rules! load_query {
    ($selected:expr, $dvr_join:literal, $dvr_columns:literal, $selected_clocked:literal $(,)?) => {
        concat!(r#"WITH zone_offsets AS (SELECT * FROM unnest($4::text[],$5::integer[]) AS z(zone_name,hour_offset)),
candidates AS MATERIALIZED (
 SELECT to_jsonb(p)::text AS raw,
        (SELECT to_jsonb(i)::text FROM django_celery_beat_intervalschedule i WHERE i.id=p.interval_id) AS interval_spec,
        to_jsonb(cr)::text AS crontab_spec,
        cl.clocked_time, "#, $dvr_columns, r#"
 FROM django_celery_beat_periodictask p
 LEFT JOIN django_celery_beat_clockedschedule cl ON cl.id=p.clocked_id
 LEFT JOIN django_celery_beat_crontabschedule cr ON cr.id=p.crontab_id
 LEFT JOIN zone_offsets z ON z.zone_name=cr.timezone
"#, $dvr_join, r#"
 WHERE p.enabled
   AND ($6::text IS NULL OR (p.solar_id IS NULL AND ((p.clocked_id IS NULL AND (
        (p.name=$6 AND p.task=$7::text AND p.interval_id IS NULL AND p.crontab_id IS NOT NULL)
        OR (p.interval_id IS NOT NULL AND p.crontab_id IS NULL
            AND EXISTS (SELECT 1 FROM django_celery_beat_intervalschedule i
                        WHERE i.id=p.interval_id AND i.every=$12::integer AND i.period='seconds')
            AND ((p.name=$8::text AND p.task=$9::text) OR (p.name=$10::text AND p.task=$11::text)))
        OR (p.name=$13::text AND p.task=$14::text AND p.crontab_id IS NULL
            AND EXISTS (SELECT 1 FROM django_celery_beat_intervalschedule i
                        WHERE i.id=p.interval_id AND i.every>0 AND i.period='hours'))
"#, $selected, r#"   ))
"#, $selected_clocked, r#"   )))
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
 CASE WHEN b.rows<=256 AND b.bytes<=1048576 THEN c.clocked_time END AS clocked_time,
 c.dvr_channel,c.dvr_end,c.saved_xc
FROM budget b LEFT JOIN candidates c ON b.rows<=256 AND b.bytes<=1048576"#)
    };
}
const LOAD_QUERY: &str = load_query!(
    "",
    "",
    "NULL::bigint AS dvr_channel,NULL::timestamptz AS dvr_end,false AS saved_xc",
    ""
);
const LOAD_SELECTED_CORE_QUERY: &str = load_query!(
    concat!(
        r#"        OR (p.task=$15::text AND p.interval_id IS NOT NULL AND p.crontab_id IS NULL
           AND EXISTS (SELECT 1 FROM epg_epgsource s
                       WHERE s.id>0 AND s.is_active AND s.source_type='xmltv'
                         AND s.refresh_task_id=p.id AND p.name='epg_source-refresh-' || s.id::text)
           AND EXISTS (SELECT 1 FROM django_celery_beat_intervalschedule i
                       WHERE i.id=p.interval_id AND i.every>0 AND i.period='hours'))
        OR (p.task=$15::text AND p.interval_id IS NULL AND p.crontab_id IS NOT NULL
           AND NOT p.one_off AND p.start_time IS NULL
           AND p.expires IS NULL AND p.expire_seconds IS NULL
           AND p.queue IS NULL AND p.exchange IS NULL AND p.routing_key IS NULL AND p.priority IS NULL
           AND EXISTS (SELECT 1 FROM epg_epgsource s
                       WHERE s.id>0 AND s.is_active AND s.source_type='xmltv' AND s.refresh_interval=0
                         AND s.refresh_task_id=p.id AND p.name='epg_source-refresh-' || s.id::text)
           AND cr.timezone='UTC' AND cr.minute ~ '^[0-5]?[0-9]$'
           AND cr.hour ~ '^(0?[0-9]|1[0-9]|2[0-3])$'
           AND cr.day_of_week='*' AND cr.day_of_month='*' AND cr.month_of_year='*')
        OR (p.interval_id IS NOT NULL AND p.crontab_id IS NULL
           AND EXISTS (SELECT 1 FROM django_celery_beat_intervalschedule i
                       WHERE i.id=p.interval_id AND i.period='seconds'
                         AND ((p.name=$16::text AND p.task=$17::text AND i.every=$18::integer)
                           OR (p.name=$19::text AND p.task=$20::text AND i.every=$21::integer))))
        OR (p.task=$22::text AND p.name<>$23::text
            AND p.expires IS NULL AND p.expire_seconds IS NULL
            AND p.queue IS NULL AND p.exchange IS NULL AND p.routing_key IS NULL AND p.priority IS NULL
            AND p.args IN ('','[]') AND p.kwargs IN ('','{}') AND p.headers IN ('','{}')
            AND ((p.one_off AND p.crontab_id IS NULL
                  AND EXISTS (SELECT 1 FROM django_celery_beat_intervalschedule i
                              WHERE i.id=p.interval_id AND i.period='seconds' AND i.every>0))
                 OR (NOT p.one_off AND p.start_time IS NULL AND p.interval_id IS NULL
                     AND cr.timezone='UTC' AND cr.minute ~ '^[0-5]?[0-9]$'
                     AND cr.hour ~ '^(0?[0-9]|1[0-9]|2[0-3])$'
                     AND cr.day_of_week='*' AND cr.day_of_month='*' AND cr.month_of_year='*')))
         OR (p.name <> ALL($27::text[]) AND char_length(p.name) BETWEEN 1 AND 200
            AND p.total_run_count>=0 AND NOT p.one_off AND p.start_time IS NULL AND p.expires IS NULL
            AND p.queue IS NULL AND p.exchange IS NULL AND p.routing_key IS NULL AND p.priority IS NULL
            AND p.args IN ('','[]') AND p.kwargs IN ('','{}') AND p.headers IN ('','{}')
            AND ((p.task=$7::text AND p.expire_seconds=43200 AND p.interval_id IS NULL
                  AND cr.timezone='UTC' AND cr.minute ~ '^[0-5]?[0-9]$'
                  AND cr.hour ~ '^(0?[0-9]|1[0-9]|2[0-3])$'
                  AND cr.day_of_week='*' AND cr.day_of_month='*' AND cr.month_of_year='*')
                 OR (p.task IN ($9::text,$11::text) AND p.expire_seconds IS NULL AND p.crontab_id IS NULL
                     AND EXISTS (SELECT 1 FROM django_celery_beat_intervalschedule i
                                 WHERE i.id=p.interval_id AND i.every=$12::integer AND i.period='seconds'))))
         OR (p.task=$24::text AND p.interval_id IS NOT NULL AND p.crontab_id IS NULL
             AND EXISTS (SELECT 1 FROM m3u_m3uaccount a
                         JOIN django_celery_beat_intervalschedule i ON i.id=p.interval_id
                         WHERE a.id>0 AND a.refresh_task_id=p.id
                           AND p.name='m3u_account-refresh-' || a.id::text
                           AND (("#,
        local_m3u_account!(),
        ") OR ($26::boolean AND ",
        saved_xc_account!(),
        ")) AND ",
        local_m3u_interval!(),
        "))"
    ),
    r#" LEFT JOIN dispatcharr_channels_recording r ON p.task=$25::text
      AND p.name=r.task_id AND p.name='dvr-recording-' || r.id::text
      AND r.start_time=cl.clocked_time AND r.end_time>r.start_time
"#,
    r#"r.channel_id::bigint AS dvr_channel,r.end_time AS dvr_end,
    CASE WHEN $26::boolean AND p.task=$24::text THEN
        EXISTS (SELECT 1 FROM m3u_m3uaccount a WHERE a.account_type='XC'
                  AND a.refresh_task_id=p.id AND p.name='m3u_account-refresh-' || a.id::text)
        ELSE false END AS saved_xc"#,
    r#"        OR (p.task=$25::text AND p.one_off
           AND p.interval_id IS NULL AND p.crontab_id IS NULL AND p.clocked_id IS NOT NULL
           AND p.start_time IS NULL AND p.expires IS NULL AND p.expire_seconds IS NULL
           AND p.queue IS NULL AND p.exchange IS NULL AND p.routing_key IS NULL AND p.priority IS NULL
           AND octet_length(p.args)<=8192 AND octet_length(p.kwargs)<=8192 AND octet_length(p.headers)<=8192
           AND cl.clocked_time<=$1::timestamptz AND r.id IS NOT NULL)
"#,
);
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
    saved_cleanup: Option<&SavedCleanupSchedule>,
) -> Result<LoadedSchedules, Error> {
    // Source enabled_models_qs excludes far-future clocked and simple numeric
    // calendar hours BEFORE model loading. Apply those exact profile predicates
    // before LIMIT/serialization budget, not after an unrelated enabled-row cap.
    let filters = QueryFilters::new(now)?;
    let (name, task) = if let Some(saved_cleanup) = saved_cleanup {
        let (name, task) = saved_cleanup.row_identity();
        (Some(name), Some(task))
    } else if scope == ScheduleScope::SavedCleanup {
        return Err(Error::Supervisor);
    } else {
        scope.row_identity()
    };
    let expiration = scope
        .is_core()
        .then_some((EXPIRATION_PERIODIC_NAME, EXPIRATION_PERIODIC_TASK));
    let version = scope
        .is_core()
        .then_some((VERSION_PERIODIC_NAME, crate::version_protocol::TASK));
    let plugin_refresh = scope.includes_plugin_refresh().then_some((
        plugin_startup_schedule::NAME,
        crate::normal_plugin_repos::TASK,
    ));
    let selected_receivers = scope.uses_selected_receivers();
    let query = if selected_receivers {
        LOAD_SELECTED_CORE_QUERY
    } else {
        LOAD_QUERY
    };
    let query = sqlx::query(query)
        .bind(filters.cutoff)
        .bind(&filters.hours)
        .bind(&filters.numeric_hours)
        .bind(filters.zones)
        .bind(filters.offsets)
        .bind(name)
        .bind(task)
        .bind(expiration.map(|(name, _)| name))
        .bind(expiration.map(|(_, task)| task))
        .bind(version.map(|(name, _)| name))
        .bind(version.map(|(_, task)| task))
        .bind(DAILY_SECONDS)
        .bind(plugin_refresh.map(|(name, _)| name))
        .bind(plugin_refresh.map(|(_, task)| task));
    let query = if selected_receivers {
        query
            .bind(crate::refresh_protocol::NAME)
            .bind(SCAN_FILES.0)
            .bind(SCAN_FILES.1)
            .bind(SCAN_FILES.2)
            .bind(MAINTAIN_RECURRING.0)
            .bind(MAINTAIN_RECURRING.1)
            .bind(MAINTAIN_RECURRING.2)
            .bind(scope.includes_saved_stats().then_some(STATS_TASK))
            .bind(DISABLED_STATS_DEFAULT)
            .bind(crate::normal_xc_live::SINGLE_TASK_NAME)
            .bind(CLOCKED_DVR_TASK)
            .bind(scope.includes_saved_xc())
            .bind(RESERVED_PERIODIC_NAMES)
    } else {
        query
    };
    let rows = query.fetch_all(&mut *connection).await?;
    let budget = rows.first().ok_or(Error::Capacity)?;
    if budget.try_get::<i64, _>("rows")? > 256 || budget.try_get::<i64, _>("bytes")? > 1048576 {
        return Err(Error::Capacity);
    }
    let mut entries = BTreeMap::new();
    let mut invalid = 0;
    let mut loaded_cleanup = None;
    for row in rows {
        let Some(raw) = row.try_get::<Option<String>, _>("raw")? else {
            continue;
        };
        let value: Value =
            serde_json::from_str(&raw).map_err(|_| Error::Configuration("periodic row JSON"))?;
        let id = integer(&value, "id")?;
        let task = text(&value, "task")?;
        let name = text(&value, "name")?;
        let skip_invalid = scope.is_linked_xmltv_task(&task)
            || scope.is_linked_local_m3u_task(&task)
            || scope.is_linked_clocked_dvr(&task)
            || scope.selected_default_interval(&name, &task).is_some()
            || scope.is_named_builtin(&name, &task);
        let interval: Option<String> = row.try_get("interval_spec")?;
        let crontab: Option<String> = row.try_get("crontab_spec")?;
        let mut clocked: Option<DateTime<Utc>> = row.try_get("clocked_time")?;
        let saved_snapshot = if saved_cleanup.is_some() {
            crontab.as_ref().map(|cron| SavedCleanupSnapshot {
                periodic: raw,
                cron: cron.clone(),
            })
        } else {
            None
        };
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
            if skip_invalid {
                invalid += 1;
                continue;
            }
            return Err(Error::Capacity);
        }
        let args = serde_json::from_str::<Value>(if args.is_empty() { "[]" } else { &args });
        let kwargs = serde_json::from_str::<Value>(if kwargs.is_empty() { "{}" } else { &kwargs });
        let mut enabled = true;
        if args.is_err() || kwargs.is_err() {
            if skip_invalid {
                invalid += 1;
                continue;
            }
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
            name,
            task,
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
        let admitted = if let Some(saved_cleanup) = saved_cleanup {
            let raw_key = |name: &str| value.get(name).and_then(Value::as_i64);
            saved_cleanup.admits(&model, raw_key("clocked_id"), raw_key("solar_id"))
        } else if scope.is_linked_clocked_dvr(&model.task) {
            let channel: Option<i64> = row.try_get("dvr_channel")?;
            let end: Option<DateTime<Utc>> = row.try_get("dvr_end")?;
            clocked_dvr(&model, clocked)
                .is_some_and(|dvr| Some(dvr.channel) == channel && Some(dvr.end) == end)
        } else {
            scope.admits(&model)
        };
        if !admitted {
            if skip_invalid {
                invalid += 1;
                continue;
            }
            return Err(Error::Configuration("stored scoped schedule admission"));
        }
        if model.crontab.is_some()
            && (!(1970..=2037).contains(&model.last_run_at.year())
                || !(1970..=2037).contains(&now.year()))
        {
            return Err(Error::Configuration(
                "calendar date outside pinned Source TZif horizon",
            ));
        }
        if saved_cleanup.is_some() {
            loaded_cleanup = Some(saved_snapshot.ok_or(Error::Supervisor)?);
        }
        let mut cached = Cached {
            entry: model,
            clocked,
            saved_xc: row.try_get("saved_xc")?,
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
    Ok((entries, invalid, loaded_cleanup))
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
    let default_queue = if entry.task == CLOCKED_DVR_TASK {
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
        (DISABLED_STATS_DEFAULT, STATS_TASK, Some(2)),
        (SCAN_FILES.0, SCAN_FILES.1, Some(SCAN_FILES.2)),
        (
            MAINTAIN_RECURRING.0,
            MAINTAIN_RECURRING.1,
            Some(MAINTAIN_RECURRING.2),
        ),
        (
            VERSION_PERIODIC_NAME,
            crate::version_protocol::TASK,
            Some(DAILY_SECONDS),
        ),
        (
            EXPIRATION_PERIODIC_NAME,
            EXPIRATION_PERIODIC_TASK,
            Some(DAILY_SECONDS),
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
    // Match update_or_create on an existing row. PostgreSQL evaluates INSERT
    // identity defaults before testing conflicts, so use INSERT only on absence.
    macro_rules! default_fields {
        () => {
            concat!(
                "task=$2,interval_id=$3,crontab_id=$4,solar_id=NULL,clocked_id=NULL,",
                "args='[]',kwargs='{}',headers='{}',queue=NULL,exchange=NULL,routing_key=NULL,",
                "priority=NULL,expire_seconds=$7,enabled=saved.enabled AND $5,",
                "last_run_at=CASE WHEN saved.enabled AND $5 THEN saved.last_run_at ELSE NULL END,",
                "date_changed=$6"
            )
        };
    }
    let enabled = name != DISABLED_STATS_DEFAULT;
    let changed = Utc::now();
    let expiry = every.is_none().then_some(43_200i32);
    let write = |statement: &'static str| {
        sqlx::query(statement)
            .bind(name)
            .bind(task)
            .bind(interval)
            .bind(cron)
            .bind(enabled)
            .bind(changed)
            .bind(expiry)
    };
    let observed = async {
        let updated = write(concat!(
            "UPDATE django_celery_beat_periodictask AS saved SET ",
            default_fields!(),
            " WHERE saved.name=$1"
        ))
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() == 0 {
            // A creator can win between UPDATE and INSERT. Normalize the
            // winner using the same existing-row fields under the unique key.
            write(concat!(
                "INSERT INTO django_celery_beat_periodictask AS saved ",
                "(name,task,interval_id,crontab_id,args,kwargs,headers,enabled,total_run_count,",
                "date_changed,description,one_off,expire_seconds) ",
                "VALUES($1,$2,$3,$4,'[]','{}','{}',$5,0,$6,'',false,$7) ",
                "ON CONFLICT(name) DO UPDATE SET ",
                default_fields!()
            ))
            .execute(&mut *transaction)
            .await?;
        }
        Ok::<(), sqlx::Error>(())
    }
    .await;
    if let Err(error) = observed {
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
    fn selected_core_saved_xc_extends_only_the_linked_hourly_receiver_gate() {
        let selected = ScheduleScope::core_with_xmltv(false, true, false, true).unwrap();
        let without = ScheduleScope::core_with_xmltv(false, true, false, false).unwrap();
        assert!(selected.includes_saved_xc());
        assert!(!without.includes_saved_xc());
        assert!(!ScheduleScope::Core.includes_saved_xc());
        assert!(!LOAD_QUERY.contains("m3u_m3uaccount"));
        assert!(LOAD_SELECTED_CORE_QUERY.contains("$26::boolean AND a.account_type='XC'"));
        assert!(LOAD_SELECTED_CORE_QUERY.contains("a.account_type='STD' AND a.is_active"));
        assert!(LOCK_CURRENT_SAVED_XC.contains("i.period='hours' AND i.every=a.refresh_interval"));
        assert!(LOCK_CURRENT_SAVED_XC.contains("p.clocked_id IS NULL AND p.crontab_id IS NULL"));
        assert!(LOCK_CURRENT_SAVED_XC.contains("FOR SHARE OF p,i NOWAIT"));
        assert!(LOCK_CURRENT_SAVED_XC.contains("\"enable_vod\":true"));
        assert!(!LOCK_CURRENT_LOCAL_M3U.contains("a.account_type='XC'"));
        let mut entry = model(crate::normal_xc_live::SINGLE_TASK_NAME);
        entry.name = "m3u_account-refresh-17".into();
        entry.kwargs = json!({"account_id":17});
        entry.interval = Some((1, "hours".into()));
        assert_eq!(selected.local_m3u_account_id(&entry), Some(17));
        assert!(selected.admits(&entry));
        entry.kwargs = json!({"account_id":18});
        assert!(!selected.admits(&entry));
    }

    #[test]
    fn selected_core_linked_xmltv_cron_keeps_the_daily_utc_boundary() {
        let scope = ScheduleScope::core_with_xmltv(false, true, false, true).unwrap();
        let due = "2026-11-23T00:01:00Z".parse::<DateTime<Utc>>().unwrap();
        let mut entry = model(crate::refresh_protocol::NAME);
        entry.name = "epg_source-refresh-17".into();
        entry.kwargs = json!({"source_id":17});
        entry.interval = None;
        entry.crontab = Some((
            "01".into(),
            "0".into(),
            "*".into(),
            "*".into(),
            "*".into(),
            "UTC".into(),
        ));
        entry.last_run_at = due - chrono::Duration::seconds(75);
        assert_eq!(scope.xmltv_source_id(&entry), Some(17));
        assert_eq!(daily_utc_crontab(&entry), Some(("01", "0")));
        assert!(scope.admits(&entry));
        assert!(!ScheduleScope::Core.admits(&entry));
        let cached = Cached {
            entry: entry.clone(),
            clocked: None,
            saved_xc: false,
            when: 0.0,
            identity: Arc::new(()),
        };
        assert!(
            !decision(&cached, due - chrono::Duration::seconds(1))
                .unwrap()
                .due
        );
        assert!(decision(&cached, due).unwrap().due);

        let mut changed = entry.clone();
        changed.kwargs = json!({"source_id":18});
        assert!(!scope.admits(&changed));
        changed = entry.clone();
        changed.crontab.as_mut().unwrap().0 = "60".into();
        assert!(!scope.admits(&changed));
        changed = entry.clone();
        changed.crontab.as_mut().unwrap().5 = "America/St_Johns".into();
        assert!(!scope.admits(&changed));
        changed = entry.clone();
        changed.crontab.as_mut().unwrap().3 = "29".into();
        assert!(!scope.admits(&changed));
        changed = entry.clone();
        changed.enabled = false;
        assert!(!scope.admits(&changed));
        changed = entry;
        changed.one_off = true;
        assert!(!scope.admits(&changed));
        assert!(LOCK_CURRENT_XMLTV_CRON.contains("AND refresh_interval=0"));
        assert!(LOCK_CURRENT_XMLTV_CRON.contains("c.minute=$5::text AND c.hour=$6::text"));
        assert!(LOCK_CURRENT_XMLTV_CRON.contains("FOR SHARE OF p,c NOWAIT"));
        assert!(!LOAD_QUERY.contains("epg_epgsource"));
        assert!(
            LOAD_SELECTED_CORE_QUERY
                .find("AND s.refresh_interval=0")
                .unwrap()
                < LOAD_SELECTED_CORE_QUERY.find("LIMIT 257").unwrap()
        );
    }

    #[tokio::test]
    async fn selected_core_clocked_dvr_uses_the_existing_recording_receiver_envelope() {
        let scope = ScheduleScope::core_with_xmltv(false, true, false, false).unwrap();
        assert!(scope.is_linked_clocked_dvr(CLOCKED_DVR_TASK));
        assert!(!ScheduleScope::Core.is_linked_clocked_dvr(CLOCKED_DVR_TASK));
        assert!(!ScheduleScope::Backup.is_linked_clocked_dvr(CLOCKED_DVR_TASK));
        let mut entry = model(CLOCKED_DVR_TASK);
        entry.name = "dvr-recording-17".into();
        entry.interval = None;
        entry.crontab = None;
        entry.one_off = true;
        entry.args = json!([
            17,
            3,
            "2026-07-20 04:30:00.120000+00:00",
            "2026-07-20 04:31:00+00:00"
        ]);
        let scheduled = DateTime::parse_from_rfc3339("2026-07-20T04:30:00.120000Z")
            .unwrap()
            .to_utc();
        let binding = clocked_dvr(&entry, Some(scheduled)).unwrap();
        assert_eq!(
            (binding.recording, binding.channel, binding.start),
            (17, 3, scheduled)
        );
        assert!(!scope.admits(&entry));
        assert!(clocked_dvr(&entry, None).is_none());
        assert!(clocked_dvr(&entry, Some(scheduled + chrono::Duration::seconds(1))).is_none());
        let mut changed = entry.clone();
        changed.name = "dvr-recording-18".into();
        assert!(clocked_dvr(&changed, Some(scheduled)).is_none());
        changed = entry.clone();
        changed.args[1] = json!(0);
        assert!(clocked_dvr(&changed, Some(scheduled)).is_none());
        changed = entry.clone();
        changed.args[3] = json!("2026-07-20T04:31:00Z");
        assert!(clocked_dvr(&changed, Some(scheduled)).is_none());
        changed = entry.clone();
        changed.queue = Some("celery".into());
        assert!(clocked_dvr(&changed, Some(scheduled)).is_none());
        changed = entry.clone();
        changed.one_off = false;
        assert!(clocked_dvr(&changed, Some(scheduled)).is_none());
        assert!(linked_arguments_current(
            &entry.args.to_string(),
            "{}",
            "{}",
            &entry
        ));
        assert!(!linked_arguments_current(
            "[17,3,\"2026-07-20 04:30:00.120000+00:00\",\"2026-07-20 04:32:00+00:00\"]",
            "{}",
            "{}",
            &entry
        ));
        let link = "p.name=r.task_id AND p.name='dvr-recording-' || r.id::text";
        assert!(!LOAD_QUERY.contains(link));
        assert!(
            LOAD_SELECTED_CORE_QUERY.find(link).unwrap()
                < LOAD_SELECTED_CORE_QUERY.find("LIMIT 257").unwrap()
        );
        assert!(LOCK_CURRENT_CLOCKED_DVR.contains("FOR SHARE OF p,c NOWAIT"));
        assert!(LOCK_CURRENT_CLOCKED_DVR.contains("c.clocked_time=$3::timestamptz"));

        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://127.0.0.1/unused_metadata_only")
            .unwrap();
        let auth = dispatcharr_store::AuthService::new(
            dispatcharr_store::Store::from_pool(pool),
            dispatcharr_store::AuthConfig::new("unused-metadata-only".into()).unwrap(),
        );
        let id = Uuid::new_v4();
        let publication =
            prepare_publication(&entry, id, &auth.producer_identity_source()).unwrap();
        let parsed =
            crate::clocked_recording_envelope::parse_normal_head(publication.bytes()).unwrap();
        assert_eq!(publication.redis_queue(), "dvr");
        assert_eq!(parsed.task_id(), id);
        assert_eq!(parsed.periodic_name(), "dvr-recording-17");
        assert_eq!((parsed.recording_id(), parsed.channel_id()), (17, 3));
        assert_eq!((parsed.start(), parsed.end()), (binding.start, binding.end));
    }

    #[test]
    fn selected_core_admits_only_canonical_saved_local_m3u_interval() {
        let scope = ScheduleScope::core_with_xmltv(false, true, false, false).unwrap();
        let mut entry = model(crate::normal_xc_live::SINGLE_TASK_NAME);
        entry.name = "m3u_account-refresh-101".into();
        entry.interval = Some((2, "hours".into()));
        entry.kwargs = json!({"account_id":101});
        assert_eq!(scope.local_m3u_account_id(&entry), Some(101));
        assert!(scope.admits(&entry));
        assert!(!ScheduleScope::Core.admits(&entry));
        assert!(linked_arguments_current(
            "[]",
            r#"{"account_id": 101}"#,
            "{}",
            &entry
        ));
        assert!(!linked_arguments_current(
            "[]",
            r#"{"account_id": 99}"#,
            "{}",
            &entry
        ));

        for kwargs in [
            json!({"account_id":102}),
            json!({"account_id":"101"}),
            json!({"account_id":101,"force":false}),
            json!({}),
        ] {
            let mut candidate = entry.clone();
            candidate.kwargs = kwargs;
            assert!(!scope.admits(&candidate));
        }
        let mut invalid = entry.clone();
        invalid.interval = Some((2, "seconds".into()));
        assert!(!scope.admits(&invalid));
        let mut invalid = entry.clone();
        invalid.one_off = true;
        assert!(!scope.admits(&invalid));
        let mut invalid = entry;
        invalid.queue = Some("celery".into());
        assert!(!scope.admits(&invalid));
    }

    #[test]
    fn selected_local_account_url_override_uses_same_locked_snapshot_guard() {
        let account_predicate = local_m3u_account!();
        let selection = LOAD_SELECTED_CORE_QUERY.find(account_predicate).unwrap();
        assert!(selection < LOAD_SELECTED_CORE_QUERY.find("LIMIT 257").unwrap());
        assert!(LOCK_CURRENT_LOCAL_M3U.contains(account_predicate));
        assert!(account_predicate.contains(r#"'{"auto_enable_new_groups_live":false}'::jsonb"#));
        assert!(!LOCK_CURRENT_SAVED_XC.contains(account_predicate));
        assert!(LOCK_CURRENT_LOCAL_M3U.contains("FOR SHARE NOWAIT"));
        assert!(LOCK_CURRENT_LOCAL_M3U.contains("p.id=a.refresh_task_id"));
        assert!(LOCK_CURRENT_LOCAL_M3U.contains("p.total_run_count=$6::bigint"));
        assert!(LOCK_CURRENT_LOCAL_M3U.contains("FOR SHARE OF p,i NOWAIT"));
    }

    #[tokio::test]
    #[ignore = "requires a disposable PostgreSQL connection in DISPATCHARR_BEAT_TEST_DATABASE_URL"]
    async fn selected_local_account_url_override_matches_actual_jsonb_predicate() {
        let mut pg =
            PgConnection::connect(&std::env::var("DISPATCHARR_BEAT_TEST_DATABASE_URL").unwrap())
                .await
                .unwrap();
        // No schema is needed. Run the exact shared account predicate against
        // typed PostgreSQL rows, including rows the SQL must actively exclude.
        let query = format!(
            r#"SELECT item.ord
               FROM unnest($1::jsonb[]) WITH ORDINALITY AS item(properties,ord)
               CROSS JOIN LATERAL (SELECT 'STD'::text account_type,true is_active,
                   NULL::text server_url,'/data/m3us/synthetic.m3u'::text file_path,
                   NULL::integer user_agent_id,NULL::integer stream_profile_id,
                   NULL::text username,NULL::text password,item.properties custom_properties,
                   'Owned local'::text name,NULL::text last_message) AS a
               WHERE {} ORDER BY item.ord"#,
            local_m3u_account!()
        );
        let defaults = json!({"enable_vod":false,"auto_enable_new_groups_live":true,
            "auto_enable_new_groups_vod":true,"auto_enable_new_groups_series":true});
        let mut override_defaults = defaults.clone();
        override_defaults["hash_key"] = json!("url");
        let mut disabled_defaults = defaults.clone();
        disabled_defaults["auto_enable_new_groups_live"] = json!(false);
        let mut disabled_override = disabled_defaults.clone();
        disabled_override["hash_key"] = json!("url");
        let rows = vec![
            json!({}),
            defaults,
            json!({"hash_key":"url"}),
            override_defaults,
            json!({"auto_enable_new_groups_live":false}),
            disabled_defaults,
            json!({"hash_key":"url","auto_enable_new_groups_live":false}),
            disabled_override,
            json!({"auto_enable_new_groups_live":"false"}),
            json!({"auto_enable_new_groups_live":false,"enable_vod":true}),
            json!({"hash_key":"name,url"}),
            json!({"hash_key":null}),
            json!({"hash_key":"unknown"}),
            json!({"hash_key":["url"]}),
            json!({"hash_key":"url","enable_vod":true}),
            json!(["url"]),
        ];
        let found: Vec<i64> = sqlx::query_scalar(&query)
            .bind(rows)
            .fetch_all(&mut pg)
            .await
            .unwrap();
        assert_eq!(found, [1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn scoped_xmltv_rechecks_actual_link_and_rejects_changed_arguments() {
        let mut entry = model(crate::refresh_protocol::NAME);
        entry.name = "epg_source-refresh-17".into();
        entry.task = crate::refresh_protocol::NAME.into();
        entry.interval = Some((1, "hours".into()));
        entry.crontab = None;
        entry.kwargs = json!({"source_id":17});
        let scope = ScheduleScope::core_with_xmltv(true, true, false, false).unwrap();
        assert!(scope.admits(&entry));
        assert!(!ScheduleScope::CoreWithPluginRefresh.admits(&entry));
        assert!(ScheduleScope::core_with_xmltv(false, false, false, false).is_err());
        assert_eq!(scope.xmltv_source_id(&entry), Some(17));
        let mut second = entry.clone();
        second.name = "epg_source-refresh-29".into();
        assert!(!scope.admits(&second));
        second.kwargs = json!({"source_id":29});
        assert_eq!(scope.xmltv_source_id(&second), Some(29));
        assert!(scope.admits(&second));
        assert!(scope.admits(&entry));
        let mut unsupported = second.clone();
        unsupported.kwargs = json!({"source_id":0});
        unsupported.name = "epg_source-refresh-0".into();
        assert!(!scope.admits(&unsupported));
        unsupported = second.clone();
        unsupported.task = crate::refresh_aggregate::NAME.into();
        assert!(!scope.admits(&unsupported));
        unsupported = second;
        unsupported.interval = Some((1, "minutes".into()));
        assert!(!scope.admits(&unsupported));
        assert!(linked_arguments_current(
            "[]",
            "{\"source_id\": 17}",
            "{}",
            &entry
        ));
        assert!(linked_arguments_current(
            "",
            "{\"source_id\":17}",
            "",
            &entry
        ));
        for kwargs in [
            "{",
            "{\"source_id\": 18}",
            "{\"source_id\": \"17\"}",
            "{\"source_id\":17, \"force\":false}",
            "{}",
        ] {
            assert!(!linked_arguments_current("[]", kwargs, "{}", &entry));
        }
        assert!(!linked_arguments_current(
            "[1]",
            "{\"source_id\":17}",
            "{}",
            &entry
        ));
        assert!(!linked_arguments_current(
            "[]",
            "{\"source_id\":17}",
            "{\"extra\":true}",
            &entry
        ));
        assert!(LOCK_CURRENT_XMLTV.contains("s.refresh_task_id"));
        assert!(LOCK_CURRENT_XMLTV.contains("AND is_active AND source_type='xmltv'"));
        assert!(LOCK_CURRENT_XMLTV.contains("p.enabled AND NOT p.one_off"));
        assert!(LOCK_CURRENT_XMLTV.contains("FOR SHARE NOWAIT"));
        assert!(LOCK_CURRENT_XMLTV.contains("FOR SHARE OF p,i NOWAIT"));
        assert!(LOCK_CURRENT_XMLTV.contains("p.total_run_count=$6::bigint"));
        assert!(
            LOCK_CURRENT_XMLTV.contains("COALESCE(p.last_run_at,p.date_changed)=$7::timestamptz")
        );
        assert!(LOAD_SELECTED_CORE_QUERY.contains("s.refresh_task_id=p.id"));
        assert!(
            LOAD_SELECTED_CORE_QUERY.contains("s.id>0 AND s.is_active AND s.source_type='xmltv'")
        );
        assert!(LOAD_SELECTED_CORE_QUERY.contains("p.name='epg_source-refresh-' || s.id::text"));
        assert!(!LOAD_SELECTED_CORE_QUERY.contains("s.id=$"));
        assert!(!LOAD_QUERY.contains("epg_epgsource"));
        assert!(
            LOAD_SELECTED_CORE_QUERY.find("s.refresh_task_id=p.id")
                < LOAD_SELECTED_CORE_QUERY.find("LIMIT 257")
        );
    }

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
            lifecycle.reserve(Operation::TimerTick),
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
        assert!(matches!(
            lifecycle.reserve(Operation::ManualTick),
            Err(Error::Closed)
        ));
        let active_tick = lifecycle.reserve(Operation::TimerTick).unwrap();
        lifecycle.close();
        assert!(*lifecycle.stop.borrow());
        assert!(!*lifecycle.done.borrow());
        assert!(lifecycle.slots.try_acquire().is_err());
        assert!(matches!(
            lifecycle.reserve(Operation::TimerTick),
            Err(Error::Closed)
        ));
        drop(active_tick);
        lifecycle.finish(Status::Stopped);
        assert!(*lifecycle.done.borrow());
        assert!(matches!(*lifecycle.status.borrow(), Status::Stopped));
        assert!(lifecycle.slots.try_acquire().is_ok());
    }
    #[test]
    fn physical_owner_returns_the_permit_only_after_the_last_transport_reference() {
        let slots = Arc::new(Semaphore::new(1));
        let (physical, mut completed) =
            PhysicalPermit::new(slots.clone().try_acquire_owned().unwrap());
        let owner = Arc::new(physical);
        let transport = Arc::clone(&owner);
        drop(owner);
        assert!(matches!(
            completed.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(slots.clone().try_acquire_owned().is_err());
        drop(transport);
        let returned = completed.try_recv().unwrap();
        assert!(slots.clone().try_acquire_owned().is_err());
        drop(returned);
        assert!(slots.try_acquire_owned().is_ok());
    }

    #[test]
    fn settled_contender_admits_cleanup_but_dirty_or_faulted_does_not() {
        assert!(shutdown_result(&Status::Contended, false).is_ok());
        assert!(matches!(
            shutdown_result(&Status::SettlingFault, false),
            Err(Error::Drain)
        ));
        assert!(matches!(
            shutdown_result(&Status::Contended, true),
            Err(Error::Drain)
        ));
        assert!(matches!(
            shutdown_result(&Status::Stopped, true),
            Err(Error::Drain)
        ));
        assert!(matches!(
            shutdown_result(&Status::Fault(Arc::new(Error::Supervisor)), false),
            Err(Error::Drain)
        ));
    }

    #[test]
    fn only_acknowledged_linked_clocked_dvr_can_confirm_retirement() {
        for (linked_clocked_dvr, acknowledged) in [(false, false), (false, true), (true, false)] {
            let checkpoint = Checkpoint::after_publication(linked_clocked_dvr, acknowledged, 7);
            assert_eq!(checkpoint, Checkpoint::Required);
            assert!(!checkpoint.can_confirm_retirement(7));
        }
        let published = Checkpoint::after_publication(true, true, 7);
        assert!(published.can_confirm_retirement(7));
        assert!(!published.can_confirm_retirement(8));
    }

    #[tokio::test]
    #[ignore = "requires a disposable PostgreSQL connection in DISPATCHARR_BEAT_TEST_DATABASE_URL"]
    async fn acknowledged_clocked_dvr_checkpoint_retires_after_worker_deletion() {
        let url = std::env::var("DISPATCHARR_BEAT_TEST_DATABASE_URL").unwrap();
        let mut connection = PgConnection::connect(&url).await.unwrap();
        sqlx::query("CREATE TEMP TABLE django_celery_beat_periodictask(id bigint PRIMARY KEY,name text UNIQUE NOT NULL,enabled boolean NOT NULL,last_run_at timestamptz,total_run_count bigint NOT NULL,date_changed timestamptz)")
            .execute(&mut connection).await.unwrap();
        let mut entry = model(CLOCKED_DVR_TASK);
        entry.name = "dvr-recording-17".into();
        let name = entry.name.clone();
        let id = entry.id;
        let published = Checkpoint::after_publication(true, true, id);
        let mut cache = Cache::default();
        cache.entries.insert(
            name.clone(),
            Cached {
                entry,
                clocked: None,
                saved_xc: false,
                when: 0.0,
                identity: Arc::new(()),
            },
        );

        sqlx::query("INSERT INTO django_celery_beat_periodictask(id,name,enabled,total_run_count) VALUES($1,$2,true,0)")
            .bind(id).bind(&name).execute(&mut connection).await.unwrap();
        cache.dirty.insert(name.clone(), published);
        assert_eq!(
            sync(&mut connection, &mut cache, true).await.unwrap(),
            (1, 0)
        );
        sqlx::query("DELETE FROM django_celery_beat_periodictask WHERE id=$1")
            .bind(id)
            .execute(&mut connection)
            .await
            .unwrap();

        cache.dirty.insert(name.clone(), Checkpoint::Required);
        assert_eq!(
            sync(&mut connection, &mut cache, true).await.unwrap(),
            (0, 1)
        );
        assert_eq!(cache.dirty.get(&name), Some(&Checkpoint::Required));

        cache.dirty.insert(name.clone(), published);
        assert_eq!(
            sync(&mut connection, &mut cache, true).await.unwrap(),
            (0, 0)
        );
        assert!(shutdown_result(&Status::Stopped, !cache.dirty.is_empty()).is_ok());

        sqlx::query("INSERT INTO django_celery_beat_periodictask(id,name,enabled,total_run_count) VALUES($1,$2,true,0)")
            .bind(id + 1).bind(&name).execute(&mut connection).await.unwrap();
        cache.dirty.insert(name.clone(), published);
        assert_eq!(
            sync(&mut connection, &mut cache, true).await.unwrap(),
            (0, 1)
        );
        cache.entries.get_mut(&name).unwrap().entry.id += 1;
        assert_eq!(
            sync(&mut connection, &mut cache, true).await.unwrap(),
            (0, 1)
        );
        assert_eq!(cache.dirty.get(&name), Some(&published));

        cache.entries.get_mut(&name).unwrap().entry.id = id;
        sqlx::query("ALTER TABLE django_celery_beat_periodictask DROP COLUMN total_run_count")
            .execute(&mut connection)
            .await
            .unwrap();
        assert!(matches!(
            sync(&mut connection, &mut cache, true).await,
            Err(Error::Database(_))
        ));
        assert_eq!(cache.dirty.get(&name), Some(&published));
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
            ScheduleScope::Core.row_identity(),
            (Some(CLEANUP_PERIODIC_TASK), Some(CLEANUP_PERIODIC_TASK))
        );
        assert!(ScheduleScope::Core.admits(&entry));
        assert!(!ScheduleScope::Backup.admits(&entry));
        entry.name = BACKUP_PERIODIC_NAME.into();
        assert!(!ScheduleScope::Core.admits(&entry));
    }
    #[tokio::test]
    async fn selected_core_keeps_distinct_named_cleanup_version_and_expiration_rows() {
        let selected = ScheduleScope::core_with_xmltv(false, true, false, false).unwrap();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://127.0.0.1/unused_metadata_only")
            .unwrap();
        let auth = dispatcharr_store::AuthService::new(
            dispatcharr_store::Store::from_pool(pool),
            dispatcharr_store::AuthConfig::new("unused-metadata-only".into()).unwrap(),
        );
        let identity = auth.producer_identity_source();
        let mut cleanup = model(CLEANUP_PERIODIC_TASK);
        cleanup.name = "owned-restart-backend-cleanup".into();
        cleanup.interval = None;
        cleanup.crontab = Some((
            "0".into(),
            "4".into(),
            "*".into(),
            "*".into(),
            "*".into(),
            "UTC".into(),
        ));
        cleanup.expire_seconds = Some(43_200);
        let mut version = model(crate::version_protocol::TASK);
        version.name = "Second release check".into();
        version.interval = Some((DAILY_SECONDS, "seconds".into()));
        let mut expiration = model(EXPIRATION_PERIODIC_TASK);
        expiration.name = "Daily account expiration reminders".into();
        expiration.interval = Some((DAILY_SECONDS, "seconds".into()));
        for entry in [cleanup, version, expiration] {
            assert!(selected.admits(&entry));
            assert!(!ScheduleScope::Core.admits(&entry));
            let mut sibling = entry.clone();
            sibling.id += 1;
            sibling.name.push_str(" copy");
            sibling.total_run_count += 1;
            assert!(selected.admits(&sibling));
            assert_ne!(sibling.id, entry.id);
            assert_ne!(sibling.name, entry.name);
            assert_ne!(sibling.total_run_count, entry.total_run_count);
            for saved in [&entry, &sibling] {
                let id = Uuid::new_v4();
                let published = prepare_publication(saved, id, &identity).unwrap();
                let envelope: Value = serde_json::from_slice(published.bytes()).unwrap();
                assert_eq!(envelope["headers"]["periodic_task_name"], saved.name);
                assert_eq!(published.redis_queue(), "celery");
                if saved.task == crate::version_protocol::TASK {
                    assert_eq!(
                        crate::version_protocol::parse(published.bytes())
                            .unwrap()
                            .task_id,
                        id
                    );
                } else {
                    let parsed = crate::default_protocol::parse(published.bytes()).unwrap();
                    assert_eq!(parsed.task_id, id);
                    let expected = if saved.task == CLEANUP_PERIODIC_TASK {
                        crate::default_protocol::Kind::RedisBackendCleanup
                    } else {
                        crate::default_protocol::Kind::InactiveAccountNotifications
                    };
                    assert_eq!(parsed.kind, expected);
                }
            }
            let mut invalid = entry.clone();
            invalid.queue = Some("dvr".into());
            assert!(!selected.admits(&invalid));
            let mut invalid = entry.clone();
            invalid.kwargs = json!({"unexpected": true});
            assert!(!selected.admits(&invalid));
            let mut invalid = entry.clone();
            invalid.task = "core.tasks.scan_and_process_files".into();
            assert!(!selected.admits(&invalid));
            for reserved in RESERVED_PERIODIC_NAMES {
                assert!(!selected.is_named_builtin(reserved, &entry.task));
            }
        }
        let candidate = LOAD_SELECTED_CORE_QUERY
            .find("p.name <> ALL($27::text[])")
            .unwrap();
        let named_daily = LOAD_SELECTED_CORE_QUERY
            .find("p.task IN ($9::text,$11::text)")
            .unwrap();
        assert!(candidate < named_daily);
        assert!(named_daily < LOAD_SELECTED_CORE_QUERY.find("LIMIT 257").unwrap());
        assert!(!LOAD_QUERY.contains("$27"));
    }
    #[test]
    fn core_expiration_scope_admits_only_the_canonical_saved_daily_task() {
        let mut entry = model(EXPIRATION_PERIODIC_TASK);
        entry.name = EXPIRATION_PERIODIC_NAME.into();
        entry.interval = Some((DAILY_SECONDS, "seconds".into()));
        assert!(ScheduleScope::Core.admits(&entry));
        assert!(!ScheduleScope::Backup.admits(&entry));

        let mut changed = entry.clone();
        changed.interval = Some((60, "seconds".into()));
        assert!(!ScheduleScope::Core.admits(&changed));
        changed = entry.clone();
        changed.name = "different-saved-expiration-row".into();
        assert!(!ScheduleScope::Core.admits(&changed));
        changed = entry.clone();
        changed.kwargs = json!({"unexpected": true});
        assert!(!ScheduleScope::Core.admits(&changed));
        entry.one_off = true;
        assert!(!ScheduleScope::Core.admits(&entry));
    }

    #[tokio::test]
    async fn selected_core_defaults_publish_to_registered_receiver_families() {
        use crate::default_protocol::Kind;
        use crate::milestone_dispatch::{builtin_family, Family, REQUIRED};
        let scope = ScheduleScope::core_with_xmltv(false, true, false, false).unwrap();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://127.0.0.1/unused_metadata_only")
            .unwrap();
        let auth = dispatcharr_store::AuthService::new(
            dispatcharr_store::Store::from_pool(pool),
            dispatcharr_store::AuthConfig::new("unused-metadata-only".into()).unwrap(),
        );
        let identity = auth.producer_identity_source();
        let mut cleanup = model(CLEANUP_PERIODIC_TASK);
        cleanup.name = CLEANUP_PERIODIC_TASK.into();
        cleanup.interval = None;
        cleanup.crontab = Some((
            "0".into(),
            "4".into(),
            "*".into(),
            "*".into(),
            "*".into(),
            "UTC".into(),
        ));
        cleanup.expire_seconds = Some(43_200);
        let mut entries = vec![(
            cleanup,
            Family::BackendCleanup,
            Some(Kind::RedisBackendCleanup),
        )];
        for ((name, task, seconds), family, kind) in [
            (SCAN_FILES, Family::Scanner, Some(Kind::LogoScanner)),
            (
                MAINTAIN_RECURRING,
                Family::Recurring,
                Some(Kind::RecurringMaintenance),
            ),
            (
                (
                    EXPIRATION_PERIODIC_NAME,
                    EXPIRATION_PERIODIC_TASK,
                    DAILY_SECONDS,
                ),
                Family::AccountExpiration,
                Some(Kind::InactiveAccountNotifications),
            ),
            (
                (
                    VERSION_PERIODIC_NAME,
                    crate::version_protocol::TASK,
                    DAILY_SECONDS,
                ),
                Family::VersionCheck,
                None,
            ),
        ] {
            let mut entry = model(task);
            entry.name = name.into();
            entry.interval = Some((seconds, "seconds".into()));
            entries.push((entry, family, kind));
        }
        for (entry, family, kind) in entries {
            assert!(scope.admits(&entry));
            assert!(REQUIRED.contains(&family));
            assert_eq!(builtin_family(&entry.task), Some(family));
            let id = Uuid::new_v4();
            let frame = prepare_publication(&entry, id, &identity).unwrap();
            let envelope: Value = serde_json::from_slice(frame.bytes()).unwrap();
            assert_eq!(frame.redis_queue(), "celery");
            assert_eq!(envelope["headers"]["periodic_task_name"], entry.name);
            if let Some(kind) = kind {
                let parsed = crate::default_protocol::parse(frame.bytes()).unwrap();
                assert_eq!(parsed.kind, kind);
                assert_eq!(parsed.task_id, id);
            } else {
                assert_eq!(
                    crate::version_protocol::parse(frame.bytes())
                        .unwrap()
                        .task_id,
                    id
                );
            }
            if matches!(family, Family::Scanner | Family::Recurring) {
                assert!(!ScheduleScope::Core.admits(&entry));
                assert!(!ScheduleScope::CoreWithPluginRefresh.admits(&entry));
                assert!(!ScheduleScope::Backup.admits(&entry));
                let mut different = entry.clone();
                different.one_off = true;
                assert!(!scope.admits(&different));
                different.one_off = false;
                different.enabled = false;
                assert!(!scope.admits(&different));
                different.enabled = true;
                different.interval.as_mut().unwrap().0 += 1;
                assert!(!scope.admits(&different));
            }
        }
        let mut stats = model("apps.proxy.tasks.fetch_channel_stats");
        stats.name = "fetch-channel-statuses".into();
        stats.interval = Some((2, "seconds".into()));
        assert!(!scope.admits(&stats));
    }

    #[tokio::test]
    async fn selected_core_saved_stats_uses_the_existing_guards_and_receiver_frame() {
        use crate::milestone_dispatch::{builtin_family, Family};

        let scope = ScheduleScope::core_with_xmltv(false, true, true, false).unwrap();
        let no_stats = ScheduleScope::core_with_xmltv(false, true, false, false).unwrap();
        let mut entry = model(STATS_TASK);
        let now = entry.last_run_at;
        entry.name = "operator-saved-stats".into();
        entry.start_time = Some(now);
        entry.last_run_at = now - chrono::Duration::seconds(30);
        entry.one_off = true;
        assert!(scope.admits(&entry));
        assert!(!no_stats.admits(&entry) && !ScheduleScope::Core.admits(&entry));
        for invalid in [
            Entry {
                name: DISABLED_STATS_DEFAULT.into(),
                ..entry.clone()
            },
            Entry {
                enabled: false,
                ..entry.clone()
            },
            Entry {
                kwargs: json!({"other": true}),
                ..entry.clone()
            },
            Entry {
                queue: Some("other".into()),
                ..entry.clone()
            },
        ] {
            assert!(!scope.admits(&invalid));
        }
        let mut cached = Cached {
            entry: entry.clone(),
            clocked: None,
            saved_xc: false,
            when: 0.0,
            identity: Arc::new(()),
        };
        let before = decision(&cached, now - chrono::Duration::milliseconds(100)).unwrap();
        assert!(!before.due && before.next_seconds == 1.0);
        assert!(decision(&cached, now).unwrap().due);
        cached.entry.total_run_count = 1;
        assert_eq!(
            decision(&cached, now).unwrap().persistence,
            PersistenceEffect::DisableCompletedOneOff
        );

        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://127.0.0.1/unused_metadata_only")
            .unwrap();
        let auth = dispatcharr_store::AuthService::new(
            dispatcharr_store::Store::from_pool(pool),
            dispatcharr_store::AuthConfig::new("unused-metadata-only".into()).unwrap(),
        );
        let id = Uuid::new_v4();
        let frame = prepare_publication(&entry, id, &auth.producer_identity_source()).unwrap();
        assert_eq!(builtin_family(&entry.task), Some(Family::ProxyStats));
        assert_eq!(frame.redis_queue(), "celery");
        assert_eq!(
            crate::stats_protocol::parse(frame.bytes()).unwrap().task_id,
            id
        );
        let frame: Value = serde_json::from_slice(frame.bytes()).unwrap();
        assert_eq!(frame["headers"]["periodic_task_name"], entry.name);
        assert!(frame["headers"]["eta"].is_null() && frame["headers"]["expires"].is_null());

        entry.one_off = false;
        entry.start_time = None;
        entry.interval = None;
        entry.crontab = Some((
            "0".into(),
            "12".into(),
            "*".into(),
            "*".into(),
            "*".into(),
            "UTC".into(),
        ));
        assert!(scope.admits(&entry));
        entry.crontab.as_mut().unwrap().5 = "America/St_Johns".into();
        assert!(!scope.admits(&entry));
    }

    #[tokio::test]
    async fn core_daily_tasks_use_the_ordinary_due_frames_and_worker_parsers() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://127.0.0.1/unused_metadata_only")
            .unwrap();
        let auth = dispatcharr_store::AuthService::new(
            dispatcharr_store::Store::from_pool(pool),
            dispatcharr_store::AuthConfig::new("unused-metadata-only".into()).unwrap(),
        );
        let mut entry = model(EXPIRATION_PERIODIC_TASK);
        entry.name = EXPIRATION_PERIODIC_NAME.into();
        entry.interval = Some((DAILY_SECONDS, "seconds".into()));
        assert!(ScheduleScope::Core.admits(&entry));
        let id = Uuid::new_v4();
        let publication =
            prepare_publication(&entry, id, &auth.producer_identity_source()).unwrap();
        let parsed = crate::default_protocol::parse(publication.bytes()).unwrap();
        assert_eq!(publication.redis_queue(), "celery");
        assert_eq!(parsed.task_id, id);
        assert_eq!(
            parsed.kind,
            crate::default_protocol::Kind::InactiveAccountNotifications
        );
        let envelope: serde_json::Value = serde_json::from_slice(publication.bytes()).unwrap();
        assert_eq!(
            envelope["headers"]["periodic_task_name"],
            EXPIRATION_PERIODIC_NAME
        );
        assert_eq!(envelope["headers"]["parent_id"], Value::Null);
        assert!(parsed.expires.is_none());

        entry.name = VERSION_PERIODIC_NAME.into();
        assert!(!ScheduleScope::Core.admits(&entry));
        entry.task = crate::version_protocol::TASK.into();
        assert!(ScheduleScope::Core.admits(&entry));
        assert!(!ScheduleScope::Backup.admits(&entry));
        let id = Uuid::new_v4();
        let publication =
            prepare_publication(&entry, id, &auth.producer_identity_source()).unwrap();
        assert_eq!(publication.redis_queue(), "celery");
        assert_eq!(
            crate::version_protocol::parse(publication.bytes())
                .unwrap()
                .task_id,
            id
        );
        let envelope: serde_json::Value = serde_json::from_slice(publication.bytes()).unwrap();
        assert_eq!(
            envelope["headers"]["periodic_task_name"],
            VERSION_PERIODIC_NAME
        );
        entry.interval = Some((1, "days".into()));
        assert!(!ScheduleScope::Core.admits(&entry));
    }

    #[tokio::test]
    async fn core_plugin_schedule_uses_the_same_plain_repository_refresh_worker_frame() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://127.0.0.1/unused_metadata_only")
            .unwrap();
        let auth = dispatcharr_store::AuthService::new(
            dispatcharr_store::Store::from_pool(pool),
            dispatcharr_store::AuthConfig::new("unused-metadata-only".into()).unwrap(),
        );
        let mut entry = model(crate::normal_plugin_repos::TASK);
        entry.name = plugin_startup_schedule::NAME.into();
        entry.interval = Some((6, "hours".into()));
        let scope = ScheduleScope::CoreWithPluginRefresh;
        assert!(scope.admits(&entry));
        assert!(!ScheduleScope::Core.admits(&entry));
        assert!(!ScheduleScope::Backup.admits(&entry));

        let id = Uuid::new_v4();
        let publication =
            prepare_publication(&entry, id, &auth.producer_identity_source()).unwrap();
        let bytes = publication.bytes();
        let parsed = crate::normal_plugin_repos::parse(bytes).unwrap();
        assert_eq!(publication.redis_queue(), "celery");
        assert_eq!(parsed.id, id);
        assert_eq!(parsed.eta, None);
        assert_eq!(
            crate::milestone_dispatch::classify(bytes),
            Some(crate::milestone_dispatch::Family::PluginRefresh)
        );
        let envelope: Value = serde_json::from_slice(bytes).unwrap();
        assert_eq!(
            envelope["headers"]["periodic_task_name"],
            plugin_startup_schedule::NAME
        );

        entry.interval = Some((3, "hours".into()));
        assert!(scope.admits(&entry));
        let mut changed = entry.clone();
        changed.name = "different-saved-refresh-row".into();
        assert!(!scope.admits(&changed));
        changed = entry.clone();
        changed.interval = Some((180, "minutes".into()));
        assert!(!scope.admits(&changed));
        changed.interval = Some((0, "hours".into()));
        assert!(!scope.admits(&changed));
        changed = entry.clone();
        changed.routing_key = Some("other".into());
        assert!(!scope.admits(&changed));
        changed = entry.clone();
        changed.kwargs = json!({"unexpected": true});
        assert!(!scope.admits(&changed));
        entry.one_off = true;
        assert!(!scope.admits(&entry));
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
            saved_xc: false,
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
                saved_xc: false,
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
                saved_xc: false,
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
            saved_xc: false,
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

mod saved_cleanup;
use saved_cleanup::{
    observe_saved_cleanup_tick, publish_saved_cleanup, SavedCleanupSchedule, SavedCleanupSnapshot,
};

type LoadedSchedules = (
    BTreeMap<String, Cached>,
    usize,
    Option<SavedCleanupSnapshot>,
);

#[path = "plugin_startup_schedule.rs"]
mod plugin_startup_schedule;
pub use plugin_startup_schedule::PluginRepositoryScheduleAdapter;

#[cfg(test)]
mod shutdown_diagnostic_tests {
    use super::*;
    #[test]
    fn dirty_observation_does_not_change_shutdown_acceptance() {
        assert_eq!(shutdown_dirty_labels(false, false), ("Clean", "None"));
        assert_eq!(shutdown_dirty_labels(true, false), ("Dirty", "Present"));
        assert_eq!(shutdown_dirty_labels(true, true), ("Dirty", "Orphaned"));
        assert!(shutdown_result(&Status::Stopped, false).is_ok());
        assert!(matches!(
            shutdown_result(&Status::Stopped, true),
            Err(Error::Drain)
        ));
        assert!(matches!(
            shutdown_result(&Status::Fault(Arc::new(Error::Drain)), false),
            Err(Error::Drain)
        ));
    }
    #[test]
    fn faults_are_classified_without_formatting_private_payloads() {
        assert_eq!(shutdown_fault_label(&Error::Drain), "Drain");
        assert_eq!(
            shutdown_fault_label(&Error::Configuration("private fixture payload")),
            "Configuration"
        );
        assert_eq!(
            shutdown_fault_label(&Error::Database(Box::new(sqlx::Error::Protocol(
                "private fixture payload".into()
            )))),
            "Database"
        );
        assert_eq!(
            shutdown_fault_label(&Error::Settlement {
                primary: Some(Box::new(Error::Drain)),
                ready: None,
                close: None,
            }),
            "SettlementDrain"
        );
    }
}
