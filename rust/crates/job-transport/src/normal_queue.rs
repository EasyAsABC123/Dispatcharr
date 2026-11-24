//! Actual bounded Celery + separate DVR lanes over a complete selected16 typed registry.
//! Original Dispatcharr/Celery authors retain all application/protocol credit.
//! Starts no scheduler; missing families cannot be advertised as prepared.
pub use crate::normal_default_families::{
    prepare_defaults, prepare_stats, prepare_stats_with_inline, Error as FamilyPreparationError,
    PreparedDefaults,
};
use crate::{
    milestone_dispatch::{self, Disposition, Family, Handler, Lifecycle, Prepared, Queue},
    transport::NormalHead,
};
use dispatcharr_contracts::BoxFuture;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, Weak,
    },
    time::Duration,
};
use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore};
#[path = "normal_core_registry_policy.rs"]
mod core_registry_policy;
/// Existing global Celery lane budget, shared with the standalone Auto family.
/// This is not an additional semaphore or a capacity increase.
pub(crate) const CELERY_CONCURRENCY: usize = 6;
fn validate_family_admission(family: Family, maximum: usize, selected: bool) -> Result<(), Error> {
    let allowed = match family {
        Family::Recording => (1..=20).contains(&maximum),
        Family::AutoChannels | Family::SdGuide | Family::RegisteredPlugin => {
            maximum == CELERY_CONCURRENCY
        }
        _ => maximum == 1,
    };
    if !allowed || !selected {
        return Err(Error::Configuration);
    }
    Ok(())
}
#[derive(Debug)]
pub enum Error {
    MissingFamily(Family),
    DuplicateFamily(Family),
    Configuration,
    Transport,
    Closed,
    DrainTimeout,
    Poisoned,
}
#[derive(Clone, Debug)]
pub enum State {
    Prepared,
    Starting,
    Working(usize),
    Observed(Disposition),
    PauseRequested,
    Paused,
    Blocked(Disposition),
    Fault(Disposition),
    Stopped,
}
impl State {
    fn ready(&self) -> bool {
        matches!(self, Self::Working(_) | Self::Observed(_))
    }
}

#[derive(Clone, Debug)]
pub struct Readiness {
    pub scope: &'static str,
    pub celery: State,
    pub dvr: State,
    pub required_families: usize,
    pub prepared_families: usize,
    pub scheduler_enabled: bool,
    pub ready: bool,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum DefaultAdmission {
    Unavailable,
    Prepared,
    Running,
    Paused,
    Closed,
}

#[derive(Clone)]
pub struct DefaultQueueControl(watch::Sender<DefaultAdmission>);

impl DefaultQueueControl {
    pub(crate) fn new() -> Self {
        Self(watch::channel(DefaultAdmission::Unavailable).0)
    }

    pub(crate) fn configure(&self) -> bool {
        self.0.send_if_modified(|state| {
            if *state != DefaultAdmission::Unavailable {
                return false;
            }
            *state = DefaultAdmission::Prepared;
            true
        })
    }

    fn receiver_if_configured(&self) -> Option<watch::Receiver<DefaultAdmission>> {
        matches!(
            *self.0.borrow(),
            DefaultAdmission::Prepared | DefaultAdmission::Running | DefaultAdmission::Paused
        )
        .then(|| self.0.subscribe())
    }

    pub(crate) fn enable(&self) -> bool {
        self.0.send_if_modified(|state| {
            if *state != DefaultAdmission::Prepared {
                return false;
            }
            *state = DefaultAdmission::Running;
            true
        })
    }

    pub(crate) fn request(&self, pause: bool) -> bool {
        let mut accepted = false;
        self.0.send_if_modified(|state| {
            if !matches!(state, DefaultAdmission::Running | DefaultAdmission::Paused) {
                return false;
            }
            accepted = true;
            let next = if pause {
                DefaultAdmission::Paused
            } else {
                DefaultAdmission::Running
            };
            let changed = *state != next;
            *state = next;
            changed
        });
        accepted
    }

    fn close(&self) {
        self.0.send_replace(DefaultAdmission::Closed);
    }
}

fn admitting<T>(
    control: Option<&watch::Receiver<DefaultAdmission>>,
    action: impl FnOnce() -> T,
) -> Option<T> {
    if let Some(control) = control {
        let state = control.borrow();
        if matches!(*state, DefaultAdmission::Paused | DefaultAdmission::Closed) {
            return None;
        }
        // Keep the read guard across the synchronous admission decision. The
        // pause write and the decision to admit a new task cannot race past it.
        Some(action())
    } else {
        Some(action())
    }
}

struct FamilyOwner {
    _slot: OwnedSemaphorePermit,
    _caller: Arc<dyn Send + Sync>,
    _family: Arc<OwnedFamily>,
}
struct OwnedFamily {
    inner: Arc<dyn Handler>,
    slot: Arc<Semaphore>,
    closed: AtomicBool,
}
#[derive(Clone)]
struct FamilyAdapter(Arc<OwnedFamily>);
impl Handler for FamilyAdapter {
    fn registered_head(&self, raw: &[u8]) -> bool {
        self.0.inner.registered_head(raw)
    }

    fn maintenance(&self, owner: Arc<dyn Send + Sync>) -> BoxFuture<'_, Result<(), Disposition>> {
        self.0.inner.maintenance(owner)
    }

    fn head_advance_notifier(&self) -> Option<Arc<tokio::sync::Notify>> {
        self.0.inner.head_advance_notifier()
    }
    fn maximum_concurrency(&self) -> usize {
        self.0.inner.maximum_concurrency()
    }
    fn supports_selected_head(&self) -> bool {
        self.0.inner.supports_selected_head()
    }
    fn due_head(&self) -> Option<Arc<[u8]>> {
        self.0.inner.due_head()
    }
    fn run_due(
        &self,
        raw: Arc<[u8]>,
        caller: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
        Box::pin(async move {
            if self.0.closed.load(Ordering::Acquire) {
                return Err(Disposition::Operational("normal due intake closed".into()));
            }
            let slot = self
                .0
                .slot
                .clone()
                .try_acquire_owned()
                .map_err(|_| Disposition::Capacity)?;
            if self.0.closed.load(Ordering::Acquire) {
                return Err(Disposition::Operational("normal due intake closed".into()));
            }
            let owner: Arc<dyn Send + Sync> = Arc::new(FamilyOwner {
                _slot: slot,
                _caller: caller,
                _family: self.0.clone(),
            });
            self.0.inner.run_due(raw, owner).await
        })
    }
    fn run_selected(
        &self,
        expected: Arc<[u8]>,
        caller: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
        Box::pin(async move {
            if self.0.closed.load(Ordering::Acquire) {
                return Err(Disposition::Operational("normal DVR intake closed".into()));
            }
            let slot = self
                .0
                .slot
                .clone()
                .try_acquire_owned()
                .map_err(|_| Disposition::Capacity)?;
            if self.0.closed.load(Ordering::Acquire) {
                return Err(Disposition::Operational("normal DVR intake closed".into()));
            }
            if self.0.closed.load(Ordering::Acquire) {
                return Err(Disposition::Operational("normal due intake closed".into()));
            }
            let owner: Arc<dyn Send + Sync> = Arc::new(FamilyOwner {
                _slot: slot,
                _caller: caller,
                _family: self.0.clone(),
            });
            self.0.inner.run_selected(expected, owner).await
        })
    }

    fn run_once(
        &self,
        caller: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
        Box::pin(async move {
            if self.0.closed.load(Ordering::Acquire) {
                return Err(Disposition::Operational(
                    "normal family intake closed".into(),
                ));
            }
            let slot = self
                .0
                .slot
                .clone()
                .try_acquire_owned()
                .map_err(|_| Disposition::Capacity)?;
            if self.0.closed.load(Ordering::Acquire) {
                return Err(Disposition::Operational(
                    "normal family intake closed".into(),
                ));
            }
            if self.0.closed.load(Ordering::Acquire) {
                return Err(Disposition::Operational("normal due intake closed".into()));
            }
            let owner: Arc<dyn Send + Sync> = Arc::new(FamilyOwner {
                _slot: slot,
                _caller: caller,
                _family: self.0.clone(),
            });
            // The owner reaches the physical transport/SQL/media receipts. A logical
            // error cannot release this admission before the real downstream reply.
            self.0.inner.run_once(owner).await
        })
    }
}
impl Lifecycle for FamilyAdapter {
    fn stop_admission(&self) {
        self.0.inner.stop_admission();
    }
    fn begin_drain(&self, budget: Duration) {
        self.0.inner.begin_drain(budget);
    }
    fn shutdown(&self, timeout: Duration) -> BoxFuture<'_, Result<(), Disposition>> {
        self.0.inner.shutdown(timeout)
    }
}
struct Lane {
    prepared: Prepared,
    state: watch::Sender<State>,
    done: watch::Sender<bool>,
}
struct Inner {
    celery: Arc<Lane>,
    dvr: Arc<Lane>,
    heads: [Arc<NormalHead>; 2],
    families: BTreeMap<Family, Arc<OwnedFamily>>,
    poll: Duration,
    lifecycle: Mutex<(bool, bool)>,
    require_registered: bool,
    schedule_owner: Mutex<ScheduleOwner>,
    default_control: DefaultQueueControl,
}
#[derive(Clone, Copy, Default, PartialEq, Eq)]
enum ScheduleOwner {
    #[default]
    Unbound,
    Core,
    Taskless,
}
impl ScheduleOwner {
    fn bind(&mut self, requested: Self) -> Result<(), Error> {
        if *self != Self::Unbound && !(*self == Self::Taskless && requested == Self::Taskless) {
            return Err(Error::Configuration);
        }
        *self = requested;
        Ok(())
    }
}
struct Lifetime(watch::Sender<bool>);
impl Drop for Lifetime {
    fn drop(&mut self) {
        self.0.send_replace(true);
    }
}
#[derive(Clone)]
pub struct Application {
    inner: Arc<Inner>,
    lifetime: Arc<Lifetime>,
}

/// Capability issued by the exact complete Selected receiver before starting it.
/// This does not start a queue or grant taskless restore authority.
pub struct SelectedCoreSchedules {
    receiver: Weak<Inner>,
    stop: Weak<Lifetime>,
    backup: Arc<crate::normal_backup::Application>,
    plugin_refresh: bool,
    saved_stats: bool,
    saved_xc: Option<crate::normal_xc_live::Worker>,
    restore_registry: Option<Weak<crate::normal_registered_plugins::Prepared>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SelectedScheduleAvailability {
    Ready,
    Waiting,
    Terminal,
}

impl SelectedCoreSchedules {
    /// Bind only the actual prepared RegisteredPlugin handler on THIS receiver.
    /// This prepares read-only restore observation, never a Taskless owner mode.
    pub fn with_restore_registry(
        mut self,
        plugins: &Arc<crate::normal_registered_plugins::Prepared>,
    ) -> Result<Self, Error> {
        if self.restore_registry.is_some() {
            return Err(Error::Configuration);
        }
        let receiver = self.receiver.upgrade().ok_or(Error::Closed)?;
        let stop = self.stop.upgrade().ok_or(Error::Closed)?;
        if !receiver.require_registered {
            return Err(Error::Configuration);
        }
        let expected: Arc<dyn Handler> = plugins.clone();
        let backup: Arc<dyn Handler> = self.backup.clone();
        if !core_registry_bindings_match(
            &receiver.families,
            receiver.require_registered,
            &expected,
            &backup,
        ) {
            return Err(Error::Configuration);
        }
        let lifecycle = receiver.lifecycle.lock().map_err(|_| Error::Poisoned)?;
        if lifecycle.0 || lifecycle.1 || *stop.0.borrow() {
            return Err(Error::Closed);
        }
        if *receiver
            .schedule_owner
            .lock()
            .map_err(|_| Error::Poisoned)?
            != ScheduleOwner::Core
        {
            return Err(Error::Configuration);
        }
        self.restore_registry = Some(Arc::downgrade(plugins));
        Ok(self)
    }
    pub(super) fn restore_registry_source(&self) -> Option<CoreRegistrySource> {
        Some(CoreRegistrySource {
            receiver: self.receiver.clone(),
            stop: self.stop.clone(),
            plugins: self.restore_registry.clone()?,
            backup: Arc::downgrade(&self.backup),
        })
    }
    pub fn with_saved_xc(
        mut self,
        scanner: &crate::normal_scanner::Prepared,
    ) -> Result<Self, Error> {
        let receiver = self.receiver.upgrade().ok_or(Error::Closed)?;
        let (single, vod) = scanner.saved_xc_handlers();
        if !saved_xc_receivers_match(&receiver.families, single, vod) {
            return Err(Error::Configuration);
        }
        self.saved_xc = Some(scanner.saved_xc_worker());
        Ok(self)
    }

    pub(super) fn owns_saved_xc(&self) -> bool {
        self.saved_xc.is_some()
    }

    pub(super) async fn admits_saved_xc(
        &self,
        account: i64,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<bool, dispatcharr_store::StoreError> {
        match self.saved_xc.as_ref() {
            Some(worker) => worker.admits_saved_root(account, caller).await,
            None => Ok(false),
        }
    }
    pub(super) fn jobs(&self) -> crate::transport::Jobs {
        self.backup.scheduled_jobs()
    }

    pub(super) fn pool(&self) -> sqlx::PgPool {
        self.backup.schedule_pool().clone()
    }

    pub(super) fn identity(&self) -> dispatcharr_store::ProducerIdentitySource {
        self.backup.schedule_identity()
    }

    pub(super) fn ready(&self) -> bool {
        self.availability() == SelectedScheduleAvailability::Ready
    }

    pub(super) fn availability(&self) -> SelectedScheduleAvailability {
        let Some((receiver, stop)) = self.receiver.upgrade().zip(self.stop.upgrade()) else {
            return SelectedScheduleAvailability::Terminal;
        };
        let stopping = *stop.0.borrow();
        let celery = receiver.celery.state.borrow();
        let dvr = receiver.dvr.state.borrow();
        selected_schedule_availability(
            stopping,
            self.backup.scheduled_admission_open(),
            &celery,
            &dvr,
        )
    }

    pub(super) fn owns_plugin_refresh(&self) -> bool {
        self.plugin_refresh
    }

    pub(super) fn owns_saved_stats(&self) -> bool {
        self.saved_stats
    }

    pub(super) fn owns_xmltv_refresh(&self) -> bool {
        self.receiver.upgrade().is_some_and(|receiver| {
            [Family::Refresh, Family::ProgrammeIndex, Family::Series]
                .iter()
                .all(|family| receiver.families.contains_key(family))
        })
    }

    pub(super) fn enable_schedule(
        &self,
        data: serde_json::Value,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<serde_json::Value, dispatcharr_store::StoreError>> {
        dispatcharr_store::BackupJobApi::update_schedule(self.backup.as_ref(), data, owner)
    }
}

fn selected_schedule_availability(
    stopping: bool,
    backup_open: bool,
    celery: &State,
    dvr: &State,
) -> SelectedScheduleAvailability {
    use SelectedScheduleAvailability::{Ready, Terminal, Waiting};
    if stopping
        || !backup_open
        || [celery, dvr]
            .into_iter()
            .any(|state| matches!(state, State::Fault(_) | State::Stopped))
    {
        Terminal
    } else if (celery.ready() || matches!(celery, State::PauseRequested | State::Paused))
        && dvr.ready()
    {
        Ready
    } else {
        Waiting
    }
}

#[cfg(test)]
fn selected_schedules_ready(
    stopping: bool,
    backup_open: bool,
    celery: &State,
    dvr: &State,
) -> bool {
    selected_schedule_availability(stopping, backup_open, celery, dvr)
        == SelectedScheduleAvailability::Ready
}

fn saved_xc_receivers_match(
    families: &BTreeMap<Family, Arc<OwnedFamily>>,
    single: &Arc<dyn Handler>,
    vod: &Arc<dyn Handler>,
) -> bool {
    [(Family::M3uSingle, single), (Family::VodIngestion, vod)]
        .into_iter()
        .all(|(family, expected)| {
            families.get(&family).is_some_and(|entry| {
                Arc::ptr_eq(&entry.inner, expected) && entry.inner.supports_selected_head()
            })
        })
        && families
            .get(&Family::XcAccountProfiles)
            .is_some_and(|entry| entry.inner.supports_selected_head())
}

fn selected_schedules_match(
    families: &BTreeMap<Family, Arc<OwnedFamily>>,
    backup: &Arc<dyn Handler>,
) -> bool {
    [Family::BackupCreate, Family::BackupScheduled]
        .iter()
        .all(|family| {
            families
                .get(family)
                .is_some_and(|actual| Arc::ptr_eq(&actual.inner, backup))
        })
        && [
            Family::BackendCleanup,
            Family::AccountExpiration,
            Family::VersionCheck,
        ]
        .iter()
        .all(|family| families.contains_key(family))
        && families
            .get(&Family::Recording)
            .is_some_and(|recording| recording.inner.supports_selected_head())
}

/// Shared Core Celery receiver for backup requests and prepared periodic families.
#[derive(Clone)]
pub struct CoreScheduledWorker {
    lane: Arc<Lane>,
    head: Arc<NormalHead>,
    families: Arc<BTreeMap<Family, FamilyAdapter>>,
    backup: Arc<crate::normal_backup::Application>,
    plugin_refresh: Option<crate::normal_plugin_repos::Prepared>,
    lifecycle: Arc<Mutex<(bool, bool)>>,
    stop: Arc<Lifetime>,
    poll: Duration,
}
impl CoreScheduledWorker {
    pub fn prepare(
        backup: Arc<crate::normal_backup::Application>,
        store: &dispatcharr_store::Store,
        events: Arc<dyn dispatcharr_contracts::UpdateEventSink>,
        version_options: crate::version_service::Options,
        plugin_refresh: Option<(
            dispatcharr_store::AuthService,
            crate::normal_revocation::Revoked,
        )>,
        rehash: Option<(
            dispatcharr_store::AuthService,
            crate::normal_revocation::Revoked,
        )>,
        poll: Duration,
    ) -> Result<Self, Error> {
        let jobs = backup.scheduled_jobs();
        if !jobs.allows_defaults()
            || !(Duration::from_millis(25)..=Duration::from_secs(5)).contains(&poll)
        {
            return Err(Error::Configuration);
        }
        let cleanup = crate::normal_default_families::prepare_cleanup(jobs.clone(), store)
            .map_err(|_| Error::Configuration)?;
        let expiration = crate::normal_default_families::prepare_account_expiration(
            jobs.clone(),
            store,
            events.clone(),
        )
        .map_err(|_| Error::Configuration)?;
        let version = crate::version_application::prepare_shared_handler(
            store,
            events.clone(),
            jobs.clone(),
            version_options,
        )
        .map_err(|_| Error::Configuration)?;
        let actual: Arc<dyn Handler> = backup.clone();
        let plugin_refresh = plugin_refresh
            .map(|(auth, revoked)| {
                crate::normal_plugin_repos::Prepared::new(jobs.clone(), auth, revoked)
            })
            .transpose()
            .map_err(|_| Error::Configuration)?;
        let rehash = rehash
            .map(|(auth, revoked)| {
                crate::normal_core_tasks::prepare_provider_rehash(
                    jobs.clone(),
                    store,
                    auth,
                    events,
                    revoked,
                )
            })
            .transpose()
            .map_err(|_| Error::Configuration)?;
        let entries = [
            (Family::BackupCreate, actual.clone()),
            (Family::BackupScheduled, actual),
            (Family::BackendCleanup, cleanup),
            (Family::AccountExpiration, expiration),
            (Family::VersionCheck, version),
        ];
        let mut families = BTreeMap::new();
        for (family, inner) in entries
            .into_iter()
            .chain(plugin_refresh.as_ref().map(|prepared| prepared.entry()))
            .chain(rehash.map(|handler| (Family::RehashStreams, handler)))
        {
            let maximum = inner.maximum_concurrency();
            validate_family_admission(family, maximum, inner.supports_selected_head())?;
            families.insert(
                family,
                FamilyAdapter(Arc::new(OwnedFamily {
                    inner,
                    slot: Arc::new(Semaphore::new(maximum)),
                    closed: AtomicBool::new(false),
                })),
            );
        }
        let head = Arc::new(NormalHead::core_scheduled(jobs));
        let handlers = families
            .iter()
            .map(|(family, adapter)| (*family, Arc::new(adapter.clone()) as Arc<dyn Handler>))
            .collect();
        let prepared =
            Prepared::core_scheduled(head.clone(), handlers).map_err(|_| Error::Configuration)?;
        Ok(Self {
            lane: Arc::new(Lane {
                prepared,
                state: watch::channel(State::Prepared).0,
                done: watch::channel(true).0,
            }),
            head,
            families: Arc::new(families),
            backup,
            plugin_refresh,
            lifecycle: Arc::new(Mutex::new((false, false))),
            stop: Arc::new(Lifetime(watch::channel(false).0)),
            poll,
        })
    }

    pub(crate) fn owns_plugin_refresh(&self) -> bool {
        self.families.contains_key(&Family::PluginRefresh)
    }

    pub(crate) fn jobs(&self) -> crate::transport::Jobs {
        self.backup.scheduled_jobs()
    }

    pub fn backup_create_api(&self) -> Arc<dyn dispatcharr_store::BackupJobApi> {
        self.backup.clone()
    }

    pub fn bind_restore_plugins(
        &self,
        plugins: dispatcharr_store::PluginActions,
    ) -> Result<(), super::normal_backup::Error> {
        self.backup.bind_restore_plugins(plugins)
    }

    pub fn stop_backup_admission(&self) {
        self.backup.close_restore_claim_admission();
        self.backup.close_http_admission();
    }

    pub(crate) fn ready(&self) -> bool {
        self.schedule_availability() == SelectedScheduleAvailability::Ready
    }

    pub(super) fn schedule_availability(&self) -> SelectedScheduleAvailability {
        let state = self.lane.state.borrow();
        if *self.stop.0.borrow() || matches!(*state, State::Fault(_) | State::Stopped) {
            SelectedScheduleAvailability::Terminal
        } else if state.ready() {
            SelectedScheduleAvailability::Ready
        } else {
            SelectedScheduleAvailability::Waiting
        }
    }

    pub(super) fn enable_schedule(
        &self,
        data: serde_json::Value,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<serde_json::Value, dispatcharr_store::StoreError>> {
        dispatcharr_store::BackupJobApi::update_schedule(self.backup.as_ref(), data, owner)
    }

    pub async fn start(
        &self,
        owner: Arc<dyn Send + Sync>,
        plugin_startup: bool,
    ) -> Result<(), Error> {
        let plugin = if plugin_startup {
            Some(self.plugin_refresh.as_ref().ok_or(Error::Configuration)?)
        } else {
            None
        };
        {
            let mut lifecycle = self.lifecycle.lock().map_err(|_| Error::Poisoned)?;
            if lifecycle.0 || lifecycle.1 {
                return Err(Error::Closed);
            }
            lifecycle.0 = true;
        }
        // Source publishes at process startup independently of its worker.
        // Attempt it before this lane can hold the plugin family's permit.
        if let Some(plugin) = plugin {
            if plugin.publish_startup(owner.clone()).await.is_err() {
                eprintln!("Could not enqueue startup plugin repository refresh");
            }
        }
        let lifecycle = self.lifecycle.lock().map_err(|_| Error::Poisoned)?;
        if lifecycle.1 {
            return Err(Error::Closed);
        }
        self.lane.state.send_replace(State::Starting);
        self.lane.done.send_replace(false);
        tokio::spawn(celery_lane(
            self.lane.clone(),
            self.head.clone(),
            self.families.as_ref().clone(),
            owner,
            self.stop.0.subscribe(),
            self.poll,
            None,
        ));
        Ok(())
    }

    pub async fn await_ready(&self, budget: Duration) -> Result<(), Error> {
        let mut state = self.lane.state.subscribe();
        tokio::time::timeout(budget, async {
            loop {
                let observed = state.borrow_and_update().clone();
                if observed.ready() && !*self.stop.0.borrow() {
                    return Ok(());
                }
                if matches!(observed, State::Fault(_) | State::Stopped) || *self.stop.0.borrow() {
                    return Err(Error::Closed);
                }
                state.changed().await.map_err(|_| Error::Closed)?;
            }
        })
        .await
        .map_err(|_| Error::Transport)?
    }

    /// Stop API/scheduler intake and await scheduler publications before
    /// calling this method. It then settles queue callbacks and shared ports.
    pub async fn shutdown(&self, budget: Duration) -> Result<(), Error> {
        let deadline = tokio::time::Instant::now() + budget;
        self.retire_callbacks(deadline).await?;
        self.head
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
            .map_err(|_| Error::Transport)?;
        self.retired_callback_status()
    }

    /// Retire worker callbacks while an independently unsettled publisher
    /// retains the shared transport.
    pub async fn drain(&self, budget: Duration) -> Result<(), Error> {
        self.retire_callbacks(tokio::time::Instant::now() + budget)
            .await?;
        self.retired_callback_status()
    }

    async fn retire_callbacks(&self, deadline: tokio::time::Instant) -> Result<(), Error> {
        if let Ok(mut lifecycle) = self.lifecycle.lock() {
            lifecycle.1 = true;
        }
        self.stop.0.send_replace(true);
        for handler in self.families.values() {
            handler.stop_admission();
        }
        let mut done = self.lane.done.subscribe();
        tokio::time::timeout_at(deadline, async {
            while !*done.borrow_and_update() {
                done.changed().await.map_err(|_| Error::Closed)?;
            }
            Ok(())
        })
        .await
        .map_err(|_| Error::DrainTimeout)??;
        let mut held = Vec::with_capacity(self.families.len());
        for handler in self.families.values() {
            handler.0.closed.store(true, Ordering::Release);
            let slot = tokio::time::timeout_at(
                deadline,
                handler
                    .0
                    .slot
                    .clone()
                    .acquire_many_owned(handler.maximum_concurrency() as u32),
            )
            .await
            .map_err(|_| Error::DrainTimeout)?
            .map_err(|_| Error::Closed)?;
            held.push(slot);
        }
        let mut clean = true;
        for handler in self.families.values() {
            clean &= handler
                .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
                .await
                .is_ok();
        }
        if !clean {
            return Err(Error::DrainTimeout);
        }
        drop(held);
        Ok(())
    }

    fn retired_callback_status(&self) -> Result<(), Error> {
        if matches!(&*self.lane.state.borrow(), State::Fault(_)) {
            return Err(Error::Transport);
        }
        Ok(())
    }
}

fn core_registry_bindings_match(
    families: &BTreeMap<Family, Arc<OwnedFamily>>,
    registered: bool,
    plugin: &Arc<dyn Handler>,
    backup: &Arc<dyn Handler>,
) -> bool {
    registered
        && [
            (Family::RegisteredPlugin, plugin, CELERY_CONCURRENCY),
            (Family::BackupCreate, backup, 1),
            (Family::BackupScheduled, backup, 1),
        ]
        .iter()
        .all(|(family, expected, maximum)| {
            families.get(family).is_some_and(|actual| {
                Arc::ptr_eq(&actual.inner, expected)
                    && actual.inner.supports_selected_head()
                    && actual.inner.maximum_concurrency() == *maximum
                    && !actual.closed.load(Ordering::Acquire)
            })
        })
}
fn core_registry_members(
    families: &BTreeMap<Family, Arc<OwnedFamily>>,
    registered: bool,
    plugin: &Arc<dyn Handler>,
    backup: &Arc<dyn Handler>,
    rows: &[serde_json::Value],
) -> Option<BTreeMap<Family, Arc<OwnedFamily>>> {
    if rows.len() > 256 || !core_registry_bindings_match(families, registered, plugin, backup) {
        return None;
    }
    let mut selected = BTreeMap::new();
    let mut ids = BTreeSet::new();
    for row in rows {
        let id = row["id"].as_i64().filter(|id| *id > 0)?;
        if !ids.insert(id) || !row["enabled"].is_boolean() {
            return None;
        }
        let family = milestone_dispatch::builtin_family(row["task"].as_str()?)?;
        let actual = families.get(&family)?;
        if actual.closed.load(Ordering::Acquire) || !actual.inner.supports_selected_head() {
            return None;
        }
        selected.insert(family, actual.clone());
    }
    Some(selected)
}

/// Read-only source for one existing Core registry. No public constructor,
/// serialization, task allowlist input, or execution method is provided.
#[derive(Clone)]
pub(crate) struct CoreRegistrySource {
    receiver: Weak<Inner>,
    stop: Weak<Lifetime>,
    plugins: Weak<crate::normal_registered_plugins::Prepared>,
    backup: Weak<crate::normal_backup::Application>,
}
pub(crate) struct CoreRegistryLease {
    source: CoreRegistrySource,
    receiver: Arc<Inner>,
    plugins: Arc<crate::normal_registered_plugins::Prepared>,
    backup: Arc<crate::normal_backup::Application>,
    families: BTreeMap<Family, Arc<OwnedFamily>>,
    rows: usize,
}
struct CoreRegistryOwners {
    receiver: Arc<Inner>,
    plugins: Arc<crate::normal_registered_plugins::Prepared>,
    backup: Arc<crate::normal_backup::Application>,
}
impl CoreRegistrySource {
    fn resolve(&self) -> Option<CoreRegistryOwners> {
        let receiver = self.receiver.upgrade()?;
        let plugins = self.plugins.upgrade()?;
        let backup = self.backup.upgrade()?;
        let stop = self.stop.upgrade()?;
        let lifecycle = receiver.lifecycle.lock().ok()?;
        let core = *receiver.schedule_owner.lock().ok()? == ScheduleOwner::Core;
        if !core_registry_policy::live_receiver(lifecycle.0, lifecycle.1, *stop.0.borrow(), core)
            || !receiver.require_registered
            || !backup.scheduled_admission_open()
        {
            return None;
        }
        let jobs = backup.scheduled_jobs();
        if !jobs.allows_defaults()
            || receiver
                .heads
                .iter()
                .any(|head| !head.uses_configured_ports(&jobs))
        {
            return None;
        }
        let plugin: Arc<dyn Handler> = plugins.clone();
        let archive: Arc<dyn Handler> = backup.clone();
        if !core_registry_bindings_match(
            &receiver.families,
            receiver.require_registered,
            &plugin,
            &archive,
        ) {
            return None;
        }
        drop(lifecycle);
        Some(CoreRegistryOwners {
            receiver,
            plugins,
            backup,
        })
    }
    pub(super) fn available(&self) -> bool {
        self.resolve().is_some()
    }
    pub(super) async fn lease(
        &self,
        reservation: &dispatcharr_store::TasklessRestoreReservation,
    ) -> Result<Option<CoreRegistryLease>, dispatcharr_store::StoreError> {
        let Some(CoreRegistryOwners {
            receiver,
            plugins,
            backup,
        }) = self.resolve()
        else {
            return Ok(None);
        };
        if !reservation.belongs_to_runtime(plugins.runtime())
            || !backup.owns_core_restore_reservation(reservation)
        {
            return Ok(None);
        }
        // Data origin is this real retained SQL session, never caller rows.
        let observed = reservation.observe_registry().await?;
        reservation.revalidate().await?;
        let rows = &observed.periodic;
        if rows.len() > 256 {
            return Ok(None);
        }
        let plugin: Arc<dyn Handler> = plugins.clone();
        let archive: Arc<dyn Handler> = backup.clone();
        let Some(families) = core_registry_members(
            &receiver.families,
            receiver.require_registered,
            &plugin,
            &archive,
            rows,
        ) else {
            return Ok(None);
        };
        let lease = CoreRegistryLease {
            source: self.clone(),
            receiver,
            plugins,
            backup,
            families,
            rows: rows.len(),
        };
        Ok(lease.validate(reservation).then_some(lease))
    }
}
impl CoreRegistryLease {
    pub(super) fn validate(
        &self,
        reservation: &dispatcharr_store::TasklessRestoreReservation,
    ) -> bool {
        if !core_registry_policy::same_generation(&self.source.receiver, &self.receiver)
            || !core_registry_policy::same_generation(&self.source.plugins, &self.plugins)
            || !core_registry_policy::same_generation(&self.source.backup, &self.backup)
            || !reservation.belongs_to_runtime(self.plugins.runtime())
            || !self.backup.owns_core_restore_reservation(reservation)
            || self.source.resolve().is_none()
        {
            return false;
        }
        self.families.iter().all(|(family, held)| {
            self.receiver.families.get(family).is_some_and(|actual| {
                Arc::ptr_eq(actual, held)
                    && !actual.closed.load(Ordering::Acquire)
                    && actual.inner.supports_selected_head()
            })
        })
    }
    pub(super) fn summary(&self) -> (usize, usize) {
        (self.rows, self.families.len())
    }
}

/// Read-only witness for the observed taskless restore periodic domain.
#[derive(Clone)]
pub struct TasklessBuiltins {
    selected: Weak<Inner>,
    plugins: Weak<crate::normal_registered_plugins::Prepared>,
    backup: Weak<crate::normal_backup::Application>,
}

fn taskless_builtin_family(task: &str) -> Option<Family> {
    let family = milestone_dispatch::builtin_family(task)?;
    // These are the three prepared builtin families observed in the ordinary
    // disabled periodic inventory of the trusted taskless Source reference.
    matches!(
        family,
        Family::M3uSingle | Family::BackupScheduled | Family::PluginRefresh
    )
    .then_some(family)
}

impl TasklessBuiltins {
    pub(crate) fn belongs_to_backup(&self, backup: &crate::normal_backup::Application) -> bool {
        self.backup
            .upgrade()
            .is_some_and(|bound| bound.same_instance(backup))
    }

    fn selected_owner(
        &self,
        reservation: &dispatcharr_store::TasklessRestoreReservation,
    ) -> Result<Arc<Inner>, Error> {
        let selected = self.selected.upgrade().ok_or(Error::Closed)?;
        let plugins = self.plugins.upgrade().ok_or(Error::Closed)?;
        if !reservation.belongs_to_runtime(plugins.runtime()) {
            return Err(Error::Configuration);
        }
        Ok(selected)
    }

    pub(crate) fn matches_owner(
        &self,
        reservation: &dispatcharr_store::TasklessRestoreReservation,
    ) -> bool {
        self.selected_owner(reservation).is_ok()
    }

    pub(crate) fn qualify(
        &self,
        reservation: &dispatcharr_store::TasklessRestoreReservation,
        rows: &[serde_json::Value],
    ) -> Result<BTreeSet<String>, Error> {
        let selected = self.selected_owner(reservation)?;
        if rows.len() > 256 {
            return Err(Error::Configuration);
        }
        rows.iter()
            .map(|row| {
                if row["enabled"] != false {
                    return Err(Error::Configuration);
                }
                let task = row["task"].as_str().ok_or(Error::Configuration)?;
                let family = taskless_builtin_family(task).ok_or(Error::Configuration)?;
                let actual = selected
                    .families
                    .get(&family)
                    .ok_or(Error::MissingFamily(family))?;
                if !actual.inner.supports_selected_head() || actual.closed.load(Ordering::Acquire) {
                    return Err(Error::Closed);
                }
                Ok(task.to_owned())
            })
            .collect()
    }
}

#[derive(Debug)]
pub struct Drain {
    pub pollers_settled: bool,
    pub admissions_settled: bool,
    pub families: Vec<(Family, Result<(), Disposition>)>,
    pub heads_settled: bool,
}
impl Drain {
    pub fn complete(&self) -> bool {
        self.pollers_settled
            && self.admissions_settled
            && self.heads_settled
            && self.families.iter().all(|(_, value)| value.is_ok())
    }
}
impl Application {
    pub(crate) fn contains_actual_handler(
        &self,
        family: Family,
        expected: &Arc<dyn Handler>,
    ) -> bool {
        self.inner
            .families
            .get(&family)
            .is_some_and(|prepared| Arc::ptr_eq(&prepared.inner, expected))
    }
    /// Bind one scoped scheduler to this already prepared Selected receiver.
    /// Call before starting either lifecycle. The caller must replace any other Beat.
    pub fn core_schedule_receiver(
        &self,
        backup: &Arc<crate::normal_backup::Application>,
        stats: &Arc<dyn Handler>,
    ) -> Result<SelectedCoreSchedules, Error> {
        let handler: Arc<dyn Handler> = backup.clone();
        let jobs = backup.scheduled_jobs();
        if !jobs.allows_defaults()
            || !backup.scheduled_admission_open()
            || !selected_schedules_match(&self.inner.families, &handler)
            || !self.contains_actual_handler(Family::ProxyStats, stats)
            || !stats.supports_selected_head()
            || self
                .inner
                .heads
                .iter()
                .any(|head| !head.uses_configured_ports(&jobs))
        {
            return Err(Error::Configuration);
        }
        let lifecycle = self.inner.lifecycle.lock().map_err(|_| Error::Poisoned)?;
        if lifecycle.0 || lifecycle.1 || *self.lifetime.0.borrow() {
            return Err(Error::Closed);
        }
        self.inner
            .schedule_owner
            .lock()
            .map_err(|_| Error::Poisoned)?
            .bind(ScheduleOwner::Core)?;
        Ok(SelectedCoreSchedules {
            receiver: Arc::downgrade(&self.inner),
            stop: Arc::downgrade(&self.lifetime),
            backup: backup.clone(),
            plugin_refresh: self.inner.families.contains_key(&Family::PluginRefresh),
            saved_stats: true,
            saved_xc: None,
            restore_registry: None,
        })
    }

    /// Only actual Selected family pointers may bind this read-only authority.
    pub fn taskless_builtin_authority(
        &self,
        plugins: &Arc<crate::normal_registered_plugins::Prepared>,
        backup: &Arc<crate::normal_backup::Application>,
    ) -> Result<TasklessBuiltins, Error> {
        let plugin_handler: Arc<dyn Handler> = plugins.clone();
        let backup_handler: Arc<dyn Handler> = backup.clone();
        if !self.registered_plugin_prepared()
            || !self.contains_actual_handler(Family::RegisteredPlugin, &plugin_handler)
            || !self.contains_actual_handler(Family::BackupCreate, &backup_handler)
            || !self.contains_actual_handler(Family::BackupScheduled, &backup_handler)
            || [Family::M3uSingle, Family::PluginRefresh]
                .iter()
                .any(|family| {
                    self.inner
                        .families
                        .get(family)
                        .is_none_or(|entry| !entry.inner.supports_selected_head())
                })
        {
            return Err(Error::Configuration);
        }
        self.inner
            .schedule_owner
            .lock()
            .map_err(|_| Error::Poisoned)?
            .bind(ScheduleOwner::Taskless)?;
        Ok(TasklessBuiltins {
            selected: Arc::downgrade(&self.inner),
            plugins: Arc::downgrade(plugins),
            backup: Arc::downgrade(backup),
        })
    }

    pub async fn prepare(
        broker: &str,
        backend: &str,
        entries: Vec<(Family, Arc<dyn Handler>)>,
        poll: Duration,
    ) -> Result<Self, Error> {
        Self::prepare_inner(broker, backend, entries, poll, false).await
    }
    pub async fn prepare_with_registered_plugins(
        broker: &str,
        backend: &str,
        entries: Vec<(Family, Arc<dyn Handler>)>,
        poll: Duration,
    ) -> Result<Self, Error> {
        Self::prepare_inner(broker, backend, entries, poll, true).await
    }
    async fn prepare_inner(
        broker: &str,
        backend: &str,
        entries: Vec<(Family, Arc<dyn Handler>)>,
        poll: Duration,
        require_registered: bool,
    ) -> Result<Self, Error> {
        if !(Duration::from_millis(25)..=Duration::from_secs(5)).contains(&poll) {
            return Err(Error::Configuration);
        }
        let mut seen = BTreeSet::new();
        for (family, _) in &entries {
            if !seen.insert(*family) {
                return Err(Error::DuplicateFamily(*family));
            }
        }
        for family in milestone_dispatch::REQUIRED
            .into_iter()
            .chain([Family::BackupCreate, Family::BackupScheduled])
        {
            if !seen.contains(&family) {
                return Err(Error::MissingFamily(family));
            }
        }
        if require_registered && !seen.contains(&Family::RegisteredPlugin) {
            return Err(Error::MissingFamily(Family::RegisteredPlugin));
        }
        let mut families = BTreeMap::new();
        for (family, inner) in entries {
            let maximum = inner.maximum_concurrency();
            validate_family_admission(family, maximum, inner.supports_selected_head())?;
            families.insert(
                family,
                Arc::new(OwnedFamily {
                    inner,
                    slot: Arc::new(Semaphore::new(maximum)),
                    closed: AtomicBool::new(false),
                }),
            );
        }
        let celery = Arc::new(
            NormalHead::connect(broker, backend, Queue::Celery)
                .await
                .map_err(|_| Error::Transport)?,
        );
        let dvr = match NormalHead::connect(broker, backend, Queue::Dvr).await {
            Ok(head) => Arc::new(head),
            Err(_) => {
                let _ = celery.shutdown(Duration::from_secs(4)).await;
                return Err(Error::Transport);
            }
        };
        let adapters = || {
            families
                .iter()
                .map(|(family, inner)| {
                    (
                        *family,
                        Arc::new(FamilyAdapter(inner.clone())) as Arc<dyn Handler>,
                    )
                })
                .collect::<Vec<_>>()
        };
        let celery_prepared =
            Prepared::new(celery.clone(), adapters()).map_err(|_| Error::Configuration)?;
        let dvr_prepared =
            Prepared::new(dvr.clone(), adapters()).map_err(|_| Error::Configuration)?;
        Ok(Self {
            inner: Arc::new(Inner {
                celery: Arc::new(Lane {
                    prepared: celery_prepared,
                    state: watch::channel(State::Prepared).0,
                    done: watch::channel(true).0,
                }),
                dvr: Arc::new(Lane {
                    prepared: dvr_prepared,
                    state: watch::channel(State::Prepared).0,
                    done: watch::channel(true).0,
                }),
                heads: [celery, dvr],
                require_registered,
                families,
                poll,
                lifecycle: Mutex::new((false, false)),
                schedule_owner: Mutex::new(ScheduleOwner::Unbound),
                default_control: DefaultQueueControl::new(),
            }),
            lifetime: Arc::new(Lifetime(watch::channel(false).0)),
        })
    }
    pub fn default_queue_control(&self) -> DefaultQueueControl {
        self.inner.default_control.clone()
    }

    pub fn registered_plugin_prepared(&self) -> bool {
        self.inner.require_registered
            && self
                .inner
                .families
                .get(&Family::RegisteredPlugin)
                .is_some_and(|family| {
                    family.inner.maximum_concurrency() == CELERY_CONCURRENCY
                        && family.inner.supports_selected_head()
                })
    }
    pub fn readiness(&self) -> Readiness {
        let celery = self.inner.celery.state.borrow().clone();
        let dvr = self.inner.dvr.state.borrow().clone();
        Readiness {
            scope: "selected16-migration-families; not all Source tasks",
            ready: celery.ready() && dvr.ready(),
            celery,
            dvr,
            required_families: milestone_dispatch::REQUIRED.len()
                + 2
                + usize::from(self.inner.require_registered),
            prepared_families: self.inner.celery.prepared.prepared_families(),
            scheduler_enabled: false,
        }
    }
    pub fn start(&self, owner: Arc<dyn Send + Sync>) -> Result<(), Error> {
        let mut lifecycle = self.inner.lifecycle.lock().map_err(|_| Error::Poisoned)?;
        if lifecycle.0 || lifecycle.1 {
            return Err(Error::Closed);
        }
        lifecycle.0 = true;
        {
            let lane = self.inner.celery.clone();
            lane.done.send_replace(false);
            lane.state.send_replace(State::Starting);
            let handlers = self
                .inner
                .families
                .iter()
                .filter(|(family, _)| **family != Family::Recording)
                .map(|(family, value)| (*family, FamilyAdapter(value.clone())))
                .collect();
            tokio::spawn(celery_lane(
                lane,
                self.inner.heads[0].clone(),
                handlers,
                owner.clone(),
                self.lifetime.0.subscribe(),
                self.inner.poll,
                self.inner.default_control.receiver_if_configured(),
            ));
        }
        let lane = self.inner.dvr.clone();
        lane.done.send_replace(false);
        lane.state.send_replace(State::Starting);
        let Some(family) = self.inner.families.get(&Family::Recording).cloned() else {
            return Err(Error::MissingFamily(Family::Recording));
        };
        let head = self.inner.heads[1].clone();
        let stop = self.lifetime.0.subscribe();
        let poll = self.inner.poll;
        tokio::spawn(dvr_lane(
            lane,
            head,
            FamilyAdapter(family),
            owner,
            stop,
            poll,
        ));
        Ok(())
    }
    /// Stop only intake immediately; callers can then settle producer startup
    /// before the full worker/physical-port drain. Existing tasks retain owners.
    pub fn stop_admission(&self) {
        self.inner.default_control.close();
        self.lifetime.0.send_replace(true);
    }
    /// Caller stops scheduler/API producers first. Do not close even one shared
    /// family port until both pollers and every actual downstream owner settle.
    pub async fn shutdown(&self, timeout: Duration) -> Drain {
        let deadline = tokio::time::Instant::now() + timeout;
        if let Ok(mut lifecycle) = self.inner.lifecycle.lock() {
            lifecycle.1 = true;
        }
        self.stop_admission();
        for family in self.inner.families.values() {
            family.inner.stop_admission();
            family
                .inner
                .begin_drain(deadline.saturating_duration_since(tokio::time::Instant::now()));
        }
        let wait = async {
            for lane in [&self.inner.celery, &self.inner.dvr] {
                let mut done = lane.done.subscribe();
                loop {
                    if *done.borrow_and_update() {
                        break;
                    }
                    if done.changed().await.is_err() {
                        return false;
                    }
                }
            }
            true
        };
        let pollers_settled = tokio::time::timeout_at(deadline, wait)
            .await
            .unwrap_or(false);
        if !pollers_settled {
            return Drain {
                pollers_settled: false,
                admissions_settled: false,
                families: vec![],
                heads_settled: false,
            };
        }
        for family in self.inner.families.values() {
            family.closed.store(true, Ordering::Release);
        }
        let quiet = async {
            let mut held = Vec::new();
            for family in self.inner.families.values() {
                let Ok(permit) = family
                    .slot
                    .clone()
                    .acquire_many_owned(family.inner.maximum_concurrency() as u32)
                    .await
                else {
                    return false;
                };
                held.push(permit);
            }
            true
        };
        let admissions_settled = tokio::time::timeout_at(deadline, quiet)
            .await
            .unwrap_or(false);
        if !admissions_settled {
            return Drain {
                pollers_settled: true,
                admissions_settled: false,
                families: vec![],
                heads_settled: false,
            };
        }
        let mut results = Vec::new();
        for (family, handler) in &self.inner.families {
            results.push((
                *family,
                handler
                    .inner
                    .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
                    .await,
            ));
        }
        let mut heads_settled = true;
        for head in &self.inner.heads {
            if head
                .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
                .await
                .is_err()
            {
                heads_settled = false;
            }
        }
        Drain {
            pollers_settled,
            admissions_settled,
            families: results,
            heads_settled,
        }
    }
}

/// Bounded same-unclaimed-head cooldown. A completed handler is not proof of
/// a claim: ETA/capacity/domain refusals must throttle the Redis peek itself.
struct HeadBackoff {
    until: tokio::time::Instant,
    raw: BTreeMap<Arc<[u8]>, tokio::time::Instant>,
    maximum: usize,
}
impl HeadBackoff {
    fn new(maximum: usize) -> Self {
        Self {
            until: tokio::time::Instant::now(),
            raw: BTreeMap::new(),
            maximum,
        }
    }
    fn can_peek(&self) -> bool {
        tokio::time::Instant::now() >= self.until
    }
    fn allow(&mut self, raw: &Arc<[u8]>) -> bool {
        let now = tokio::time::Instant::now();
        self.raw.retain(|_, until| *until > now);
        if let Some(until) = self.raw.get(raw) {
            self.until = *until;
            false
        } else {
            true
        }
    }
    fn outcome(
        &mut self,
        raw: Arc<[u8]>,
        result: &Result<Disposition, Disposition>,
        poll: Duration,
    ) {
        let value = match result {
            Ok(value) | Err(value) => value,
        };
        if matches!(
            value,
            Disposition::Empty
                | Disposition::NotDue
                | Disposition::Capacity
                | Disposition::HeadChanged
                | Disposition::UnsupportedHead
                | Disposition::UnsupportedDomain
        ) {
            let now = tokio::time::Instant::now();
            let until = now + poll;
            self.raw.retain(|_, until| *until > now);
            if self.raw.len() >= self.maximum && !self.raw.contains_key(&raw) {
                if let Some(oldest) = self
                    .raw
                    .iter()
                    .min_by_key(|(_, until)| **until)
                    .map(|(raw, _)| raw.clone())
                {
                    self.raw.remove(&oldest);
                }
            }
            self.raw.insert(raw, until);
            // A real changed-head observation permits one immediate fresh peek;
            // an identical reappearing raw remains cooled by allow().
            self.until = if matches!(value, Disposition::HeadChanged) {
                now
            } else {
                until
            };
        } else {
            self.raw.remove(&raw);
            self.wake();
        }
    }
    fn wake(&mut self) {
        self.until = tokio::time::Instant::now();
    }
    fn deadline(&self, poll: Duration) -> tokio::time::Instant {
        let now = tokio::time::Instant::now();
        if self.until > now {
            self.until
        } else {
            now + poll
        }
    }
}

async fn dvr_lane(
    lane: Arc<Lane>,
    head: Arc<dyn crate::milestone_dispatch::Head>,
    handler: FamilyAdapter,
    caller: Arc<dyn Send + Sync>,
    mut stop: watch::Receiver<bool>,
    poll: Duration,
) {
    let maximum = handler.maximum_concurrency();
    let advanced = handler.head_advance_notifier();
    let mut tasks = tokio::task::JoinSet::new();
    let mut pending = BTreeSet::<Arc<[u8]>>::new();
    let mut fault = None;
    let mut backoff = HeadBackoff::new(maximum);
    loop {
        if *stop.borrow() && tasks.is_empty() {
            break;
        }
        if fault.is_some() && tasks.is_empty() {
            break;
        }
        if !*stop.borrow() && fault.is_none() && tasks.len() < maximum && backoff.can_peek() {
            match head.peek(caller.clone()).await {
                Ok(Some(raw)) => {
                    if milestone_dispatch::classify(&raw) != Some(Family::Recording) {
                        lane.state
                            .send_replace(State::Blocked(Disposition::UnsupportedHead));
                    } else {
                        let expected: Arc<[u8]> = Arc::from(raw);
                        if backoff.allow(&expected) && !pending.contains(&expected) {
                            pending.insert(expected.clone());
                            let selected = expected.clone();
                            let owned = handler.clone();
                            let owner = caller.clone();
                            tasks.spawn(async move {
                                let result = owned.run_selected(selected.clone(), owner).await;
                                (selected, result)
                            });
                            lane.state.send_replace(State::Working(tasks.len()));
                        }
                    }
                }
                Ok(None) => {
                    if tasks.is_empty() {
                        lane.state.send_replace(State::Observed(Disposition::Empty));
                    }
                }
                Err(error @ Disposition::UnsupportedHead)
                | Err(error @ Disposition::UnsupportedDomain) => {
                    lane.state.send_replace(State::Blocked(error));
                }
                Err(error) => {
                    fault = Some(error.clone());
                    lane.state.send_replace(State::Fault(error));
                }
            }
        }
        tokio::select! {
              result=tasks.join_next(),if !tasks.is_empty()=>{
               match result{
                Some(Ok((raw,result)))=>{pending.remove(&raw);backoff.outcome(raw.clone(),&result,poll);match result{Ok(value@Disposition::UnsupportedHead)|Ok(value@Disposition::UnsupportedDomain)=>{if fault.is_none(){lane.state.send_replace(State::Blocked(value));}},Ok(value)=>{if fault.is_none(){lane.state.send_replace(State::Observed(value));}},Err(error@Disposition::UnsupportedHead)|Err(error@Disposition::UnsupportedDomain)=>{if fault.is_none(){lane.state.send_replace(State::Blocked(error));}},Err(error@Disposition::Capacity)|Err(error@Disposition::HeadChanged)|Err(error@Disposition::NotDue)=>{if fault.is_none(){lane.state.send_replace(State::Observed(error));}},Err(error)=>{fault=Some(error.clone());lane.state.send_replace(State::Fault(error));}}},
                Some(Err(_))=>{let error=Disposition::Operational("normal DVR task supervisor failure".into());fault=Some(error.clone());lane.state.send_replace(State::Fault(error));},None=>{}
               }
              },
              _=stop.changed(),if !*stop.borrow()=>{},
              _=async{match &advanced{Some(notify)=>notify.notified().await,None=>std::future::pending::<()>().await}}=>{backoff.wake();},
        _=tokio::time::sleep_until(backoff.deadline(poll))=>{}
             }
    }
    if fault.is_none() {
        lane.state.send_replace(State::Stopped);
    }
    lane.done.send_replace(true);
}

async fn celery_lane(
    lane: Arc<Lane>,
    head: Arc<dyn crate::milestone_dispatch::Head>,
    handlers: BTreeMap<Family, FamilyAdapter>,
    caller: Arc<dyn Send + Sync>,
    mut stop: watch::Receiver<bool>,
    poll: Duration,
    mut default_control: Option<watch::Receiver<DefaultAdmission>>,
) {
    let maximum = CELERY_CONCURRENCY;
    let advanced: Option<Arc<tokio::sync::Notify>> = None;
    let mut tasks = tokio::task::JoinSet::new();
    let mut pending = BTreeSet::<Arc<[u8]>>::new();
    let mut fault = None;
    let mut backoff = HeadBackoff::new(maximum);
    loop {
        if *stop.borrow() && tasks.is_empty() {
            break;
        }
        if fault.is_some() && tasks.is_empty() {
            break;
        }
        let requested = default_control
            .as_mut()
            .map(|state| *state.borrow_and_update());
        let held = matches!(
            requested,
            Some(DefaultAdmission::Paused | DefaultAdmission::Closed)
        );
        if requested == Some(DefaultAdmission::Paused) && !*stop.borrow() && fault.is_none() {
            let drained = tasks.is_empty()
                && handlers.values().all(|handler| {
                    handler.0.slot.available_permits() == handler.0.inner.maximum_concurrency()
                });
            lane.state.send_if_modified(|state| {
                if drained && !matches!(state, State::Paused) {
                    *state = State::Paused;
                    true
                } else if !drained && !matches!(state, State::PauseRequested) {
                    *state = State::PauseRequested;
                    true
                } else {
                    false
                }
            });
        }
        if !held && !*stop.borrow() && fault.is_none() {
            for handler in handlers.values() {
                let Some(maintenance) = admitting(default_control.as_ref(), || {
                    handler.maintenance(caller.clone())
                }) else {
                    break;
                };
                if let Err(error) = maintenance.await {
                    fault = Some(error.clone());
                    lane.state.send_replace(State::Fault(error));
                    break;
                }
            }
        }
        if !held && !*stop.borrow() && fault.is_none() && tasks.len() < maximum {
            for (family, handler) in &handlers {
                if *family == Family::Recording {
                    continue;
                }
                if tasks.len() >= maximum {
                    break;
                }
                if handler.0.slot.available_permits() == 0 {
                    continue;
                }
                admitting(default_control.as_ref(), || {
                    if let Some(raw) = handler.due_head() {
                        if pending.insert(raw.clone()) {
                            let owned = handler.clone();
                            let owner = caller.clone();
                            tasks.spawn(async move {
                                let result = owned.run_due(raw.clone(), owner).await;
                                (raw, result)
                            });
                        }
                    }
                });
            }
        }
        if !held
            && !*stop.borrow()
            && fault.is_none()
            && tasks.len() < maximum
            && backoff.can_peek()
        {
            let peek = admitting(default_control.as_ref(), || head.peek(caller.clone()));
            // A pause racing with the physical peek must be rechecked before
            // its result can be turned into an admitted claim.
            let Some(peek) = peek else { continue };
            match peek.await {
                Ok(Some(raw)) => {
                    let selected = if handlers
                        .get(&Family::RegisteredPlugin)
                        .is_some_and(|handler| handler.registered_head(&raw))
                    {
                        Some(Family::RegisteredPlugin)
                    } else {
                        milestone_dispatch::classify(&raw)
                    }
                    .filter(|family| *family != Family::Recording);
                    if selected.is_none_or(|family| !handlers.contains_key(&family)) {
                        lane.state
                            .send_replace(State::Blocked(Disposition::UnsupportedHead));
                    } else {
                        let handler = handlers
                            .get(&selected.expect("checked selected family"))
                            .expect("checked registered family");
                        if handler.0.slot.available_permits() == 0 {
                            lane.state.send_replace(State::Working(tasks.len()));
                        } else {
                            let expected: Arc<[u8]> = Arc::from(raw);
                            admitting(default_control.as_ref(), || {
                                if backoff.allow(&expected) && !pending.contains(&expected) {
                                    pending.insert(expected.clone());
                                    let selected = expected.clone();
                                    let owned = handler.clone();
                                    let owner = caller.clone();
                                    tasks.spawn(async move {
                                        let result =
                                            owned.run_selected(selected.clone(), owner).await;
                                        (selected, result)
                                    });
                                    lane.state.send_replace(State::Working(tasks.len()));
                                }
                            });
                        }
                    }
                }
                Ok(None) => {
                    if tasks.is_empty() {
                        lane.state.send_replace(State::Observed(Disposition::Empty));
                    } else {
                        // A due task may have started before the first broker
                        // peek. The empty observation and held task together
                        // establish that this lane is active.
                        let starting = matches!(*lane.state.borrow(), State::Starting);
                        if starting {
                            lane.state.send_replace(State::Working(tasks.len()));
                        }
                    }
                }
                Err(error @ Disposition::UnsupportedHead)
                | Err(error @ Disposition::UnsupportedDomain) => {
                    lane.state.send_replace(State::Blocked(error));
                }
                Err(error) => {
                    fault = Some(error.clone());
                    lane.state.send_replace(State::Fault(error));
                }
            }
        }
        tokio::select! {
              result=tasks.join_next(),if !tasks.is_empty()=>{
               match result{
                Some(Ok((raw,result)))=>{pending.remove(&raw);backoff.outcome(raw.clone(),&result,poll);match result{Ok(value@Disposition::UnsupportedHead)|Ok(value@Disposition::UnsupportedDomain)=>{if fault.is_none(){lane.state.send_replace(State::Blocked(value));}},Ok(value)=>{if fault.is_none(){lane.state.send_replace(State::Observed(value));}},Err(error@Disposition::UnsupportedHead)|Err(error@Disposition::UnsupportedDomain)=>{if fault.is_none(){lane.state.send_replace(State::Blocked(error));}},Err(error@Disposition::Capacity)|Err(error@Disposition::HeadChanged)|Err(error@Disposition::NotDue)=>{if fault.is_none(){lane.state.send_replace(State::Observed(error));}},Err(error)=>{fault=Some(error.clone());lane.state.send_replace(State::Fault(error));}}},
                Some(Err(_))=>{let error=Disposition::Operational("normal Celery task supervisor failure".into());fault=Some(error.clone());lane.state.send_replace(State::Fault(error));},None=>{}
               }
              },
              _=stop.changed(),if !*stop.borrow()=>{},
              _=async { match default_control.as_mut() {
                  Some(state) => { let _ = state.changed().await; },
                  None => std::future::pending().await,
              }}=>{},
              _=async{match &advanced{Some(notify)=>notify.notified().await,None=>std::future::pending::<()>().await}}=>{backoff.wake();},
        _=tokio::time::sleep_until(backoff.deadline(poll))=>{}
             }
    }
    if fault.is_none() {
        lane.state.send_replace(State::Stopped);
    }
    lane.done.send_replace(true);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lane_is_ready_after_a_successful_poll() {
        for state in [
            State::Prepared,
            State::Starting,
            State::Stopped,
            State::Blocked(Disposition::UnsupportedHead),
            State::Fault(Disposition::Operational("Redis unavailable".into())),
        ] {
            assert!(!state.ready(), "{state:?}");
        }
        assert!(State::Working(1).ready());
        assert!(State::Observed(Disposition::Empty).ready());
    }

    #[test]
    fn actual_auto_family_admission_matches_existing_celery_lane_budget() {
        // Exercise the SAME guard called by Application::prepare, using the
        // exact metadata returned by the real normal Auto Handler below.
        assert!(validate_family_admission(
            Family::AutoChannels,
            crate::normal_auto_sync::CONCURRENCY,
            true
        )
        .is_ok());
        assert!(validate_family_admission(
            Family::AutoChannels,
            crate::normal_auto_sync::CONCURRENCY,
            false
        )
        .is_err());
        for invalid in [0, 1, CELERY_CONCURRENCY + 1, 21] {
            assert!(validate_family_admission(Family::AutoChannels, invalid, true).is_err());
        }
        assert!(validate_family_admission(Family::Recording, 20, true).is_ok());
        assert!(validate_family_admission(Family::Recording, 21, true).is_err());
        assert!(validate_family_admission(Family::M3uSingle, CELERY_CONCURRENCY, true).is_err());
        assert!(validate_family_admission(Family::M3uSingle, 1, true).is_ok());
    }

    use crate::milestone_dispatch::Head;
    use std::collections::VecDeque;
    use std::sync::atomic::AtomicUsize;
    struct Probe {
        queue: Mutex<VecDeque<Vec<u8>>>,
        starts: AtomicUsize,
        claims: AtomicUsize,
        active: AtomicUsize,
        duplicates: AtomicUsize,
        peeked: AtomicUsize,
        claim_gate: Arc<Semaphore>,
        finish_gate: Arc<Semaphore>,
        advanced: Arc<tokio::sync::Notify>,
    }
    impl Head for Probe {
        fn queue(&self) -> Queue {
            Queue::Dvr
        }
        fn peek(
            &self,
            _owner: Arc<dyn Send + Sync>,
        ) -> BoxFuture<'_, Result<Option<Vec<u8>>, Disposition>> {
            Box::pin(async move {
                self.peeked.fetch_add(1, Ordering::SeqCst);
                Ok(self.queue.lock().unwrap().front().cloned())
            })
        }
    }
    impl Lifecycle for Probe {
        fn shutdown(&self, _timeout: Duration) -> BoxFuture<'_, Result<(), Disposition>> {
            Box::pin(async { Ok(()) })
        }
    }
    impl Handler for Probe {
        fn maximum_concurrency(&self) -> usize {
            20
        }
        fn supports_selected_head(&self) -> bool {
            true
        }
        fn head_advance_notifier(&self) -> Option<Arc<tokio::sync::Notify>> {
            Some(self.advanced.clone())
        }
        fn run_once(
            &self,
            _owner: Arc<dyn Send + Sync>,
        ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
            Box::pin(async { Err(Disposition::UnsupportedDomain) })
        }
        fn run_selected(
            &self,
            expected: Arc<[u8]>,
            owner: Arc<dyn Send + Sync>,
        ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
            Box::pin(async move {
                let _owner = owner;
                self.starts.fetch_add(1, Ordering::SeqCst);
                self.claim_gate
                    .clone()
                    .acquire_owned()
                    .await
                    .unwrap()
                    .forget();
                {
                    let mut queue = self.queue.lock().unwrap();
                    if queue.front().map(Vec::as_slice) != Some(expected.as_ref()) {
                        self.duplicates.fetch_add(1, Ordering::SeqCst);
                        return Ok(Disposition::HeadChanged);
                    }
                    queue.pop_front();
                }
                self.claims.fetch_add(1, Ordering::SeqCst);
                self.active.fetch_add(1, Ordering::SeqCst);
                self.advanced.notify_one();
                self.finish_gate
                    .clone()
                    .acquire_owned()
                    .await
                    .unwrap()
                    .forget();
                self.active.fetch_sub(1, Ordering::SeqCst);
                Ok(Disposition::CompletedWithoutId)
            })
        }
    }
    #[test]
    fn core_schedule_binding_requires_both_actual_backup_entries_and_supported_due_families() {
        fn handler() -> Arc<dyn Handler> {
            Arc::new(Probe {
                queue: Mutex::new(VecDeque::new()),
                starts: AtomicUsize::new(0),
                claims: AtomicUsize::new(0),
                active: AtomicUsize::new(0),
                duplicates: AtomicUsize::new(0),
                peeked: AtomicUsize::new(0),
                claim_gate: Arc::new(Semaphore::new(0)),
                finish_gate: Arc::new(Semaphore::new(0)),
                advanced: Arc::new(tokio::sync::Notify::new()),
            })
        }
        let actual = handler();
        let unrelated = handler();
        let entry = |inner| {
            Arc::new(OwnedFamily {
                inner,
                slot: Arc::new(Semaphore::new(1)),
                closed: AtomicBool::new(false),
            })
        };
        let mut families = [
            Family::BackupCreate,
            Family::BackupScheduled,
            Family::BackendCleanup,
            Family::AccountExpiration,
            Family::VersionCheck,
            Family::Recording,
        ]
        .into_iter()
        .map(|family| (family, entry(actual.clone())))
        .collect::<BTreeMap<_, _>>();
        assert!(selected_schedules_match(&families, &actual));
        assert!(!selected_schedules_match(&families, &unrelated));
        families.insert(Family::BackupScheduled, entry(unrelated));
        assert!(!selected_schedules_match(&families, &actual));
        families.insert(Family::BackupScheduled, entry(actual.clone()));
        for family in [
            Family::BackendCleanup,
            Family::AccountExpiration,
            Family::VersionCheck,
            Family::Recording,
        ] {
            let removed = families.remove(&family).unwrap();
            assert!(!selected_schedules_match(&families, &actual));
            families.insert(family, removed);
        }
        assert!(!saved_xc_receivers_match(&families, &actual, &actual));
        for family in [
            Family::M3uSingle,
            Family::VodIngestion,
            Family::XcAccountProfiles,
        ] {
            families.insert(family, entry(actual.clone()));
        }
        assert!(saved_xc_receivers_match(&families, &actual, &actual));
        let other = handler();
        assert!(!saved_xc_receivers_match(&families, &actual, &other));
        assert!(!saved_xc_receivers_match(&families, &other, &actual));
        families.remove(&Family::XcAccountProfiles);
        assert!(!saved_xc_receivers_match(&families, &actual, &actual));
    }

    #[test]
    fn core_schedule_binding_rejects_duplicate_owner_and_taskless_conflict_in_either_order() {
        let mut mode = ScheduleOwner::Unbound;
        assert!(mode.bind(ScheduleOwner::Core).is_ok());
        assert!(mode.bind(ScheduleOwner::Core).is_err());
        assert!(mode.bind(ScheduleOwner::Taskless).is_err());

        let mut mode = ScheduleOwner::Unbound;
        assert!(mode.bind(ScheduleOwner::Taskless).is_ok());
        assert!(mode.bind(ScheduleOwner::Taskless).is_ok());
        assert!(mode.bind(ScheduleOwner::Core).is_err());
    }

    #[test]
    fn selected_schedule_receiver_blocked_can_recover_but_stopped_is_terminal() {
        use SelectedScheduleAvailability::{Terminal, Waiting};
        let ready = State::Working(1);
        let blocked = State::Blocked(Disposition::NotDue);
        let fault = State::Fault(Disposition::NotDue);
        assert_eq!(
            selected_schedule_availability(false, true, &blocked, &ready),
            Waiting
        );
        assert_eq!(
            selected_schedule_availability(false, true, &fault, &ready),
            Terminal
        );
        assert_eq!(
            selected_schedule_availability(false, true, &ready, &State::Stopped),
            Terminal
        );
    }

    #[test]
    fn core_schedule_binding_requires_live_selected_lanes_and_open_backup() {
        let running = State::Observed(Disposition::Empty);
        assert!(selected_schedules_ready(false, true, &running, &running));
        assert!(!selected_schedules_ready(true, true, &running, &running));
        assert!(!selected_schedules_ready(false, false, &running, &running));
        for blocked in [
            State::Prepared,
            State::Starting,
            State::Stopped,
            State::Blocked(Disposition::UnsupportedHead),
        ] {
            assert!(!selected_schedules_ready(false, true, &blocked, &running));
            assert!(!selected_schedules_ready(false, true, &running, &blocked));
        }
    }

    async fn until(mut test: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while !test() {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("bounded queue progress");
    }
    #[tokio::test]
    async fn same_selected_head_is_not_launched_twice_and_twenty_real_claims_drain_before_stop() {
        let probe=Arc::new(Probe {queue:Mutex::new((1..=21).map(|n|serde_json::to_vec(&serde_json::json!({"headers":{"task":"apps.channels.tasks.run_recording","id":n}})).unwrap()).collect()),starts:AtomicUsize::new(0),claims:AtomicUsize::new(0),active:AtomicUsize::new(0),duplicates:AtomicUsize::new(0),peeked:AtomicUsize::new(0),claim_gate:Arc::new(Semaphore::new(0)),finish_gate:Arc::new(Semaphore::new(0)),advanced:Arc::new(tokio::sync::Notify::new())});
        let head: Arc<dyn Head> = probe.clone();
        let wrapped = FamilyAdapter(Arc::new(OwnedFamily {
            inner: probe.clone(),
            slot: Arc::new(Semaphore::new(20)),
            closed: AtomicBool::new(false),
        }));
        let entries = milestone_dispatch::REQUIRED
            .into_iter()
            .chain([Family::BackupCreate, Family::BackupScheduled])
            .map(|family| (family, Arc::new(wrapped.clone()) as Arc<dyn Handler>))
            .collect();
        let lane = Arc::new(Lane {
            prepared: Prepared::new(head.clone(), entries).unwrap(),
            state: watch::channel(State::Starting).0,
            done: watch::channel(false).0,
        });
        let (stop, rx) = watch::channel(false);
        let task = tokio::spawn(dvr_lane(
            lane.clone(),
            head,
            wrapped,
            Arc::new(()),
            rx,
            Duration::from_millis(2),
        ));
        until(|| probe.peeked.load(Ordering::SeqCst) >= 6).await;
        assert_eq!(
            probe.starts.load(Ordering::SeqCst),
            1,
            "same unclaimed raw must stay single in flight"
        );
        probe.claim_gate.add_permits(21);
        until(|| probe.claims.load(Ordering::SeqCst) == 20).await;
        assert_eq!(probe.active.load(Ordering::SeqCst), 20);
        assert_eq!(probe.starts.load(Ordering::SeqCst), 20);
        assert_eq!(probe.queue.lock().unwrap().len(), 1);
        stop.send_replace(true);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            !*lane.done.borrow(),
            "stop cannot settle twenty retained physical task owners"
        );
        assert!(!task.is_finished());
        probe.finish_gate.add_permits(20);
        tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(probe.duplicates.load(Ordering::SeqCst), 0);
        assert_eq!(probe.active.load(Ordering::SeqCst), 0);
        assert_eq!(probe.queue.lock().unwrap().len(), 1);
        assert!(*lane.done.borrow());
        assert!(matches!(*lane.state.borrow(), State::Stopped));
    }

    #[derive(Default)]
    struct EmptyCeleryHead(AtomicUsize);

    impl Head for EmptyCeleryHead {
        fn queue(&self) -> Queue {
            Queue::Celery
        }
        fn peek(
            &self,
            _owner: Arc<dyn Send + Sync>,
        ) -> BoxFuture<'_, Result<Option<Vec<u8>>, Disposition>> {
            Box::pin(async move {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(None)
            })
        }
    }

    struct HeldDueTask {
        started: AtomicBool,
        finish: Semaphore,
    }

    impl Lifecycle for HeldDueTask {
        fn shutdown(&self, _timeout: Duration) -> BoxFuture<'_, Result<(), Disposition>> {
            Box::pin(async { Ok(()) })
        }
    }

    impl Handler for HeldDueTask {
        fn due_head(&self) -> Option<Arc<[u8]>> {
            (!self.started.load(Ordering::Acquire)).then(|| Arc::from(&b"owned due task"[..]))
        }
        fn run_due(
            &self,
            expected: Arc<[u8]>,
            owner: Arc<dyn Send + Sync>,
        ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
            Box::pin(async move {
                let _owner = owner;
                assert_eq!(expected.as_ref(), b"owned due task");
                assert!(!self.started.swap(true, Ordering::AcqRel));
                self.finish
                    .acquire()
                    .await
                    .expect("release held task")
                    .forget();
                Ok(Disposition::CompletedWithoutId)
            })
        }
        fn run_once(
            &self,
            _owner: Arc<dyn Send + Sync>,
        ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
            Box::pin(async { panic!("the empty broker head cannot start ordinary work") })
        }
    }

    #[test]
    fn default_pause_requires_activation_and_cannot_reopen_closed_admission() {
        let control = DefaultQueueControl::new();
        assert!(!control.request(true));
        assert!(control.receiver_if_configured().is_none());
        assert!(!control.enable());
        assert!(control.receiver_if_configured().is_none());
        assert!(control.configure());
        assert!(!control.configure());
        assert!(!control.request(true));
        let receiver = control.receiver_if_configured().unwrap();
        assert!(control.enable());
        assert_eq!(admitting(Some(&receiver), || 1), Some(1));
        assert!(control.request(true));
        assert!(control.request(true));
        assert_eq!(admitting(Some(&receiver), || 1), None);
        assert!(control.request(false));
        assert_eq!(admitting(Some(&receiver), || 1), Some(1));
        control.close();
        assert!(!control.enable());
        assert!(!control.request(false));
        assert_eq!(admitting(Some(&receiver), || 1), None);
    }

    #[test]
    fn configured_default_pause_keeps_scheduler_publication_available_only_with_live_dvr() {
        for state in [State::PauseRequested, State::Paused] {
            assert!(!state.ready());
            assert!(selected_schedules_ready(
                false,
                true,
                &state,
                &State::Working(0)
            ));
            assert!(!selected_schedules_ready(
                false,
                true,
                &state,
                &State::Stopped
            ));
            assert!(!selected_schedules_ready(
                false,
                true,
                &state,
                &State::Starting
            ));
            assert!(!selected_schedules_ready(
                true,
                true,
                &state,
                &State::Working(0)
            ));
        }
    }

    #[test]
    fn default_failure_blocks_scheduler_even_with_live_dvr() {
        let dvr = State::Working(0);
        assert_eq!(
            selected_schedule_availability(
                false,
                true,
                &State::Fault(Disposition::Operational(
                    "owned default receiver fault".into()
                ),),
                &dvr
            ),
            SelectedScheduleAvailability::Terminal
        );
        assert_eq!(
            selected_schedule_availability(false, true, &State::Stopped, &dvr),
            SelectedScheduleAvailability::Terminal
        );
    }

    async fn default_pause_with_admitted_due_lease(stop_before_release: bool) {
        let head = Arc::new(EmptyCeleryHead::default());
        let due = Arc::new(HeldDueTask {
            started: AtomicBool::new(false),
            finish: Semaphore::new(0),
        });
        let handler = FamilyAdapter(Arc::new(OwnedFamily {
            inner: due.clone(),
            slot: Arc::new(Semaphore::new(1)),
            closed: AtomicBool::new(false),
        }));
        let entries = milestone_dispatch::REQUIRED
            .into_iter()
            .chain([Family::BackupCreate, Family::BackupScheduled])
            .map(|family| (family, Arc::new(handler.clone()) as Arc<dyn Handler>))
            .collect();
        let lane = Arc::new(Lane {
            prepared: Prepared::new(head.clone(), entries).unwrap(),
            state: watch::channel(State::Starting).0,
            done: watch::channel(false).0,
        });
        let (stop, rx) = watch::channel(false);
        let control = DefaultQueueControl::new();
        assert!(control.configure());
        assert!(control.enable());
        let running = tokio::spawn(celery_lane(
            lane.clone(),
            head.clone(),
            BTreeMap::from([(Family::Artwork, handler)]),
            Arc::new(()),
            rx,
            Duration::from_millis(2),
            control.receiver_if_configured(),
        ));
        until(|| due.started.load(Ordering::Acquire)).await;
        assert!(control.request(true));
        until(|| matches!(*lane.state.borrow(), State::PauseRequested)).await;
        let peeks = head.0.load(Ordering::SeqCst);
        if stop_before_release {
            control.close();
            stop.send_replace(true);
            assert!(!control.request(false));
            assert!(!running.is_finished());
            due.finish.add_permits(1);
            running.await.unwrap();
            assert!(*lane.done.borrow());
            assert_eq!(head.0.load(Ordering::SeqCst), peeks);
            return;
        }
        due.finish.add_permits(1);
        until(|| matches!(*lane.state.borrow(), State::Paused)).await;
        assert_eq!(head.0.load(Ordering::SeqCst), peeks);
        assert!(control.request(false));
        until(|| head.0.load(Ordering::SeqCst) > peeks).await;
        stop.send_replace(true);
        running.await.unwrap();
        assert!(*lane.done.borrow());
    }

    #[tokio::test]
    async fn default_pause_waits_for_admitted_due_lease_and_resume_wakes_receiver() {
        default_pause_with_admitted_due_lease(false).await;
    }

    #[tokio::test]
    async fn stopping_during_default_pause_closes_control_and_drains_the_admitted_due_lease() {
        default_pause_with_admitted_due_lease(true).await;
    }

    struct HeldDefaultPeek {
        entered: AtomicBool,
        release: Semaphore,
    }
    impl Head for HeldDefaultPeek {
        fn queue(&self) -> Queue {
            Queue::Celery
        }
        fn peek(
            &self,
            _owner: Arc<dyn Send + Sync>,
        ) -> BoxFuture<'_, Result<Option<Vec<u8>>, Disposition>> {
            Box::pin(async move {
                self.entered.store(true, Ordering::SeqCst);
                self.release.acquire().await.unwrap().forget();
                Ok(Some(br#"{"headers":{"task":"apps.channels.tasks.prefetch_recording_artwork","id":"owned-head"}}"#.to_vec()))
            })
        }
    }

    #[tokio::test]
    async fn pause_racing_inflight_peek_does_not_admit_the_returned_head() {
        let head = Arc::new(HeldDefaultPeek {
            entered: AtomicBool::new(false),
            release: Semaphore::new(0),
        });
        // HeldDueTask panics if it is asked to run a source FIFO claim.
        let handler = FamilyAdapter(Arc::new(OwnedFamily {
            inner: Arc::new(HeldDueTask {
                started: AtomicBool::new(true),
                finish: Semaphore::new(0),
            }),
            slot: Arc::new(Semaphore::new(1)),
            closed: AtomicBool::new(false),
        }));
        let entries = milestone_dispatch::REQUIRED
            .into_iter()
            .chain([Family::BackupCreate, Family::BackupScheduled])
            .map(|family| (family, Arc::new(handler.clone()) as Arc<dyn Handler>))
            .collect();
        let lane = Arc::new(Lane {
            prepared: Prepared::new(head.clone(), entries).unwrap(),
            state: watch::channel(State::Starting).0,
            done: watch::channel(false).0,
        });
        let (stop, rx) = watch::channel(false);
        let control = DefaultQueueControl::new();
        assert!(control.configure());
        assert!(control.enable());
        let running = tokio::spawn(celery_lane(
            lane.clone(),
            head.clone(),
            BTreeMap::from([(Family::Artwork, handler)]),
            Arc::new(()),
            rx,
            Duration::from_millis(2),
            control.receiver_if_configured(),
        ));
        until(|| head.entered.load(Ordering::SeqCst)).await;
        assert!(control.request(true));
        assert!(!matches!(*lane.state.borrow(), State::Paused));
        head.release.add_permits(1);
        until(|| matches!(*lane.state.borrow(), State::Paused)).await;
        stop.send_replace(true);
        running.await.unwrap();
        assert!(matches!(*lane.state.borrow(), State::Stopped));
    }

    #[tokio::test]
    async fn celery_first_empty_broker_peek_reports_working_during_held_due_task() {
        let head = Arc::new(EmptyCeleryHead::default());
        let due = Arc::new(HeldDueTask {
            started: AtomicBool::new(false),
            finish: Semaphore::new(0),
        });
        let handler = FamilyAdapter(Arc::new(OwnedFamily {
            inner: due.clone(),
            slot: Arc::new(Semaphore::new(1)),
            closed: AtomicBool::new(false),
        }));
        let entries = milestone_dispatch::REQUIRED
            .into_iter()
            .chain([Family::BackupCreate, Family::BackupScheduled])
            .map(|family| (family, Arc::new(handler.clone()) as Arc<dyn Handler>))
            .collect();
        let lane = Arc::new(Lane {
            prepared: Prepared::new(head.clone(), entries).expect("complete family fixture"),
            state: watch::channel(State::Starting).0,
            done: watch::channel(false).0,
        });
        let (stop, rx) = watch::channel(false);
        let running = tokio::spawn(celery_lane(
            lane.clone(),
            head.clone(),
            BTreeMap::from([(Family::Artwork, handler)]),
            Arc::new(()),
            rx,
            Duration::from_millis(10),
            None,
        ));
        until(|| due.started.load(Ordering::Acquire) && head.0.load(Ordering::SeqCst) > 0).await;
        assert!(matches!(*lane.state.borrow(), State::Working(1)));
        assert!(!running.is_finished());

        stop.send_replace(true);
        due.finish.add_permits(1);
        tokio::time::timeout(Duration::from_secs(3), running)
            .await
            .expect("bounded Celery lane retirement")
            .expect("actual Celery lane task");
        assert!(*lane.done.borrow());
        assert!(matches!(*lane.state.borrow(), State::Stopped));
    }

    struct UnclaimedProbe {
        queue: Queue,
        raw: Mutex<Vec<u8>>,
        outcome: Mutex<Disposition>,
        peeks: AtomicUsize,
        runs: AtomicUsize,
        advanced: Arc<tokio::sync::Notify>,
    }
    impl Head for UnclaimedProbe {
        fn queue(&self) -> Queue {
            self.queue
        }
        fn peek(
            &self,
            _owner: Arc<dyn Send + Sync>,
        ) -> BoxFuture<'_, Result<Option<Vec<u8>>, Disposition>> {
            Box::pin(async move {
                self.peeks.fetch_add(1, Ordering::SeqCst);
                Ok(Some(self.raw.lock().unwrap().clone()))
            })
        }
    }
    impl Lifecycle for UnclaimedProbe {
        fn shutdown(&self, _timeout: Duration) -> BoxFuture<'_, Result<(), Disposition>> {
            Box::pin(async { Ok(()) })
        }
    }
    impl Handler for UnclaimedProbe {
        fn maximum_concurrency(&self) -> usize {
            if self.queue == Queue::Dvr {
                20
            } else {
                1
            }
        }
        fn supports_selected_head(&self) -> bool {
            true
        }
        fn head_advance_notifier(&self) -> Option<Arc<tokio::sync::Notify>> {
            Some(self.advanced.clone())
        }
        fn run_once(
            &self,
            _owner: Arc<dyn Send + Sync>,
        ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
            Box::pin(async { Err(Disposition::UnsupportedDomain) })
        }
        fn run_selected(
            &self,
            _expected: Arc<[u8]>,
            _owner: Arc<dyn Send + Sync>,
        ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
            Box::pin(async move {
                self.runs.fetch_add(1, Ordering::SeqCst);
                Ok(self.outcome.lock().unwrap().clone())
            })
        }
    }
    async fn settle_driver() {
        for _ in 0..128 {
            tokio::task::yield_now().await;
        }
    }
    /// Real production lane loops with counted broker Head reads, paused clock
    /// and immediate unclaimed handler completions. This reproduces the actual
    /// ETA hotloop rather than merely testing a cooldown function in isolation.
    #[tokio::test(start_paused = true)]
    async fn both_lanes_bound_actual_peeks_and_relaunches_for_unclaimed_heads() {
        for queue in [Queue::Celery, Queue::Dvr] {
            let task_name = if queue == Queue::Dvr {
                "apps.channels.tasks.run_recording"
            } else {
                "apps.channels.tasks.prefetch_recording_artwork"
            };
            let raw =
                serde_json::to_vec(&serde_json::json!({"headers":{"task":task_name,"id":"first"}}))
                    .unwrap();
            let probe = Arc::new(UnclaimedProbe {
                queue,
                raw: Mutex::new(raw),
                outcome: Mutex::new(Disposition::NotDue),
                peeks: AtomicUsize::new(0),
                runs: AtomicUsize::new(0),
                advanced: Arc::new(tokio::sync::Notify::new()),
            });
            let maximum = probe.maximum_concurrency();
            let handler = FamilyAdapter(Arc::new(OwnedFamily {
                inner: probe.clone(),
                slot: Arc::new(Semaphore::new(maximum)),
                closed: AtomicBool::new(false),
            }));
            let head: Arc<dyn Head> = probe.clone();
            let entries = milestone_dispatch::REQUIRED
                .into_iter()
                .chain([Family::BackupCreate, Family::BackupScheduled])
                .map(|family| (family, Arc::new(handler.clone()) as Arc<dyn Handler>))
                .collect();
            let lane = Arc::new(Lane {
                prepared: Prepared::new(head.clone(), entries).unwrap(),
                state: watch::channel(State::Starting).0,
                done: watch::channel(false).0,
            });
            let (stop, rx) = watch::channel(false);
            let poll = Duration::from_millis(100);
            let running = if queue == Queue::Dvr {
                tokio::spawn(dvr_lane(
                    lane.clone(),
                    head,
                    handler,
                    Arc::new(()),
                    rx,
                    poll,
                ))
            } else {
                let family = Family::Artwork;
                tokio::spawn(celery_lane(
                    lane.clone(),
                    head,
                    [(family, handler)].into_iter().collect(),
                    Arc::new(()),
                    rx,
                    poll,
                    None,
                ))
            };
            settle_driver().await;
            assert_eq!(probe.peeks.load(Ordering::SeqCst), 1);
            assert_eq!(probe.runs.load(Ordering::SeqCst), 1);
            tokio::time::advance(Duration::from_millis(99)).await;
            settle_driver().await;
            assert_eq!(probe.peeks.load(Ordering::SeqCst), 1);
            assert_eq!(probe.runs.load(Ordering::SeqCst), 1);
            tokio::time::advance(Duration::from_millis(1)).await;
            settle_driver().await;
            assert_eq!(probe.peeks.load(Ordering::SeqCst), 2);
            assert_eq!(probe.runs.load(Ordering::SeqCst), 2);
            *probe.outcome.lock().unwrap() = Disposition::Capacity;
            tokio::time::advance(poll).await;
            settle_driver().await;
            assert_eq!(probe.peeks.load(Ordering::SeqCst), 3);
            assert_eq!(probe.runs.load(Ordering::SeqCst), 3);
            *probe.outcome.lock().unwrap() = Disposition::HeadChanged;
            tokio::time::advance(poll).await;
            settle_driver().await;
            // HeadChanged permits one fresh read, but an identical raw cannot
            // create another task/read loop at the same clock instant.
            assert_eq!(probe.peeks.load(Ordering::SeqCst), 5);
            assert_eq!(probe.runs.load(Ordering::SeqCst), 4);
            *probe.outcome.lock().unwrap() = Disposition::NotDue;
            tokio::time::advance(poll).await;
            settle_driver().await;
            assert_eq!(probe.peeks.load(Ordering::SeqCst), 6);
            assert_eq!(probe.runs.load(Ordering::SeqCst), 5);
            if queue == Queue::Dvr {
                // Genuine head advance hint follows the old raw's removal.
                // The changed next head progresses without waiting100ms.
                *probe.raw.lock().unwrap() = serde_json::to_vec(
                    &serde_json::json!({"headers":{"task":task_name,"id":"next"}}),
                )
                .unwrap();
                let before = tokio::time::Instant::now();
                probe.advanced.notify_one();
                settle_driver().await;
                assert_eq!(tokio::time::Instant::now(), before);
                assert_eq!(probe.peeks.load(Ordering::SeqCst), 7);
                assert_eq!(probe.runs.load(Ordering::SeqCst), 6);
            }
            stop.send_replace(true);
            running.await.unwrap();
            assert!(*lane.done.borrow());
            assert!(matches!(*lane.state.borrow(), State::Stopped));
        }
    }
}

#[cfg(test)]
#[path = "normal_queue_eta_tests.rs"]
mod delayed_tests;

#[cfg(test)]
mod registered_plugin_admission_tests {
    use super::*;
    struct Metadata {
        maximum: usize,
        selected: bool,
    }
    impl Lifecycle for Metadata {
        fn shutdown(&self, _timeout: Duration) -> BoxFuture<'_, Result<(), Disposition>> {
            Box::pin(async { Ok(()) })
        }
    }
    impl Handler for Metadata {
        fn maximum_concurrency(&self) -> usize {
            self.maximum
        }
        fn supports_selected_head(&self) -> bool {
            self.selected
        }
        fn run_once(
            &self,
            _owner: Arc<dyn Send + Sync>,
        ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
            Box::pin(async { Err(Disposition::UnsupportedDomain) })
        }
    }
    fn entries(plugin: Option<(usize, bool)>) -> Vec<(Family, Arc<dyn Handler>)> {
        let mut entries = milestone_dispatch::REQUIRED
            .into_iter()
            .chain([Family::BackupCreate, Family::BackupScheduled])
            .map(|f| {
                (
                    f,
                    Arc::new(Metadata {
                        maximum: 1,
                        selected: true,
                    }) as Arc<dyn Handler>,
                )
            })
            .collect::<Vec<_>>();
        if let Some((maximum, selected)) = plugin {
            entries.push((
                Family::RegisteredPlugin,
                Arc::new(Metadata { maximum, selected }),
            ));
        }
        entries
    }
    #[tokio::test]
    async fn actual_extended_constructor_rejects_missing_or_duplicate_registered_family() {
        let absent = Application::prepare_with_registered_plugins(
            "not-a-redis-url",
            "not-a-redis-url",
            entries(None),
            Duration::from_millis(100),
        )
        .await;
        assert!(matches!(
            absent,
            Err(Error::MissingFamily(Family::RegisteredPlugin))
        ));
        let mut duplicate = entries(Some((6, true)));
        duplicate.push((
            Family::RegisteredPlugin,
            Arc::new(Metadata {
                maximum: 6,
                selected: true,
            }),
        ));
        let duplicate = Application::prepare_with_registered_plugins(
            "not-a-redis-url",
            "not-a-redis-url",
            duplicate,
            Duration::from_millis(100),
        )
        .await;
        assert!(matches!(
            duplicate,
            Err(Error::DuplicateFamily(Family::RegisteredPlugin))
        ));
    }
    #[tokio::test]
    async fn actual_extended_constructor_admits_selected_six_to_transport_but_not_bad_metadata() {
        // Invalid URL prevents sockets. Reaching the real transport branch proves
        // the actual application validator accepted metadata; this is NOT Main
        // startup/runtime proof, which belongs to the grouped physical cohort.
        let accepted = Application::prepare_with_registered_plugins(
            "not-a-redis-url",
            "not-a-redis-url",
            entries(Some((6, true))),
            Duration::from_millis(100),
        )
        .await;
        assert!(matches!(accepted, Err(Error::Transport)));
        for invalid in [(1, true), (5, true), (7, true), (6, false)] {
            let rejected = Application::prepare_with_registered_plugins(
                "not-a-redis-url",
                "not-a-redis-url",
                entries(Some(invalid)),
                Duration::from_millis(100),
            )
            .await;
            assert!(matches!(rejected, Err(Error::Configuration)));
        }
    }
}

#[cfg(test)]
mod sd_registry_controls {
    use super::*;
    #[test]
    fn actual_sd_descriptor_is_admitted_only_at_existing_global_six() {
        let (maximum, selected) = crate::normal_sd_guide::descriptor();
        assert_eq!(maximum, CELERY_CONCURRENCY);
        assert!(validate_family_admission(Family::SdGuide, maximum, selected).is_ok());
        for wrong in [0, 1, 2, 5, 7, 20, usize::MAX] {
            assert!(validate_family_admission(Family::SdGuide, wrong, true).is_err());
        }
        assert!(validate_family_admission(Family::SdGuide, maximum, false).is_err());
        assert!(validate_family_admission(Family::TvgProgrammes, maximum, true).is_err());
    }
}

#[cfg(test)]
mod taskless_builtin_controls {
    use super::*;

    #[test]
    fn taskless_builtin_authority_uses_only_the_observed_selected_families() {
        for (task, expected) in [
            (
                "apps.m3u.tasks.refresh_single_m3u_account",
                Family::M3uSingle,
            ),
            (
                "apps.backups.tasks.scheduled_backup_task",
                Family::BackupScheduled,
            ),
            (
                "apps.plugins.tasks.refresh_plugin_repos",
                Family::PluginRefresh,
            ),
        ] {
            assert_eq!(milestone_dispatch::builtin_family(task), Some(expected));
            assert_eq!(taskless_builtin_family(task), Some(expected));
        }
        for task in [
            "apps.backups.tasks.restore_backup_task",
            "apps.plugins.tasks.fixture_task",
            "fixture.plugin.tasks.refresh_plugin_repos",
            "",
        ] {
            assert_eq!(taskless_builtin_family(task), None);
        }
    }
}

#[cfg(test)]
mod core_registry_sublease_tests {
    use super::*;
    use serde_json::json;
    struct Actor {
        maximum: usize,
        selected: bool,
    }
    impl Lifecycle for Actor {
        fn shutdown(&self, _: Duration) -> BoxFuture<'_, Result<(), Disposition>> {
            Box::pin(async { Ok(()) })
        }
    }
    impl Handler for Actor {
        fn maximum_concurrency(&self) -> usize {
            self.maximum
        }
        fn supports_selected_head(&self) -> bool {
            self.selected
        }
        fn run_once(
            &self,
            _: Arc<dyn Send + Sync>,
        ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
            Box::pin(async { Err(Disposition::UnsupportedDomain) })
        }
    }
    pub(super) fn actor(maximum: usize) -> Arc<dyn Handler> {
        Arc::new(Actor {
            maximum,
            selected: true,
        })
    }
    pub(super) fn entry(inner: Arc<dyn Handler>) -> Arc<OwnedFamily> {
        Arc::new(OwnedFamily {
            slot: Arc::new(Semaphore::new(inner.maximum_concurrency())),
            inner,
            closed: AtomicBool::new(false),
        })
    }
    type Fixture = (
        BTreeMap<Family, Arc<OwnedFamily>>,
        Arc<dyn Handler>,
        Arc<dyn Handler>,
    );
    fn prepared() -> Fixture {
        let plugin = actor(CELERY_CONCURRENCY);
        let backup = actor(1);
        let map = [
            (Family::RegisteredPlugin, entry(plugin.clone())),
            (Family::BackupCreate, entry(backup.clone())),
            (Family::BackupScheduled, entry(backup.clone())),
        ]
        .into_iter()
        .collect();
        (map, plugin, backup)
    }
    #[test]
    fn actual_bound_handlers_and_enabled_or_disabled_snapshot_rows_are_read_only_members() {
        let (map, plugin, backup) = prepared();
        assert!(core_registry_members(&map, true, &plugin, &backup, &[]).is_some());
        for enabled in [true, false] {
            let rows = vec![
                json!({"id":1,"task":"apps.backups.tasks.scheduled_backup_task","enabled":enabled}),
            ];
            let members = core_registry_members(&map, true, &plugin, &backup, &rows).unwrap();
            assert!(Arc::ptr_eq(
                members.get(&Family::BackupScheduled).unwrap(),
                map.get(&Family::BackupScheduled).unwrap()
            ));
        }
    }
    #[test]
    fn same_metadata_foreign_handlers_and_missing_registration_cannot_issue_even_empty_snapshot() {
        let (map, plugin, backup) = prepared();
        assert!(
            core_registry_members(&map, true, &actor(CELERY_CONCURRENCY), &backup, &[]).is_none()
        );
        assert!(core_registry_members(&map, true, &plugin, &actor(1), &[]).is_none());
        assert!(core_registry_members(&map, false, &plugin, &backup, &[]).is_none());
    }
    #[test]
    fn missing_closed_and_unprepared_actual_family_reject_before_metadata_can_supply_authority() {
        let (mut map, plugin, backup) = prepared();
        let old = map.remove(&Family::BackupCreate).unwrap();
        assert!(core_registry_members(&map, true, &plugin, &backup, &[]).is_none());
        map.insert(Family::BackupCreate, old);
        map[&Family::RegisteredPlugin]
            .closed
            .store(true, Ordering::Release);
        assert!(core_registry_members(&map, true, &plugin, &backup, &[]).is_none());
        map[&Family::RegisteredPlugin]
            .closed
            .store(false, Ordering::Release);
        map.insert(
            Family::BackupScheduled,
            entry(Arc::new(Actor {
                maximum: 1,
                selected: false,
            })),
        );
        assert!(core_registry_members(&map, true, &plugin, &backup, &[]).is_none());
    }
    #[test]
    fn matching_pointer_without_selected_support_is_not_a_prepared_receiver() {
        let (mut map, plugin, _) = prepared();
        let backup: Arc<dyn Handler> = Arc::new(Actor {
            maximum: 1,
            selected: false,
        });
        map.insert(Family::BackupCreate, entry(backup.clone()));
        map.insert(Family::BackupScheduled, entry(backup.clone()));
        assert!(core_registry_members(&map, true, &plugin, &backup, &[]).is_none());
    }
    #[test]
    fn unknown_missing_malformed_duplicate_or_over_cap_rows_fail_closed() {
        let (map, plugin, backup) = prepared();
        let row = json!({"id":1,"task":"apps.backups.tasks.scheduled_backup_task","enabled":true});
        for rows in [
            vec![json!({"id":1,"task":"unowned.plugin.task","enabled":true})],
            vec![json!({"id":1,"task":"apps.m3u.tasks.refresh_single_m3u_account","enabled":true})],
            vec![
                json!({"id":1,"task":"apps.backups.tasks.scheduled_backup_task","enabled":"true"}),
            ],
            vec![row.clone(), row.clone()],
            vec![row.clone(); 257],
        ] {
            assert!(core_registry_members(&map, true, &plugin, &backup, &rows).is_none());
        }
        map[&Family::BackupScheduled]
            .closed
            .store(true, Ordering::Release);
        assert!(core_registry_members(&map, true, &plugin, &backup, &[row]).is_none());
    }
}

#[cfg(test)]
mod core_registry_budget_controls {
    use super::core_registry_sublease_tests::{actor, entry};
    use super::*;
    #[test]
    fn actual_registered_concurrency_is_required_even_with_matching_pointer() {
        let plugin = actor(1);
        let backup = actor(1);
        let map = [
            (Family::RegisteredPlugin, entry(plugin.clone())),
            (Family::BackupCreate, entry(backup.clone())),
            (Family::BackupScheduled, entry(backup.clone())),
        ]
        .into_iter()
        .collect();
        assert!(!core_registry_bindings_match(&map, true, &plugin, &backup));
    }
}
