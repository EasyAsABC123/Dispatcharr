//! Experimental selected-family composition; complete Source parity is open.
//! The composed SDK batch passed; packaged normal Main acceptance is separate.
//! Original Dispatcharr/Celery authors retain all application/protocol credit.
//! One bounded queue head is selected without removal/reordering. Each handler
//! still performs its own actual family/lineage/domain preflight and exact CAS.
use dispatcharr_contracts::BoxFuture;
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Family {
    StartupRecovery,
    RehashStreams,
    RescheduleOffsets,
    Scanner,
    M3uGroups,
    M3uSingle,
    Refresh,
    ProgrammeIndex,
    TvgProgrammes,
    Series,
    Recurring,
    Artwork,
    Recording,
    BackendCleanup,
    ProxyStats,
    VersionCheck,
    AccountExpiration,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Disposition {
    Empty,
    NotDue,
    Capacity,
    Restored(uuid::Uuid),
    HeadChanged,
    Completed(uuid::Uuid),
    CompletedNamed(String),
    CompletedWithoutId,
    TaskFailedWithoutId,
    TaskFailed(uuid::Uuid),
    TaskFailedNamed(String),
    Revoked(uuid::Uuid),
    RevokedNamed(String),
    UnsupportedHead,
    UnsupportedDomain,
    Operational(String),
}
/// A bounded read-only peek, physically retaining the SAME caller through its
/// real Redis reply. Implementations must never pop, quarantine or skip a head.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Queue {
    Celery,
    Dvr,
}
pub trait Head: Send + Sync {
    fn queue(&self) -> Queue;
    fn peek(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Option<Vec<u8>>, Disposition>>;
}
/// Prepared family adapters own their existing worker/service and authority.
/// This dispatcher grants no Jobs/lease authority and never synthesizes a result.
pub trait Lifecycle: Send + Sync {
    /// Stop new admissions / restore only unstarted reservations, without
    /// closing shared ports needed by other still-active family owners.
    fn stop_admission(&self) {}

    fn shutdown(&self, timeout: std::time::Duration) -> BoxFuture<'_, Result<(), Disposition>>;
}
pub trait Handler: Lifecycle + Send + Sync {
    /// Wake hint only; selectors still re-read the actual raw head.
    fn head_advance_notifier(&self) -> Option<Arc<tokio::sync::Notify>> {
        None
    }
    fn maximum_concurrency(&self) -> usize {
        1
    }
    fn supports_selected_head(&self) -> bool {
        false
    }
    /// Normal concurrent DVR admission independently checks exact re-peeked
    /// bytes. Legacy handlers have no implicit concurrent/selected authority.
    fn run_selected(
        &self,
        _expected: Arc<[u8]>,
        _owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
        Box::pin(async { Err(Disposition::UnsupportedDomain) })
    }

    fn run_once(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Disposition, Disposition>>;
}
#[derive(Debug, PartialEq, Eq)]
pub enum PrepareError {
    MissingFamily(Family),
    DuplicateFamily(Family),
}
pub struct Prepared {
    head: Arc<dyn Head>,
    handlers: BTreeMap<Family, Arc<dyn Handler>>,
}
pub(crate) const REQUIRED: [Family; 14] = [
    Family::Scanner,
    Family::M3uGroups,
    Family::M3uSingle,
    Family::Refresh,
    Family::ProgrammeIndex,
    Family::TvgProgrammes,
    Family::Series,
    Family::Recurring,
    Family::Artwork,
    Family::Recording,
    Family::BackendCleanup,
    Family::ProxyStats,
    Family::VersionCheck,
    Family::AccountExpiration,
];
impl Prepared {
    pub fn prepared_families(&self) -> usize {
        self.handlers.len()
    }
    /// No partial set can be mislabeled full-milestone-ready. The selected
    /// batch may diagnose a missing family, but MUST NOT start normal intake.
    pub fn new(
        head: Arc<dyn Head>,
        entries: Vec<(Family, Arc<dyn Handler>)>,
    ) -> Result<Self, PrepareError> {
        let mut handlers = BTreeMap::new();
        for (family, handler) in entries {
            if handlers.insert(family, handler).is_some() {
                return Err(PrepareError::DuplicateFamily(family));
            }
        }
        for family in REQUIRED {
            if !handlers.contains_key(&family) {
                return Err(PrepareError::MissingFamily(family));
            }
        }
        Ok(Self { head, handlers })
    }
    pub async fn shutdown_all(
        &self,
        deadline: tokio::time::Instant,
    ) -> Vec<(Family, Result<(), Disposition>)> {
        let mut results = Vec::new();
        // Global poller intake must be stopped first. Attempt EVERY family even
        // when one is unsettled, sharing one actual absolute shutdown deadline.
        for (family, handler) in &self.handlers {
            let outcome = handler
                .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
                .await;
            results.push((*family, outcome));
        }
        results
    }
    pub async fn run_once(&self, owner: Arc<dyn Send + Sync>) -> Result<Disposition, Disposition> {
        let Some(raw) = self.head.peek(owner.clone()).await? else {
            return Ok(Disposition::Empty);
        };
        let family = classify(&raw).ok_or(Disposition::UnsupportedHead)?;
        if (family == Family::Recording) != (self.head.queue() == Queue::Dvr) {
            return Err(Disposition::UnsupportedHead);
        }
        // Selection alone is never ACK authority. The selected adapter re-peeks
        // and validates under its own retained owner/CAS. A changed FIFO is not
        // retried by another handler, and an unknown head is left untouched.
        self.handlers
            .get(&family)
            .ok_or(Disposition::UnsupportedHead)?
            .run_once(owner)
            .await
    }
}
pub fn classify(raw: &[u8]) -> Option<Family> {
    if raw.len() > 65536 {
        return None;
    }
    let frame: serde_json::Value = serde_json::from_slice(raw).ok()?;
    let task = frame.get("headers")?.get("task")?.as_str()?;
    Some(match task {
        "core.tasks.rehash_streams" => Family::RehashStreams,
        "apps.channels.tasks.reschedule_upcoming_recordings_for_offset_change" => {
            Family::RescheduleOffsets
        }
        "apps.channels.tasks.recover_recordings_on_startup" => Family::StartupRecovery,
        "core.tasks.scan_and_process_files" => Family::Scanner,
        "apps.m3u.tasks.refresh_m3u_groups" => Family::M3uGroups,
        "apps.m3u.tasks.refresh_single_m3u_account" => Family::M3uSingle,
        "apps.epg.tasks.refresh_epg_data" => Family::Refresh,
        "apps.epg.tasks.build_programme_index_task" => Family::ProgrammeIndex,
        "apps.epg.tasks.parse_programs_for_tvg_id" => Family::TvgProgrammes,
        "apps.channels.tasks.evaluate_series_rules" => Family::Series,
        "apps.channels.tasks.maintain_recurring_recordings" => Family::Recurring,
        "apps.channels.tasks.prefetch_recording_artwork" => Family::Artwork,
        "apps.channels.tasks.run_recording" => Family::Recording,
        "celery.backend_cleanup" => Family::BackendCleanup,
        "apps.proxy.tasks.fetch_channel_stats" => Family::ProxyStats,
        "core.tasks.check_for_version_update" => Family::VersionCheck,
        "apps.m3u.tasks.check_account_expirations" => Family::AccountExpiration,
        _ => return None,
    })
}
