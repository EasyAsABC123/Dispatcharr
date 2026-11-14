//! PRIVATE UNCOMPILED composition proposal, not Main/default registration.
//! Original Dispatcharr/Celery authors retain all application/protocol credit.
//! One bounded queue head is selected without removal/reordering. Each handler
//! still performs its own actual family/lineage/domain preflight and exact CAS.
use dispatcharr_contracts::BoxFuture;
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Family {
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
    Completed(uuid::Uuid),
    CompletedWithoutId,
    TaskFailedWithoutId,
    TaskFailed(uuid::Uuid),
    Revoked(uuid::Uuid),
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
    fn shutdown(&self, timeout: std::time::Duration) -> BoxFuture<'_, Result<(), Disposition>>;
}
pub trait Handler: Lifecycle + Send + Sync {
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
const REQUIRED: [Family; 14] = [
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
