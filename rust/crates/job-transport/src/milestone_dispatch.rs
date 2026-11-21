//! Experimental selected-family composition; complete Source parity is open.
//! The composed SDK batch passed; packaged normal Main acceptance is separate.
//! Original Dispatcharr/Celery authors retain all application/protocol credit.
//! One bounded queue head is selected without removal/reordering. Each handler
//! still performs its own actual family/lineage/domain preflight and exact CAS.
use dispatcharr_contracts::BoxFuture;
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Family {
    RegisteredPlugin,
    PluginRefresh,
    BackupCreate,
    BackupScheduled,
    XcAccountInfo,
    XcAccountProfiles,
    StartupRecovery,
    RehashStreams,
    RescheduleOffsets,
    Scanner,
    M3uGroups,
    M3uSingle,
    M3uAll,
    VodIngestion,
    AutoChannels,
    Refresh,
    ProgrammeIndex,
    TvgProgrammes,
    SdGuide,
    Series,
    Recurring,
    RecurringPurge,
    Artwork,
    Recording,
    BackendCleanup,
    ChannelBulk,
    ChannelNames,
    ChannelLogos,
    ChannelTvgIds,
    ChannelMatchAll,
    ChannelMatchSelected,
    ChannelMatchSingle,
    ComskipManual,
    ProxyStats,
    VersionCheck,
    AccountExpiration,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Disposition {
    Empty,
    NotDue,
    Capacity,
    Reserved(uuid::Uuid),
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
    /// Signal owned dependency deadlines BEFORE the normal shell joins tasks.
    /// This must not close shared ports or synthesize completion receipts.
    fn begin_drain(&self, _budget: std::time::Duration) {}

    fn shutdown(&self, timeout: std::time::Duration) -> BoxFuture<'_, Result<(), Disposition>>;
}
pub trait Handler: Lifecycle + Send + Sync {
    /// Bounded receipt recovery only; never executes application work.
    fn maintenance(&self, _owner: Arc<dyn Send + Sync>) -> BoxFuture<'_, Result<(), Disposition>> {
        Box::pin(async { Ok(()) })
    }

    /// Already durable local reservation, never a synthetic broker head. The
    /// normal Celery supervisor executes it within its SAME global task budget.
    fn due_head(&self) -> Option<Arc<[u8]>> {
        None
    }
    fn run_due(
        &self,
        _raw: Arc<[u8]>,
        _owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
        Box::pin(async { Err(Disposition::UnsupportedDomain) })
    }

    /// Wake hint only; selectors still re-read the actual raw head.
    fn head_advance_notifier(&self) -> Option<Arc<tokio::sync::Notify>> {
        None
    }
    /// Only the sealed loaded-callable handler overrides this. A prefix or
    /// metadata task name alone never grants registered-plugin execution.
    fn registered_head(&self, _raw: &[u8]) -> bool {
        false
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
    pub fn classify(&self, raw: &[u8]) -> Option<Family> {
        if self
            .handlers
            .get(&Family::RegisteredPlugin)
            .is_some_and(|handler| handler.registered_head(raw))
        {
            Some(Family::RegisteredPlugin)
        } else {
            classify(raw)
        }
    }
    pub async fn run_once(&self, owner: Arc<dyn Send + Sync>) -> Result<Disposition, Disposition> {
        let Some(raw) = self.head.peek(owner.clone()).await? else {
            return Ok(Disposition::Empty);
        };
        let family = self.classify(&raw).ok_or(Disposition::UnsupportedHead)?;
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
    builtin_family(frame.get("headers")?.get("task")?.as_str()?)
}

pub(crate) fn builtin_family(task: &str) -> Option<Family> {
    Some(match task {
        "apps.plugins.tasks.refresh_plugin_repos" => Family::PluginRefresh,
        "apps.backups.tasks.create_backup_task" | "apps.backups.tasks.restore_backup_task" => {
            Family::BackupCreate
        }
        "apps.backups.tasks.scheduled_backup_task" => Family::BackupScheduled,
        "apps.m3u.tasks.refresh_account_info" => Family::XcAccountInfo,
        "apps.m3u.tasks.refresh_account_profiles" => Family::XcAccountProfiles,
        "core.tasks.rehash_streams" => Family::RehashStreams,
        "apps.channels.tasks.reschedule_upcoming_recordings_for_offset_change" => {
            Family::RescheduleOffsets
        }
        "apps.channels.tasks.recover_recordings_on_startup" => Family::StartupRecovery,
        "core.tasks.scan_and_process_files" | crate::scanner_files_protocol::COMPAT_BEAT => {
            Family::Scanner
        }
        "apps.m3u.tasks.refresh_m3u_groups" => Family::M3uGroups,
        "apps.m3u.tasks.refresh_single_m3u_account" => Family::M3uSingle,
        "apps.m3u.tasks.refresh_m3u_accounts" => Family::M3uAll,
        "apps.vod.tasks.refresh_vod_content" => Family::VodIngestion,
        "apps.m3u.tasks.sync_auto_channels" => Family::AutoChannels,
        "apps.epg.tasks.refresh_epg_data" | crate::refresh_aggregate::NAME => Family::Refresh,
        "apps.epg.tasks.build_programme_index_task" => Family::ProgrammeIndex,
        "apps.epg.tasks.parse_programs_for_tvg_id" => Family::TvgProgrammes,
        "apps.epg.tasks.fetch_schedules_direct_stations"
        | "apps.epg.tasks.fetch_sd_mapped_guide_batch"
        | "apps.epg.tasks.fetch_sd_guide_for_epg" => Family::SdGuide,
        "apps.channels.tasks.evaluate_series_rules" => Family::Series,
        "apps.channels.tasks.maintain_recurring_recordings" => Family::Recurring,
        "apps.channels.tasks.purge_recurring_rule" => Family::RecurringPurge,
        "apps.channels.tasks.prefetch_recording_artwork" => Family::Artwork,
        "apps.channels.tasks.run_recording" => Family::Recording,
        "celery.backend_cleanup" => Family::BackendCleanup,
        "apps.channels.tasks.bulk_create_channels_from_streams" => Family::ChannelBulk,
        "apps.channels.tasks.set_channels_names_from_epg" => Family::ChannelNames,
        "apps.channels.tasks.set_channels_logos_from_epg" => Family::ChannelLogos,
        "apps.channels.tasks.set_channels_tvg_ids_from_epg" => Family::ChannelTvgIds,
        "apps.channels.tasks.match_epg_channels" => Family::ChannelMatchAll,
        "apps.channels.tasks.match_selected_channels_epg" => Family::ChannelMatchSelected,
        "apps.channels.tasks.match_single_channel_epg" => Family::ChannelMatchSingle,
        "apps.channels.tasks.comskip_process_recording" => Family::ComskipManual,
        "apps.proxy.tasks.fetch_channel_stats" => Family::ProxyStats,
        "core.tasks.check_for_version_update" => Family::VersionCheck,
        "apps.m3u.tasks.check_account_expirations" => Family::AccountExpiration,
        _ => return None,
    })
}
