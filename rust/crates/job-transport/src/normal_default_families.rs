//! Real inert normal cleanup/account/recurring/statistics family construction.
//! Original Dispatcharr/Celery and dependency authors retain all credit.
use crate::{
    configured_recording_worker::{
        Assembly, ExecutionReport, Family as RecordingFamily, Run as RecordingRun,
        Worker as RecordingWorker,
    },
    default_domains::KnownDomains,
    default_protocol::{Kind, Message},
    default_worker::DefaultWorker,
    milestone_dispatch::{Disposition, Family, Handler, Lifecycle},
    recording_artwork::Publisher,
    transport::{Config, Jobs, StatsJobs},
    ArtworkCompletions,
};
use dispatcharr_contracts::{BoxFuture, UpdateEventSink};
use dispatcharr_coordination::{TaskLockConfig, TaskLocks};
use dispatcharr_store::Store;
use std::{sync::Arc, time::Duration};
#[derive(Debug)]
pub enum Error {
    Configuration,
    Transport,
}
enum Ports {
    Default(Jobs),
    Stats(StatsJobs),
}
struct FamilyBundle {
    worker: Arc<dyn Handler>,
    ports: Ports,
    stop: Option<Arc<dyn Fn() + Send + Sync>>,
}
impl Handler for FamilyBundle {
    fn supports_selected_head(&self) -> bool {
        self.worker.supports_selected_head()
    }
    fn run_selected(
        &self,
        expected: Arc<[u8]>,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
        self.worker.run_selected(expected, owner)
    }
    fn run_once(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
        self.worker.run_once(owner)
    }
}
impl Lifecycle for FamilyBundle {
    fn stop_admission(&self) {
        if let Some(stop) = &self.stop {
            stop();
        }
        self.worker.stop_admission();
    }
    fn shutdown(&self, timeout: Duration) -> BoxFuture<'_, Result<(), Disposition>> {
        Box::pin(async move {
            let deadline = tokio::time::Instant::now() + timeout;
            self.worker.shutdown(timeout).await?;
            match &self.ports {
                Ports::Default(jobs) => {
                    jobs.shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
                        .await
                }
                Ports::Stats(jobs) => {
                    jobs.shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
                        .await
                }
            }
            .map_err(|e| Disposition::Operational(format!("normal family transport {e:?}")))
        })
    }
}
/// Real resources and handlers only. This starts neither intake nor scheduling.
pub struct PreparedDefaults {
    cleanup: Arc<dyn Handler>,
    accounts: Arc<dyn Handler>,
    recurring: Arc<dyn Handler>,
}
impl PreparedDefaults {
    pub fn entries(&self) -> Vec<(Family, Arc<dyn Handler>)> {
        vec![
            (Family::BackendCleanup, self.cleanup.clone()),
            (Family::AccountExpiration, self.accounts.clone()),
            (Family::Recurring, self.recurring.clone()),
        ]
    }
}
/// Prepare the retained cleanup handler for a registry that owns the Jobs port.
/// Stopping the handler drains its own task admission. The registry drains
/// the shared transport after all handlers and the scheduler have stopped.
pub fn prepare_cleanup(jobs: Jobs, store: &Store) -> Result<Arc<dyn Handler>, Error> {
    let domains = Arc::new(KnownDomains::new(store.pool().clone()));
    let worker = DefaultWorker::new_normal_family(jobs, domains, Kind::RedisBackendCleanup)
        .map_err(|_| Error::Configuration)?;
    Ok(Arc::new(NormalWorker::Default(worker)))
}

pub async fn prepare_defaults(
    store: Store,
    events: Arc<dyn UpdateEventSink>,
    broker: &str,
    backend: &str,
    cache: &str,
    completions: ArtworkCompletions,
) -> Result<PreparedDefaults, Error> {
    let domains = Arc::new(KnownDomains::with_active_notifications(
        store.pool().clone(),
        events.clone(),
    ));
    let cleanup_jobs = Jobs::connect_defaults(broker, backend, Config::default())
        .await
        .map_err(|_| Error::Transport)?;
    let cleanup = prepare_cleanup(cleanup_jobs.clone(), &store)?;
    let account_jobs = Jobs::connect_defaults(broker, backend, Config::default())
        .await
        .map_err(|_| Error::Transport)?;
    let accounts = DefaultWorker::new_normal_family(
        account_jobs.clone(),
        domains,
        Kind::InactiveAccountNotifications,
    )
    .map_err(|_| Error::Configuration)?;
    let jobs = Jobs::connect_defaults(broker, backend, Config::default())
        .await
        .map_err(|_| Error::Transport)?;
    let locks = TaskLocks::connect(cache, TaskLockConfig::default())
        .await
        .map_err(|_| Error::Transport)?;
    let publisher = Arc::new(
        Publisher::connect(broker, backend, Config::default())
            .await
            .map_err(|_| Error::Transport)?
            .retain_normal_publications(),
    );
    let worker = RecordingWorker::new(Assembly {
        jobs: jobs.clone(),
        locks,
        store,
        cache: cache.to_owned(),
        artwork: publisher.clone(),
        events,
        family: RecordingFamily::Recurring,
    })
    .map_err(|_| Error::Configuration)?;
    let recurring = Arc::new(RecurringHandler {
        worker,
        jobs,
        publisher,
        completions,
    });
    let account_stop = accounts.clone();
    Ok(PreparedDefaults {
        cleanup: Arc::new(FamilyBundle {
            worker: cleanup,
            ports: Ports::Default(cleanup_jobs),
            stop: None,
        }),
        accounts: Arc::new(FamilyBundle {
            worker: Arc::new(NormalWorker::Default(accounts)),
            ports: Ports::Default(account_jobs),
            stop: Some(Arc::new(move || account_stop.request_stop())),
        }),
        recurring,
    })
}
/// Production app provides the actual retained live-statistics Source and its
/// existing realtime sink. No synthetic Empty source or helper process exists.
pub async fn prepare_stats(
    broker: &str,
    backend: &str,
    source: Arc<dyn crate::stats_worker::Source>,
    events: Arc<dyn UpdateEventSink>,
    maximum: usize,
) -> Result<Arc<dyn Handler>, Error> {
    prepare_stats_with_inline(broker, backend, source, events, maximum)
        .await
        .map(|(handler, _)| handler)
}

/// The handler and scanner's inline Stats capability share one admitted worker.
pub async fn prepare_stats_with_inline(
    broker: &str,
    backend: &str,
    source: Arc<dyn crate::stats_worker::Source>,
    events: Arc<dyn UpdateEventSink>,
    maximum: usize,
) -> Result<(Arc<dyn Handler>, crate::stats_worker::StatsWorker), Error> {
    let jobs = StatsJobs::connect(broker, backend, Config::default())
        .await
        .map_err(|_| Error::Transport)?;
    let worker = crate::stats_worker::StatsWorker::new(jobs.clone(), source, events, maximum)
        .map_err(|_| Error::Configuration)?;
    let handler = Arc::new(FamilyBundle {
        worker: Arc::new(NormalWorker::Stats(worker.clone())),
        ports: Ports::Stats(jobs),
        stop: None,
    });
    Ok((handler, worker))
}
struct RecurringHandler {
    worker: RecordingWorker,
    jobs: Jobs,
    publisher: Arc<Publisher>,
    completions: ArtworkCompletions,
}
impl Handler for RecurringHandler {
    fn supports_selected_head(&self) -> bool {
        true
    }
    fn run_once(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
        Box::pin(self.execute(None, owner))
    }
    fn run_selected(
        &self,
        expected: Arc<[u8]>,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
        Box::pin(self.execute(Some(expected), owner))
    }
}
impl RecurringHandler {
    async fn execute(
        &self,
        expected: Option<Arc<[u8]>>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Disposition, Disposition> {
        let (run, report) = self
            .worker
            .run_expected(expected, owner.clone())
            .await
            .map_err(|e| match e {
                crate::configured_recording_worker::Error::Transport(
                    crate::transport::Error::ClaimRace,
                ) => Disposition::HeadChanged,
                crate::configured_recording_worker::Error::Capacity => Disposition::Capacity,
                other => Disposition::Operational(format!("normal recurring {other:?}")),
            })?;
        if let Some(ExecutionReport::Recurring(report)) = report {
            let id = report.task_id();
            let backend = self
                .jobs
                .read_recurring_lineage(id, owner)
                .await
                .map_err(|e| Disposition::Operational(format!("recurring result {e:?}")))?
                .ok_or_else(|| Disposition::Operational("recurring result missing".into()))?;
            self.completions
                .verify_recurring(
                    Message {
                        task_id: id,
                        kind: Kind::RecurringMaintenance,
                        expires: None,
                    },
                    report,
                    &backend,
                )
                .map_err(|e| {
                    Disposition::Operational(format!("recurring artwork provenance {e:?}"))
                })?;
        }
        Ok(match run {
            RecordingRun::Empty => Disposition::Empty,
            RecordingRun::UnsupportedHead => Disposition::UnsupportedHead,
            RecordingRun::Completed(id) | RecordingRun::AlreadySucceeded(id) => {
                Disposition::Completed(id)
            }
        })
    }
}
impl Lifecycle for RecurringHandler {
    fn stop_admission(&self) {
        self.completions.close_admission();
    }
    fn shutdown(&self, timeout: Duration) -> BoxFuture<'_, Result<(), Disposition>> {
        Box::pin(async move {
            let deadline = tokio::time::Instant::now() + timeout;
            self.worker
                .shutdown(timeout)
                .await
                .map_err(|e| Disposition::Operational(format!("recurring drain {e:?}")))?;
            self.publisher
                .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
                .await
                .map_err(|e| {
                    Disposition::Operational(format!("recurring publisher drain {e:?}"))
                })?;
            self.jobs
                .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
                .await
                .map_err(|e| Disposition::Operational(format!("recurring ports drain {e:?}")))
        })
    }
}

enum NormalWorker {
    Default(DefaultWorker),
    Stats(crate::stats_worker::StatsWorker),
}
impl Handler for NormalWorker {
    fn supports_selected_head(&self) -> bool {
        true
    }
    fn run_once(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
        Box::pin(self.execute(None, owner))
    }
    fn run_selected(
        &self,
        expected: Arc<[u8]>,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
        Box::pin(self.execute(Some(expected), owner))
    }
}
impl NormalWorker {
    async fn execute(
        &self,
        expected: Option<Arc<[u8]>>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Disposition, Disposition> {
        match self {
            Self::Default(worker) => {
                use crate::default_worker::{Error, Run};
                match worker.run_expected(expected, owner).await {
                    Ok(Run::Empty) => Ok(Disposition::Empty),
                    Ok(Run::UnsupportedHead) => Ok(Disposition::UnsupportedHead),
                    Ok(Run::Unsupported(_)) => Ok(Disposition::UnsupportedDomain),
                    Ok(Run::Completed(id) | Run::AlreadySucceeded(id)) => {
                        Ok(Disposition::Completed(id))
                    }
                    Ok(Run::Failed(id)) => Ok(Disposition::TaskFailed(id)),
                    Ok(Run::Revoked(id, _)) => Ok(Disposition::Revoked(id)),
                    Err(Error::Capacity) => Ok(Disposition::Capacity),
                    Err(Error::Transport(crate::transport::Error::ClaimRace)) => {
                        Ok(Disposition::HeadChanged)
                    }
                    Err(error) => Err(Disposition::Operational(format!(
                        "normal default {error:?}"
                    ))),
                }
            }
            Self::Stats(worker) => {
                use crate::stats_worker::{Error, Run};
                match worker.run_expected(expected, owner).await {
                    Ok(Run::Empty) => Ok(Disposition::Empty),
                    Ok(Run::Completed(id) | Run::AlreadySucceeded(id)) => {
                        Ok(Disposition::Completed(id))
                    }
                    Err(Error::Capacity) => Ok(Disposition::Capacity),
                    Err(Error::Transport(crate::transport::Error::ClaimRace)) => {
                        Ok(Disposition::HeadChanged)
                    }
                    Err(Error::Transport(crate::transport::Error::UnsupportedTask)) => {
                        Ok(Disposition::UnsupportedHead)
                    }
                    Err(error) => Err(Disposition::Operational(format!("normal stats {error:?}"))),
                }
            }
        }
    }
}
impl Lifecycle for NormalWorker {
    fn stop_admission(&self) {
        if let Self::Default(worker) = self {
            worker.request_stop();
        }
    }
    fn shutdown(&self, timeout: Duration) -> BoxFuture<'_, Result<(), Disposition>> {
        Box::pin(async move {
            let settled = match self {
                Self::Default(worker) => worker.shutdown(timeout).await,
                Self::Stats(worker) => worker.shutdown(timeout).await,
            };
            if settled {
                Ok(())
            } else {
                Err(Disposition::Operational("normal family drain".into()))
            }
        })
    }
}
