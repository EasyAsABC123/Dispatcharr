//! Bounded index/current-file service assembly. Original Dispatcharr authors
//! retain application/algorithm credit and AGPL licensing. Intake starts only
//! after the application explicitly calls start(); workflow gates are separate.
use crate::{
    index_classifier::ClassifiedServiceExecutor,
    index_dispatch::ProgrammeIndexDispatcher,
    index_driver::{Config as DriverConfig, IndexDriver},
    index_worker::ServiceExecutor,
    transport::{Config as JobsConfig, Jobs},
};
use dispatcharr_coordination::{TaskLockConfig, TaskLocks};
use dispatcharr_ingest_epg::{
    current_lookup::{
        CurrentLookup, CurrentLookupConfig, CurrentLookupServices, SourcePathContext,
    },
    index_service::{IndexConfig, IndexDispatcher, IndexService, IndexServices},
    programme_index::IndexLimits,
    ImportError, SwapPolicyContext,
};
use dispatcharr_store::{CurrentFileLookup, Store};
use std::{
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

struct NoUnboundDispatch;
impl IndexDispatcher for NoUnboundDispatch {
    fn defer<'a>(
        &'a self,
        _: i64,
        _: u32,
        _: u32,
        _: SwapPolicyContext,
    ) -> Pin<Box<dyn Future<Output = Result<(), ImportError>> + Send + 'a>> {
        // ServiceExecutor supplies the immutable task's real ChildPublisher.
        Box::pin(async { Err(ImportError::Worker) })
    }
}

pub struct Options {
    pub broker: String,
    pub backend: String,
    pub cache: String,
    pub source_cwd: PathBuf,
    pub media_root: PathBuf,
    pub cache_dir: PathBuf,
}
#[derive(Debug)]
pub enum Error {
    Configuration,
    Transport,
    Service,
    Closed,
}
struct Inner {
    jobs: Jobs,
    dispatcher: ProgrammeIndexDispatcher,
    lookup: CurrentLookup,
    service: IndexService,
    driver: Mutex<Option<IndexDriver>>,
    normal_dispatch: AtomicBool,
    closed: AtomicBool,
}
#[derive(Clone)]
pub struct IndexApplication {
    inner: Arc<Inner>,
}
impl IndexApplication {
    /// Connect and validate without consuming a queue head. The application can
    /// finish initializing every producer/consumer before enabling intake.
    pub async fn connect(store: Store, options: Options) -> Result<Self, Error> {
        Self::with_clock(store, options, Arc::new(chrono::Utc::now)).await
    }
    /// Explicit clock dependency for Source-controlled integration captures.
    /// It changes neither queue ownership nor production intake authority.
    pub async fn with_clock(
        store: Store,
        options: Options,
        clock: dispatcharr_ingest_epg::cache_policy::PolicyClock,
    ) -> Result<Self, Error> {
        let context = SourcePathContext::new(options.source_cwd, options.media_root)
            .map_err(|_| Error::Configuration)?;
        if !options.cache_dir.is_absolute() || options.cache_dir.as_os_str().len() > 4096 {
            return Err(Error::Configuration);
        }
        let jobs = Jobs::connect_index(&options.broker, &options.backend, JobsConfig::default())
            .await
            .map_err(|_| Error::Transport)?;
        let locks = TaskLocks::connect(
            &options.cache,
            TaskLockConfig {
                max_leases: 2,
                max_commands: 4,
                ..Default::default()
            },
        )
        .await
        .map_err(|_| Error::Transport)?;
        let dispatcher =
            ProgrammeIndexDispatcher::new(jobs.clone(), 4).map_err(|_| Error::Configuration)?;
        let lookup = CurrentLookup::new(
            CurrentLookupServices {
                store: store.clone(),
                dispatcher: Arc::new(dispatcher.clone()),
                clock: clock.clone(),
            },
            CurrentLookupConfig {
                path_context: Some(context.clone()),
                ..Default::default()
            },
        )
        .map_err(|_| Error::Configuration)?;
        let service = IndexService::new(
            IndexServices {
                store,
                locks,
                dispatcher: Arc::new(NoUnboundDispatch),
                clock,
            },
            IndexConfig {
                path_context: Some(context),
                max_jobs: 2,
                limits: IndexLimits {
                    state_bytes: 8 * 1024 * 1024,
                    ..Default::default()
                },
                cache_dir: options.cache_dir,
                release_timeout: Duration::from_secs(5),
                max_index_json_bytes: 8 * 1024 * 1024,
                max_memory_bytes: 128 * 1024 * 1024,
            },
        )
        .map_err(|_| Error::Configuration)?;
        Ok(Self {
            inner: Arc::new(Inner {
                jobs,
                dispatcher,
                lookup,
                service,
                driver: Mutex::new(None),
                normal_dispatch: AtomicBool::new(false),
                closed: AtomicBool::new(false),
            }),
        })
    }
    pub fn into_normal_handler(
        self,
    ) -> Result<Arc<dyn crate::milestone_dispatch::Handler>, &'static str> {
        {
            let driver = self
                .inner
                .driver
                .lock()
                .map_err(|_| "normal lifecycle poisoned")?;
            if driver.is_some()
                || self.inner.closed.load(Ordering::Acquire)
                || self.inner.normal_dispatch.swap(true, Ordering::AcqRel)
            {
                return Err("family already started/closed");
            }
        }
        let config = DriverConfig::default();
        self.inner
            .jobs
            .eta_pool(config.reservations)
            .map_err(|_| "normal reservation pool")?;
        let worker = crate::scheduled_index::ScheduledIndexWorker::new(
            self.inner.jobs.clone(),
            Arc::new(ClassifiedServiceExecutor::new(self.inner.service.clone())),
            config.scheduling,
        )
        .map_err(|_| "normal scheduled worker")?;
        Ok(Arc::new(NormalHandler {
            app: self,
            worker,
            limits: config.reservations,
        }))
    }
    pub fn start(&self, owner: Arc<dyn Send + Sync>) -> Result<(), Error> {
        let mut driver = self.inner.driver.lock().unwrap();
        if self.inner.closed.load(Ordering::Acquire)
            || self.inner.normal_dispatch.load(Ordering::Acquire)
        {
            return Err(Error::Closed);
        }
        if driver.is_some() {
            return Err(Error::Configuration);
        }
        *driver = Some(
            IndexDriver::start(
                self.inner.jobs.clone(),
                Arc::new(ServiceExecutor::new(self.inner.service.clone())),
                DriverConfig::default(),
                owner,
            )
            .map_err(|_| Error::Service)?,
        );
        Ok(())
    }
    pub fn lookup(&self) -> Arc<dyn CurrentFileLookup> {
        Arc::new(self.inner.lookup.clone())
    }
    pub fn physical_lookup_jobs(&self) -> usize {
        self.inner.lookup.physical_jobs()
    }
    pub fn retained_lookup_memory_bytes(&self) -> usize {
        self.inner.lookup.retained_memory_bytes()
    }
    pub fn last_execution_error(&self) -> Option<crate::index_worker::Error> {
        self.inner
            .driver
            .lock()
            .unwrap()
            .as_ref()
            .and_then(IndexDriver::last_execution_error)
    }
    /// Stop/drain the worker while keeping the publication transport alive for
    /// HTTP reads already accepted before application shutdown.
    pub async fn drain_intake(&self, timeout: Duration) -> bool {
        let driver = self.inner.driver.lock().unwrap().clone();
        match driver {
            Some(driver) => driver.shutdown(timeout).await.is_ok(),
            None => true,
        }
    }
    /// The HTTP read service must stop/drain first, so accepted read callbacks
    /// can publish their missing-index work before this dispatcher is closed.
    pub async fn shutdown(&self, timeout: Duration) -> bool {
        self.inner.closed.store(true, Ordering::Release);
        let deadline = tokio::time::Instant::now() + timeout;
        let left = || deadline.saturating_duration_since(tokio::time::Instant::now());
        let lookup = self.inner.lookup.shutdown(left()).await;
        let publication = self.inner.dispatcher.shutdown(left()).await.is_ok();
        let driver = self.inner.driver.lock().unwrap().clone();
        let intake = match driver {
            Some(driver) => driver.shutdown(left()).await.is_ok(),
            None => true,
        };
        let service = self.inner.service.shutdown(left()).await;
        // Keep transport ownership alive if physical work is still pending.
        if !(lookup && publication && intake && service) {
            return false;
        }
        self.inner.jobs.shutdown(left()).await.is_ok()
    }
}

struct NormalHandler {
    app: IndexApplication,
    worker: crate::scheduled_index::ScheduledIndexWorker,
    limits: crate::eta::Limits,
}
impl crate::milestone_dispatch::Lifecycle for NormalHandler {
    fn stop_admission(&self) {
        self.worker.stop_admission();
    }
    fn shutdown(
        &self,
        timeout: Duration,
    ) -> dispatcharr_contracts::BoxFuture<'_, Result<(), crate::milestone_dispatch::Disposition>>
    {
        Box::pin(async move {
            let deadline = tokio::time::Instant::now() + timeout;
            self.worker.shutdown(timeout).await.map_err(|error| {
                crate::milestone_dispatch::Disposition::Operational(format!(
                    "scheduled family drain {error:?}"
                ))
            })?;
            if self
                .app
                .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
                .await
            {
                Ok(())
            } else {
                Err(crate::milestone_dispatch::Disposition::Operational(
                    "normal app drain incomplete".into(),
                ))
            }
        })
    }
}
impl crate::milestone_dispatch::Handler for NormalHandler {
    fn supports_selected_head(&self) -> bool {
        true
    }
    fn run_once(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> dispatcharr_contracts::BoxFuture<
        '_,
        Result<crate::milestone_dispatch::Disposition, crate::milestone_dispatch::Disposition>,
    > {
        Box::pin(self.execute(None, owner))
    }
    fn run_selected(
        &self,
        expected: Arc<[u8]>,
        owner: Arc<dyn Send + Sync>,
    ) -> dispatcharr_contracts::BoxFuture<
        '_,
        Result<crate::milestone_dispatch::Disposition, crate::milestone_dispatch::Disposition>,
    > {
        Box::pin(self.execute(Some(expected), owner))
    }
}
impl NormalHandler {
    async fn execute(
        &self,
        expected: Option<Arc<[u8]>>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<crate::milestone_dispatch::Disposition, crate::milestone_dispatch::Disposition>
    {
        use crate::milestone_dispatch::Disposition as D;
        let candidate = match self
            .app
            .inner
            .jobs
            .peek_index_candidate(owner.clone(), self.limits)
            .await
        {
            Ok(Some(value)) => value,
            Ok(None) => {
                return Ok(if expected.is_some() {
                    D::HeadChanged
                } else {
                    D::Empty
                })
            }
            Err(crate::transport::Error::UnsupportedTask) => return Ok(D::UnsupportedHead),
            Err(error) => return Err(D::Operational(format!("normal scheduled peek {error:?}"))),
        };
        if expected
            .as_ref()
            .is_some_and(|selected| selected.as_ref() != candidate.receipt.queued_bytes.as_slice())
        {
            return Ok(D::HeadChanged);
        }
        let receipt = self
            .app
            .inner
            .jobs
            .reserve_index_candidate_settled(candidate, owner.clone(), self.limits)
            .await
            .map_err(|error| match error {
                crate::transport::Error::ClaimRace => D::HeadChanged,
                crate::transport::Error::Capacity => D::Capacity,
                other => D::Operational(format!("normal reservation {other:?}")),
            })?;
        let outcome = self
            .worker
            .start(&receipt, owner)
            .map_err(|error| D::Operational(format!("normal scheduled admission {error:?}")))?
            .wait()
            .await
            .map_err(|error| D::Operational(format!("normal scheduled execution {error:?}")))?;
        Ok(match outcome {
            crate::scheduled_index::Outcome::Completed(id)
            | crate::scheduled_index::Outcome::AlreadySucceeded(id) => D::Completed(id),
            crate::scheduled_index::Outcome::Restored(id) => D::Restored(id),
        })
    }
}
