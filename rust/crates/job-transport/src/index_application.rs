//! Bounded index/current-file service assembly. Original Dispatcharr authors
//! retain application/algorithm credit and AGPL licensing. Intake starts only
//! after the application explicitly calls start(); workflow gates are separate.
use crate::{
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
                closed: AtomicBool::new(false),
            }),
        })
    }
    pub fn start(&self, owner: Arc<dyn Send + Sync>) -> Result<(), Error> {
        let mut driver = self.inner.driver.lock().unwrap();
        if self.inner.closed.load(Ordering::Acquire) {
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
