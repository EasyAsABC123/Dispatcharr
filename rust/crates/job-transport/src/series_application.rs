//! Prepared, explicit no-recording Series lane. Original Dispatcharr authors
//! retain full application/algorithm credit. Preparing opens no intake.
use crate::{
    series_driver::{SeriesDriver, State},
    series_worker::{Error, SeriesWorker},
    transport::{Config, Jobs},
};
use dispatcharr_coordination::{TaskLockConfig, TaskLocks};
use dispatcharr_ingest_epg::cache_policy::CachedSwapPolicy;
use dispatcharr_store::Store;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
pub struct Options {
    pub broker: String,
    pub backend: String,
    pub cache: String,
    pub poll_interval: Duration,
}
struct Inner {
    jobs: Jobs,
    worker: SeriesWorker,
    driver: Mutex<Option<SeriesDriver>>,
    closed: AtomicBool,
    poll_interval: Duration,
}
#[derive(Clone)]
pub struct SeriesApplication {
    inner: Arc<Inner>,
}
impl SeriesApplication {
    pub async fn prepare(store: Store, options: Options) -> Result<Self, &'static str> {
        if !(Duration::from_millis(25)..=Duration::from_secs(5)).contains(&options.poll_interval) {
            return Err("Invalid Series polling interval");
        }
        let jobs = Jobs::connect_series(
            &options.broker,
            &options.backend,
            Config {
                operations: 2,
                command_timeout: Duration::from_secs(2),
                max_queued_jobs: 10000,
            },
        )
        .await
        .map_err(|_| "Series broker initialization failed")?;
        let locks = TaskLocks::connect(&options.cache, TaskLockConfig::default())
            .await
            .map_err(|_| "Series global lock initialization failed")?;
        let policy = Arc::new(CachedSwapPolicy::new(store, Some(options.cache)));
        let worker = SeriesWorker::new(jobs.clone(), locks, policy)
            .map_err(|_| "Invalid Series worker authority")?;
        Ok(Self {
            inner: Arc::new(Inner {
                jobs,
                worker,
                driver: Mutex::new(None),
                closed: AtomicBool::new(false),
                poll_interval: options.poll_interval,
            }),
        })
    }
    pub fn start(&self) -> Result<(), Error> {
        let mut driver = self.inner.driver.lock().unwrap();
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        if driver.is_some() {
            return Err(Error::Configuration);
        }
        *driver = Some(SeriesDriver::start(
            self.inner.worker.clone(),
            self.inner.poll_interval,
        )?);
        Ok(())
    }
    pub fn state(&self) -> Option<State> {
        self.inner
            .driver
            .lock()
            .unwrap()
            .as_ref()
            .map(SeriesDriver::state)
    }
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), Error> {
        let deadline = tokio::time::Instant::now() + timeout;
        self.inner.closed.store(true, Ordering::Release);
        let driver = self.inner.driver.lock().unwrap().clone();
        if let Some(driver) = driver {
            driver
                .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
                .await?;
        } else {
            self.inner
                .worker
                .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
                .await?;
        }
        self.inner
            .jobs
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
            .map_err(Error::Transport)
    }
}
