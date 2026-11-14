//! Prepared per-channel EPG consumer in the application's existing EPG lane.
//! Original Dispatcharr authors retain application and algorithm credit.
//! Unsupported frames stay at the FIFO head; shutdown retains physical owners.
use crate::{
    transport::{Config, TvgJobs},
    tvg_programme_cache::Cache,
    tvg_programme_files::LocalFiles,
    tvg_programme_worker::{Error, Run, Worker},
};
use dispatcharr_coordination::{TaskLockConfig, TaskLocks};
use dispatcharr_ingest_epg::cache_policy::CachedSwapPolicy;
use dispatcharr_store::Store;
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::watch;

pub struct Options {
    pub broker: String,
    pub backend: String,
    pub cache: String,
    pub input_roots: Vec<PathBuf>,
    pub input_uid: u32,
}
#[derive(Clone, Debug)]
pub enum State {
    Prepared,
    Starting,
    Observed(Arc<Run>),
    Fault(Arc<Error>),
    Stopped,
}
struct Lifecycle {
    started: bool,
    closed: bool,
}
struct Inner {
    worker: Worker,
    jobs: TvgJobs,
    lifecycle: Mutex<Lifecycle>,
    stop: watch::Sender<bool>,
    done: watch::Sender<bool>,
    state: watch::Sender<State>,
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
    _lifetime: Arc<Lifetime>,
}
impl Application {
    /// Connections and local roots are prepared before any queue intake starts.
    pub async fn prepare(store: Store, options: Options) -> Result<Self, &'static str> {
        let files = LocalFiles::prepare(options.input_roots, options.input_uid)
            .map_err(|_| "Invalid per-channel EPG input roots")?;
        let jobs = TvgJobs::connect(&options.broker, &options.backend, Config::default())
            .await
            .map_err(|_| "Per-channel EPG broker initialization failed")?;
        let locks = TaskLocks::connect(&options.cache, TaskLockConfig::default())
            .await
            .map_err(|_| "Per-channel EPG locks initialization failed")?;
        let cache = Cache::connect(&options.cache)
            .await
            .map_err(|_| "Per-channel EPG cache initialization failed")?;
        let policy = Arc::new(CachedSwapPolicy::new(store.clone(), Some(options.cache)));
        let worker = Worker::prepare(jobs.clone(), store, locks, cache, Arc::new(files), policy);
        let stop = watch::channel(false).0;
        Ok(Self {
            inner: Arc::new(Inner {
                worker,
                jobs,
                lifecycle: Mutex::new(Lifecycle {
                    started: false,
                    closed: false,
                }),
                stop: stop.clone(),
                done: watch::channel(true).0,
                state: watch::channel(State::Prepared).0,
            }),
            _lifetime: Arc::new(Lifetime(stop)),
        })
    }
    pub fn state(&self) -> State {
        self.inner.state.borrow().clone()
    }
    pub fn start(&self, owner: Arc<dyn Send + Sync>) -> Result<(), &'static str> {
        let mut lifecycle = self
            .inner
            .lifecycle
            .lock()
            .map_err(|_| "EPG lifecycle poisoned")?;
        if lifecycle.started || lifecycle.closed {
            return Err("EPG intake already started or closed");
        }
        lifecycle.started = true;
        self.inner.done.send_replace(false);
        self.inner.state.send_replace(State::Starting);
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let mut stop = inner.stop.subscribe();
            while !*stop.borrow() {
                match inner.worker.run_once(owner.clone()).await {
                    Ok(run) => {
                        inner.state.send_replace(State::Observed(Arc::new(run)));
                    }
                    Err(Error::Closed) if *stop.borrow() => break,
                    Err(error) => {
                        inner.state.send_replace(State::Fault(Arc::new(error)));
                        inner.done.send_replace(true);
                        return;
                    }
                }
                tokio::select! { _ = stop.changed() => {}, _ = tokio::time::sleep(Duration::from_millis(100)) => {} }
            }
            inner.state.send_replace(State::Stopped);
            inner.done.send_replace(true);
        });
        Ok(())
    }
    pub async fn shutdown(&self, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        {
            let Ok(mut lifecycle) = self.inner.lifecycle.lock() else {
                return false;
            };
            lifecycle.closed = true;
            self.inner.stop.send_replace(true);
        }
        let worker = self
            .inner
            .worker
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
            .is_ok();
        let mut done = self.inner.done.subscribe();
        let poller = tokio::time::timeout_at(deadline, async {
            loop {
                if *done.borrow_and_update() {
                    return true;
                }
                if done.changed().await.is_err() {
                    return false;
                }
            }
        })
        .await
        .unwrap_or(false);
        // A bounded logical timeout never closes a transport whose worker is
        // still physically alive. Its retained owner keeps that transport alive.
        if !worker || !poller {
            return false;
        }
        self.inner
            .jobs
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
            .is_ok()
    }
}
