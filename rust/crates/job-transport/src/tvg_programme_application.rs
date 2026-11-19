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

#[path = "normal_tvg_deferred.rs"]
mod deferred;

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
    /// Reserve this prepared application for the single normal queue shell;
    /// a cloned public handle cannot also start its legacy polling loop.
    pub fn into_normal_handler_with_deferred_download(
        self,
        jobs: crate::transport::Jobs,
        identity: dispatcharr_store::ProducerIdentitySource,
        revoked: crate::normal_revocation::Revoked,
        download: dispatcharr_store::xmltv_download::Downloader,
        staging: dispatcharr_store::tvg_programme_stage::Service,
    ) -> Result<Arc<dyn crate::milestone_dispatch::Handler>, &'static str> {
        self.inner
            .worker
            .bind_staging(staging)
            .map_err(|_| "normal TVG staging port unavailable")?;
        self.inner
            .worker
            .bind_download(download)
            .map_err(|_| "normal TVG download port unavailable")?;
        self.into_normal_handler_with_deferred(jobs, identity, revoked)
    }
    pub fn into_normal_handler_with_deferred(
        self,
        jobs: crate::transport::Jobs,
        identity: dispatcharr_store::ProducerIdentitySource,
        revoked: crate::normal_revocation::Revoked,
    ) -> Result<Arc<dyn crate::milestone_dispatch::Handler>, &'static str> {
        {
            let mut lifecycle = self
                .inner
                .lifecycle
                .lock()
                .map_err(|_| "normal TVG lifecycle poisoned")?;
            if lifecycle.started || lifecycle.closed {
                return Err("family already started/closed");
            }
            lifecycle.started = true;
        }
        deferred::HandlerImpl::prepare(self, jobs, identity, revoked)
    }
    pub fn into_normal_handler(
        self,
    ) -> Result<Arc<dyn crate::milestone_dispatch::Handler>, &'static str> {
        {
            let mut lifecycle = self
                .inner
                .lifecycle
                .lock()
                .map_err(|_| "normal family lifecycle poisoned")?;
            if lifecycle.started || lifecycle.closed {
                return Err("family already started/closed");
            }
            lifecycle.started = true;
            self.inner.jobs.enable_normal_intake();
        }
        Ok(Arc::new(NormalHandler { app: self }))
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
    /// No new programme roots are admitted after Main begins termination.
    pub fn stop_channel_publications(&self) {
        if let Ok(mut lifecycle) = self.inner.lifecycle.lock() {
            lifecycle.closed = true;
        }
    }

    pub fn channel_publication_ready(&self) -> bool {
        self.inner.lifecycle.lock().is_ok_and(|lifecycle| {
            channel_publication_ready(&lifecycle, &self.inner.state.borrow())
        })
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

fn channel_publication_ready(lifecycle: &Lifecycle, state: &State) -> bool {
    lifecycle.started && !lifecycle.closed && matches!(state, State::Observed(_))
}

impl dispatcharr_store::TvgProgrammePublisher for Application {
    fn ready(&self) -> bool {
        self.channel_publication_ready()
    }

    fn publish(
        &self,
        publication: dispatcharr_store::TvgProgrammePublication,
        owner: Arc<dyn Send + Sync>,
    ) -> dispatcharr_contracts::BoxFuture<'_, Result<(), dispatcharr_store::StoreError>> {
        Box::pin(async move {
            if !self.channel_publication_ready() {
                return Err(dispatcharr_store::StoreError::Unsupported(
                    "TVG programme publisher is not accepting requests",
                ));
            }
            self.inner
                .jobs
                .publish_channel_root(publication, owner)
                .await
                .map_err(|_| {
                    dispatcharr_store::StoreError::Unsupported(
                        "TVG programme publication did not acknowledge",
                    )
                })
        })
    }
}

struct NormalHandler {
    app: Application,
}
impl crate::milestone_dispatch::Lifecycle for NormalHandler {
    fn shutdown(
        &self,
        timeout: Duration,
    ) -> dispatcharr_contracts::BoxFuture<'_, Result<(), crate::milestone_dispatch::Disposition>>
    {
        Box::pin(async move {
            if self.app.shutdown(timeout).await {
                Ok(())
            } else {
                Err(crate::milestone_dispatch::Disposition::Operational(
                    "normal family drain incomplete".into(),
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
        let inner = self.app.inner.clone();
        match inner.worker.run_expected(expected, owner).await {
            Ok(Run::Empty) => Ok(D::Empty),
            Ok(Run::UnsupportedHead) => Ok(D::UnsupportedHead),
            Ok(Run::Completed(id)) => Ok(D::Completed(id)),
            Ok(Run::Failed(id)) => Ok(D::TaskFailed(id)),
            Err(crate::tvg_programme_worker::Error::Transport(
                crate::transport::Error::ClaimRace,
            )) => Ok(D::HeadChanged),
            Err(crate::tvg_programme_worker::Error::Capacity) => Ok(D::Capacity),
            Err(error) => Err(D::Operational(format!("normal TVG {error:?}"))),
        }
    }
}

#[cfg(test)]
mod publication_readiness_tests {
    use super::*;

    #[test]
    fn empty_broker_observation_enables_legacy_programme_publication_until_close() {
        let mut lifecycle = Lifecycle {
            started: true,
            closed: false,
        };
        assert!(!channel_publication_ready(&lifecycle, &State::Prepared));
        assert!(!channel_publication_ready(&lifecycle, &State::Starting));
        let empty = State::Observed(Arc::new(Run::Empty));
        assert!(channel_publication_ready(&lifecycle, &empty));
        lifecycle.closed = true;
        assert!(!channel_publication_ready(&lifecycle, &empty));
    }
}
