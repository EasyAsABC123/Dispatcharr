//! Normal configured Series consumer, including completed Refresh descendants.
//! Original Dispatcharr authors retain application/algorithm/license credit.
//! No source-parent reconstruction, artwork fallback or unqualified FIFO skip.
use crate::{
    configured_recording_worker,
    configured_recording_worker::{Assembly, Error, Family, Run, Worker},
    recording_artwork::Publisher,
    refresh_series_handoff::Handoff,
    transport::{Config, Error as TransportError, Jobs},
};
use dispatcharr_contracts::UpdateEventSink;
use dispatcharr_coordination::{TaskLockConfig, TaskLocks};
use dispatcharr_store::Store;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::watch;

pub struct Options {
    pub broker: String,
    pub backend: String,
    pub cache: String,
    pub poll_interval: Duration,
}
#[derive(Clone, Debug)]
pub enum State {
    Prepared,
    Starting,
    Empty,
    UnsupportedHead,
    WaitingForCompletedParent,
    Observed(Arc<Run>),
    Fault(Arc<Error>),
    Stopped,
}
struct Lifecycle {
    started: bool,
    closed: bool,
}
struct Inner {
    jobs: Jobs,
    locks: TaskLocks,
    store: Store,
    cache: String,
    artwork: Arc<Publisher>,
    events: Arc<dyn UpdateEventSink>,
    handoff: Handoff,
    completions: Option<crate::ArtworkCompletions>,
    direct: Worker,
    poll: Duration,
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
struct Caller {
    _application: Arc<Inner>,
}
#[derive(Clone)]
pub struct Application {
    inner: Arc<Inner>,
    _lifetime: Arc<Lifetime>,
}
impl Inner {
    fn assembly(&self) -> Assembly {
        Assembly {
            jobs: self.jobs.clone(),
            locks: self.locks.clone(),
            store: self.store.clone(),
            cache: self.cache.clone(),
            artwork: self.artwork.clone(),
            events: self.events.clone(),
            family: Family::Series,
        }
    }
}
impl Application {
    pub async fn prepare(
        store: Store,
        events: Arc<dyn UpdateEventSink>,
        handoff: Handoff,
        options: Options,
    ) -> Result<Self, &'static str> {
        Self::prepare_inner(store, events, handoff, options, None).await
    }
    /// Explicit normal Main artwork composition. Legacy prepare remains unchanged.
    pub async fn prepare_with_artwork(
        store: Store,
        events: Arc<dyn UpdateEventSink>,
        handoff: Handoff,
        options: Options,
        completions: crate::ArtworkCompletions,
    ) -> Result<Self, &'static str> {
        Self::prepare_inner(store, events, handoff, options, Some(completions)).await
    }
    async fn prepare_inner(
        store: Store,
        events: Arc<dyn UpdateEventSink>,
        handoff: Handoff,
        options: Options,
        completions: Option<crate::ArtworkCompletions>,
    ) -> Result<Self, &'static str> {
        if !(Duration::from_millis(25)..=Duration::from_secs(5)).contains(&options.poll_interval) {
            return Err("Invalid configured Series poll interval");
        }
        let jobs = Jobs::connect_series(&options.broker, &options.backend, Config::default())
            .await
            .map_err(|_| "Configured Series broker initialization failed")?;
        let locks = TaskLocks::connect(&options.cache, TaskLockConfig::default())
            .await
            .map_err(|_| "Configured Series locks initialization failed")?;
        let publisher = Publisher::connect(&options.broker, &options.backend, Config::default())
            .await
            .map_err(|_| "Configured Series artwork publisher initialization failed")?;
        let artwork = Arc::new(if completions.is_some() {
            publisher.retain_normal_publications()
        } else {
            publisher
        });
        let assembly = Assembly {
            jobs: jobs.clone(),
            locks: locks.clone(),
            store: store.clone(),
            cache: options.cache.clone(),
            artwork: artwork.clone(),
            events: events.clone(),
            family: Family::Series,
        };
        let direct = if completions.is_some() {
            Worker::new_normal(assembly)
        } else {
            Worker::new(assembly)
        }
        .map_err(|_| "Configured Series worker initialization failed")?;
        let stop = watch::channel(false).0;
        Ok(Self {
            inner: Arc::new(Inner {
                jobs,
                locks,
                store,
                cache: options.cache,
                artwork,
                events,
                handoff,
                completions,
                direct,
                poll: options.poll_interval,
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
        }
        Ok(Arc::new(NormalHandler { app: self }))
    }
    pub fn state(&self) -> State {
        self.inner.state.borrow().clone()
    }
    pub fn start(&self) -> Result<(), &'static str> {
        let mut lifecycle = self
            .inner
            .lifecycle
            .lock()
            .map_err(|_| "Configured Series lifecycle poisoned")?;
        if lifecycle.started || lifecycle.closed {
            return Err("Configured Series intake already started or closed");
        }
        lifecycle.started = true;
        self.inner.done.send_replace(false);
        self.inner.state.send_replace(State::Starting);
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let mut stop = inner.stop.subscribe();
            while !*stop.borrow() {
                let owner: Arc<dyn Send + Sync> = Arc::new(Caller {
                    _application: inner.clone(),
                });
                match run(&inner, owner).await {
                    Ok(state) => {
                        inner.state.send_replace(state);
                    }
                    Err(error) => {
                        inner.state.send_replace(State::Fault(Arc::new(error)));
                        inner.done.send_replace(true);
                        return;
                    }
                }
                tokio::select! { _=stop.changed()=>{}, _=tokio::time::sleep(inner.poll)=>{} }
            }
            inner.state.send_replace(State::Stopped);
            inner.done.send_replace(true);
        });
        Ok(())
    }
    /// Stop all Series reads/effects before the Refresh app closes its shared
    /// parent metadata reader. Timeout leaves the actual caller/ports retained.
    pub async fn drain_intake(&self, timeout: Duration) -> bool {
        {
            let Ok(mut lifecycle) = self.inner.lifecycle.lock() else {
                return false;
            };
            lifecycle.closed = true;
            self.inner.stop.send_replace(true);
        }
        let mut done = self.inner.done.subscribe();
        tokio::time::timeout(timeout, async {
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
        .unwrap_or(false)
    }
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), Error> {
        let deadline = tokio::time::Instant::now() + timeout;
        if !self.drain_intake(timeout).await {
            return Err(Error::DrainTimeout);
        }
        let left = || deadline.saturating_duration_since(tokio::time::Instant::now());
        self.inner.direct.shutdown(left()).await?;
        self.inner
            .artwork
            .shutdown(left())
            .await
            .map_err(Error::Transport)?;
        self.inner
            .jobs
            .shutdown(left())
            .await
            .map_err(Error::Transport)
    }
}
async fn run(inner: &Arc<Inner>, owner: Arc<dyn Send + Sync>) -> Result<State, Error> {
    let message = match inner.jobs.peek_series(owner.clone()).await {
        Ok(Some((_, message))) => message,
        Ok(None) => return Ok(State::Empty),
        Err(TransportError::UnsupportedTask) => return Ok(State::UnsupportedHead),
        Err(error) => return Err(Error::Transport(error)),
    };
    if message.parent.is_none() {
        if let Some(completions) = &inner.completions {
            let (run, report) = inner.direct.run_once_completed(owner.clone()).await?;
            if let Some(report) = report {
                let backend = inner
                    .jobs
                    .read_series_lineage(&message, owner)
                    .await
                    .map_err(Error::Transport)?
                    .ok_or(Error::Transport(TransportError::Protocol))?;
                completions
                    .record_direct(message, report, backend)
                    .await
                    .map_err(Error::Transport)?;
            }
            return Ok(State::Observed(Arc::new(run)));
        }
        return inner
            .direct
            .run_once(owner)
            .await
            .map(|run| State::Observed(Arc::new(run)));
    }
    let Some(pending) = inner
        .handoff
        .completed_parent(&message, owner.clone())
        .await
        .map_err(Error::Transport)?
    else {
        return Ok(State::WaitingForCompletedParent);
    };
    // Capability is derived from the original retained parent and actual
    // completed backend, not from an arbitrary parented envelope alone.
    let worker = if inner.completions.is_some() {
        Worker::new_normal_refresh_series_child(inner.assembly(), pending.child)
    } else {
        Worker::new_refresh_series_child(inner.assembly(), pending.child)
    }?;
    let result = worker.run_once_completed(owner.clone()).await;
    // This worker is scoped to one capability. Do not detach physical work on
    // an observer result; its one slot must actually drain before handoff removal.
    worker.shutdown(Duration::from_secs(4)).await?;
    let (run, report) = result?;
    if let (Some(completions), Some(report)) = (&inner.completions, report) {
        let backend = inner
            .jobs
            .read_series_lineage(&message, owner)
            .await
            .map_err(Error::Transport)?
            .ok_or(Error::Transport(TransportError::Protocol))?;
        completions
            .record(message.clone(), pending.child, report, backend)
            .await
            .map_err(Error::Transport)?;
    }
    match run {
        Run::Completed(id) | Run::AlreadySucceeded(id) => {
            inner
                .handoff
                .completed_child(pending, id)
                .map_err(Error::Transport)?;
            Ok(State::Observed(Arc::new(run)))
        }
        other => Ok(State::Observed(Arc::new(other))),
    }
}

struct NormalHandler {
    app: Application,
}
impl crate::milestone_dispatch::Lifecycle for NormalHandler {
    fn stop_admission(&self) {
        if let Some(completions) = &self.app.inner.completions {
            completions.close_admission();
        }
    }
    fn shutdown(
        &self,
        timeout: Duration,
    ) -> dispatcharr_contracts::BoxFuture<'_, Result<(), crate::milestone_dispatch::Disposition>>
    {
        Box::pin(async move {
            if self.app.shutdown(timeout).await.is_ok() {
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
        let inner = &self.app.inner;
        let (raw, message) = match inner.jobs.peek_series(owner.clone()).await {
            Ok(Some(value)) => value,
            Ok(None) => {
                return Ok(if expected.is_some() {
                    D::HeadChanged
                } else {
                    D::Empty
                })
            }
            Err(TransportError::UnsupportedTask) => return Ok(D::UnsupportedHead),
            Err(error) => return Err(D::Operational(format!("normal Series peek {error:?}"))),
        };
        if expected
            .as_ref()
            .is_some_and(|selected| selected.as_ref() != raw.as_slice())
        {
            return Ok(D::HeadChanged);
        }
        let worker = Worker::new_normal_broker(inner.assembly())
            .map_err(|error| D::Operational(format!("normal Series prepare {error:?}")))?;
        let result = worker
            .run_expected(Some(Arc::from(raw)), owner.clone())
            .await;
        worker
            .shutdown(Duration::from_secs(4))
            .await
            .map_err(|error| D::Operational(format!("normal Series physical drain {error:?}")))?;
        let (run, report) = match result {
            Ok(value) => value,
            Err(configured_recording_worker::Error::Transport(TransportError::ClaimRace)) => {
                return Ok(D::HeadChanged)
            }
            Err(configured_recording_worker::Error::Capacity) => return Ok(D::Capacity),
            Err(error) => return Err(D::Operational(format!("normal Series {error:?}"))),
        };
        if let (
            Some(completions),
            Some(configured_recording_worker::ExecutionReport::Series(report)),
        ) = (&inner.completions, report)
        {
            let backend = inner
                .jobs
                .read_series_lineage(&message, owner)
                .await
                .map_err(|error| D::Operational(format!("normal Series backend {error:?}")))?
                .ok_or_else(|| D::Operational("normal Series backend missing".into()))?;
            completions
                .verify_broker_series(message, report, &backend)
                .map_err(|error| D::Operational(format!("normal Series provenance {error:?}")))?;
        }
        match run {
            Run::Empty => Ok(D::Empty),
            Run::UnsupportedHead => Ok(D::UnsupportedHead),
            Run::Completed(id) | Run::AlreadySucceeded(id) => Ok(D::Completed(id)),
        }
    }
}
