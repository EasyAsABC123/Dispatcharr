//! Normal configured Series consumer, including completed Refresh descendants.
//! Original Dispatcharr authors retain application/algorithm/license credit.
//! No source-parent reconstruction, artwork fallback or unqualified FIFO skip.
use crate::{
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
        if !(Duration::from_millis(25)..=Duration::from_secs(5)).contains(&options.poll_interval) {
            return Err("Invalid configured Series poll interval");
        }
        let jobs = Jobs::connect_series(&options.broker, &options.backend, Config::default())
            .await
            .map_err(|_| "Configured Series broker initialization failed")?;
        let locks = TaskLocks::connect(&options.cache, TaskLockConfig::default())
            .await
            .map_err(|_| "Configured Series locks initialization failed")?;
        let artwork = Arc::new(
            Publisher::connect(&options.broker, &options.backend, Config::default())
                .await
                .map_err(|_| "Configured Series artwork publisher initialization failed")?,
        );
        let direct = Worker::new(Assembly {
            jobs: jobs.clone(),
            locks: locks.clone(),
            store: store.clone(),
            cache: options.cache.clone(),
            artwork: artwork.clone(),
            events: events.clone(),
            family: Family::Series,
        })
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
    let worker = Worker::new_refresh_series_child(inner.assembly(), pending.child)?;
    let result = worker.run_once(owner).await;
    // This worker is scoped to one capability. Do not detach physical work on
    // an observer result; its one slot must actually drain before handoff removal.
    worker.shutdown(Duration::from_secs(4)).await?;
    let run = result?;
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
