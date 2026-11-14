//! Normal Main existing-VODLogo artwork consumer, from actual Series provenance.
//! Original Dispatcharr authors retain all application/algorithm/license credit.
//! Unsupported provider/manual branches leave the exact default FIFO head intact.
use crate::{
    recording_artwork_completion::Completions,
    transport::{self, Jobs},
};
use dispatcharr_contracts::UpdateEventSink;
use dispatcharr_store::recording_existing_artwork::{self, Store};
use serde_json::json;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{oneshot, watch};
use uuid::Uuid;
#[derive(Debug)]
pub enum Error {
    Transport(transport::Error),
    Domain(recording_existing_artwork::Error),
    Event,
    DrainTimeout,
}
#[derive(Clone, Debug)]
pub enum State {
    Prepared,
    Starting,
    Empty,
    UnsupportedHead,
    NotDue,
    WaitingForCompletedSeries,
    Completed { task_id: Uuid, changed: bool },
    AlreadySucceeded { task_id: Uuid, changed: bool },
    Fault(Arc<Error>),
    Stopped,
}
struct Lifecycle {
    started: bool,
    closed: bool,
}
struct Inner {
    jobs: Jobs,
    store: Store,
    events: Arc<dyn UpdateEventSink>,
    completions: Completions,
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
struct Receipt {
    _owner: Arc<dyn Send + Sync>,
    done: Option<oneshot::Sender<()>>,
}
impl Drop for Receipt {
    fn drop(&mut self) {
        if let Some(done) = self.done.take() {
            let _ = done.send(());
        }
    }
}
#[derive(Clone)]
pub struct Application {
    inner: Arc<Inner>,
    _lifetime: Arc<Lifetime>,
}
impl Application {
    pub async fn prepare(
        pool: sqlx::PgPool,
        events: Arc<dyn UpdateEventSink>,
        completions: Completions,
        broker: &str,
        backend: &str,
        poll: Duration,
    ) -> Result<Self, &'static str> {
        if !(Duration::from_millis(25)..=Duration::from_secs(5)).contains(&poll) {
            return Err("Invalid artwork poll interval");
        }
        let jobs = Jobs::connect_local_artwork(broker, backend, transport::Config::default())
            .await
            .map_err(|_| "Artwork broker initialization failed")?;
        let stop = watch::channel(false).0;
        Ok(Self {
            inner: Arc::new(Inner {
                jobs,
                store: Store::new(pool),
                events,
                completions,
                poll,
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
            .map_err(|_| "Artwork lifecycle poisoned")?;
        if lifecycle.started || lifecycle.closed {
            return Err("Artwork intake already started or closed");
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
                tokio::select! {_=stop.changed()=>{},_=tokio::time::sleep(inner.poll)=>{}}
            }
            inner.state.send_replace(State::Stopped);
            inner.done.send_replace(true);
        });
        Ok(())
    }
    /// First cancel bounded producer admission, then drain Series while this
    /// consumer stays alive. Already recorded provenance is preserved unchanged.
    pub fn close_producer_admission(&self) {
        self.inner.completions.close_admission();
    }
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), Error> {
        let deadline = tokio::time::Instant::now() + timeout;
        {
            let mut lifecycle = self
                .inner
                .lifecycle
                .lock()
                .map_err(|_| Error::DrainTimeout)?;
            lifecycle.closed = true;
            self.inner.stop.send_replace(true);
        }
        let mut done = self.inner.done.subscribe();
        tokio::time::timeout(timeout, async {
            loop {
                if *done.borrow_and_update() {
                    return Ok(());
                }
                done.changed().await.map_err(|_| Error::DrainTimeout)?;
            }
        })
        .await
        .map_err(|_| Error::DrainTimeout)??;
        self.inner
            .jobs
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
            .map_err(Error::Transport)
    }
}
async fn run(inner: &Arc<Inner>, owner: Arc<dyn Send + Sync>) -> Result<State, Error> {
    let candidate = match inner.jobs.peek_local_artwork(owner.clone()).await {
        Ok(Some(v)) => v,
        Ok(None) => return Ok(State::Empty),
        Err(transport::Error::UnsupportedTask) => return Ok(State::UnsupportedHead),
        Err(error) => return Err(Error::Transport(error)),
    };
    let Some(cap) = inner
        .completions
        .admit(&candidate.message)
        .map_err(Error::Transport)?
    else {
        return Ok(State::WaitingForCompletedSeries);
    };
    if !cap.admits(&candidate.message) {
        return Err(Error::Transport(transport::Error::Protocol));
    }
    if candidate.message.eta > chrono::Utc::now() {
        return Ok(State::NotDue);
    }
    // Resolve and bound all existing-artwork branches before ACK. Unsupported
    // branches keep original bytes/order, so other families are never skipped.
    let prepared = match inner
        .store
        .prepare(candidate.message.recording, owner.clone())
        .await
    {
        Ok(v) => v,
        Err(recording_existing_artwork::Error::Unsupported(_)) => {
            return Ok(State::UnsupportedHead)
        }
        Err(error) => return Err(Error::Domain(error)),
    };
    let message = candidate.message.clone();
    inner
        .jobs
        .claim_local_artwork(candidate, owner.clone())
        .await
        .map_err(Error::Transport)?;
    let completed = inner
        .store
        .execute(prepared, owner.clone())
        .await
        .map_err(Error::Domain)?;
    if completed.changed {
        let (tx, rx) = oneshot::channel();
        let receipt = Arc::new(Receipt {
            _owner: owner.clone(),
            done: Some(tx),
        });
        let observed=inner.events.send_retained("updates",json!({"type":"update","data":{"success":true,"type":"recording_updated","recording_id":completed.recording}}),receipt.clone()).await;
        drop(receipt);
        rx.await.map_err(|_| Error::Event)?;
        observed.map_err(|_| Error::Event)?;
    }
    let written = inner
        .jobs
        .write_existing_artwork(&message, &completed, owner)
        .await
        .map_err(Error::Transport)?;
    inner
        .completions
        .complete(&message)
        .map_err(Error::Transport)?;
    Ok(if written {
        State::Completed {
            task_id: message.task_id,
            changed: completed.changed,
        }
    } else {
        State::AlreadySucceeded {
            task_id: message.task_id,
            changed: completed.changed,
        }
    })
}
