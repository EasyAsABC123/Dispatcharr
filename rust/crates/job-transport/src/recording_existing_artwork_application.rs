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
    HeadChanged,
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
    /// Actual existing local-artwork Jobs shared with the normal booking signal.
    /// This creates no connection pool and exposes no arbitrary frame publisher.
    pub fn booking_publisher(&self) -> Result<transport::ArtworkRootPublisher, transport::Error> {
        transport::ArtworkRootPublisher::new(self.inner.jobs.clone())
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
    run_mode(inner, owner, false, None).await
}
async fn run_mode(
    inner: &Arc<Inner>,
    owner: Arc<dyn Send + Sync>,
    adopt: bool,
    expected: Option<Arc<[u8]>>,
) -> Result<State, Error> {
    let peeked = match expected {
        Some(raw) => {
            inner
                .jobs
                .peek_selected_normal_artwork(&raw, owner.clone())
                .await
        }
        None => inner.jobs.peek_normal_artwork(owner.clone()).await,
    };
    let candidate = match peeked {
        Ok(Some(v)) => v,
        Ok(None) => return Ok(State::Empty),
        Err(transport::Error::UnsupportedTask) => return Ok(State::UnsupportedHead),
        Err(transport::Error::ClaimRace) => return Ok(State::HeadChanged),
        Err(error) => return Err(Error::Transport(error)),
    };
    let cap = inner
        .completions
        .admit(&candidate.message)
        .map_err(Error::Transport)?;
    if cap
        .as_ref()
        .is_some_and(|proof| !proof.admits(&candidate.message))
    {
        return Err(Error::Transport(transport::Error::Protocol));
    }
    if cap.is_none() && !adopt {
        return Ok(State::WaitingForCompletedSeries);
    }
    // For normal intake the sealed authenticated broker candidate plus bounded
    // actual Recording domain preflight is admission. Parent SUCCESS/in-memory
    // Native producer witness remains optional provenance, never Source authority.

    if candidate.message.eta > chrono::Utc::now() {
        return Ok(State::NotDue);
    }
    // Resolve and bound all existing-artwork branches before ACK. Unsupported
    // branches keep original bytes/order, so other families are never skipped.
    let preparing = if adopt {
        inner
            .store
            .prepare_normal(candidate.message.recording, owner.clone())
            .await
    } else {
        inner
            .store
            .prepare(candidate.message.recording, owner.clone())
            .await
    };
    let prepared = match preparing {
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
    let completed = if adopt {
        inner.store.execute_normal(prepared, owner.clone()).await
    } else {
        inner.store.execute(prepared, owner.clone()).await
    }
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
    if cap.is_some() {
        inner
            .completions
            .complete(&message)
            .map_err(Error::Transport)?;
    }
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

struct NormalHandler {
    app: Application,
}
impl crate::milestone_dispatch::Lifecycle for NormalHandler {
    fn stop_admission(&self) {
        self.app.close_producer_admission();
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
        let inner = self.app.inner.clone();
        let selected = match expected {
            Some(raw) => raw,
            None => match inner.jobs.peek_artwork_frame(owner.clone()).await {
                Ok(Some(raw)) => Arc::from(raw),
                Ok(None) => return Ok(D::Empty),
                Err(error) => {
                    return Err(D::Operational(format!(
                        "normal artwork selection {error:?}"
                    )))
                }
            },
        };
        let frame = crate::clocked_recording_envelope::json_value(&selected)
            .map_err(|_| D::UnsupportedHead)?;
        let outcome = if frame["headers"]["parent_id"].is_null() {
            run_root(&inner, owner, selected).await
        } else {
            run_mode(&inner, owner, true, Some(selected)).await
        };
        match outcome {
            Ok(State::Empty) => Ok(D::Empty),
            Ok(State::HeadChanged) => Ok(D::HeadChanged),
            Ok(State::UnsupportedHead) => Ok(D::UnsupportedHead),
            Ok(State::NotDue) => Ok(D::NotDue),
            Ok(State::WaitingForCompletedSeries) => Ok(D::UnsupportedDomain),
            Ok(State::Completed { task_id, .. } | State::AlreadySucceeded { task_id, .. }) => {
                Ok(D::Completed(task_id))
            }
            Ok(_) => Err(D::Operational("unexpected normal artwork state".into())),
            Err(error) => Err(D::Operational(format!("normal artwork {error:?}"))),
        }
    }
}

/// Root intake has its own typed message/candidate; no invented parent UUID or
/// child-publication witness is used to reuse the actual artwork domain service.
async fn run_root(
    inner: &Arc<Inner>,
    owner: Arc<dyn Send + Sync>,
    expected: Arc<[u8]>,
) -> Result<State, Error> {
    let candidate = match inner
        .jobs
        .peek_root_artwork(Some(&expected), owner.clone())
        .await
    {
        Ok(Some(candidate)) => candidate,
        Ok(None) => return Ok(State::Empty),
        Err(transport::Error::ClaimRace) => return Ok(State::HeadChanged),
        Err(transport::Error::UnsupportedTask) => return Ok(State::UnsupportedHead),
        Err(error) => return Err(Error::Transport(error)),
    };
    if candidate
        .message
        .eta
        .is_some_and(|eta| eta > chrono::Utc::now())
    {
        return Ok(State::NotDue);
    }
    let prepared = match inner
        .store
        .prepare_normal(candidate.message.recording, owner.clone())
        .await
    {
        Ok(value) => value,
        Err(recording_existing_artwork::Error::Unsupported(_)) => {
            return Ok(State::UnsupportedHead)
        }
        Err(error) => return Err(Error::Domain(error)),
    };
    let message = candidate.message.clone();
    inner
        .jobs
        .claim_root_artwork(candidate, owner.clone())
        .await
        .map_err(Error::Transport)?;
    let completed = inner
        .store
        .execute_normal(prepared, owner.clone())
        .await
        .map_err(Error::Domain)?;
    if completed.changed {
        let (done, settled) = oneshot::channel();
        let receipt = Arc::new(Receipt {
            _owner: owner.clone(),
            done: Some(done),
        });
        let observed=inner.events.send_retained("updates",json!({"type":"update","data":{"success":true,"type":"recording_updated","recording_id":completed.recording}}),receipt.clone()).await;
        drop(receipt);
        settled.await.map_err(|_| Error::Event)?;
        observed.map_err(|_| Error::Event)?;
    }
    let written = inner
        .jobs
        .write_root_artwork(&message, &completed, owner)
        .await
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
