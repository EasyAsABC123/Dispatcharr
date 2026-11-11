//! Bounded inert default-task library. Original Dispatcharr/Celery authors retain credit.
//! No Main registration or automatic intake; unsupported domains remain explicit.
use crate::default_worker::{DefaultWorker, Error, Run};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::watch,
    time::{self, Instant},
};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LifecycleError {
    Configuration,
    Lifecycle,
}
#[derive(Clone, Debug)]
pub enum State {
    Prepared,
    Starting,
    Empty,
    Completed,
    UnsupportedHead,
    UnsupportedDomain,
    // Keep exact typed error without pretending the poller is healthy/stopped.
    Fault(Arc<Error>),
    Stopped,
}
#[derive(Clone, Debug)]
pub struct Drain {
    pub drained: bool,
    pub state: State,
}
#[derive(Default)]
struct Lifecycle {
    started: bool,
    closed: bool,
}
struct ApplicationLifetime {
    stop: watch::Sender<bool>,
    worker: DefaultWorker,
}
impl Drop for ApplicationLifetime {
    fn drop(&mut self) {
        self.stop.send_replace(true);
        self.worker.request_stop();
    }
}
#[derive(Clone)]
pub struct DefaultApplication {
    inner: Arc<Inner>,
    _lifetime: Arc<ApplicationLifetime>,
}
struct Inner {
    worker: DefaultWorker,
    poll: Duration,
    lifecycle: Mutex<Lifecycle>,
    stop: watch::Sender<bool>,
    done: watch::Sender<bool>,
    state: watch::Sender<State>,
}
impl DefaultApplication {
    pub fn prepare(worker: DefaultWorker, poll: Duration) -> Result<Self, LifecycleError> {
        if !(Duration::from_millis(25)..=Duration::from_secs(5)).contains(&poll) {
            return Err(LifecycleError::Configuration);
        }
        let stop = watch::channel(false).0;
        Ok(Self {
            _lifetime: Arc::new(ApplicationLifetime {
                stop: stop.clone(),
                worker: worker.clone(),
            }),
            inner: Arc::new(Inner {
                worker,
                poll,
                lifecycle: Mutex::new(Lifecycle::default()),
                stop,
                done: watch::channel(true).0,
                state: watch::channel(State::Prepared).0,
            }),
        })
    }
    pub fn state(&self) -> State {
        self.inner.state.borrow().clone()
    }
    pub fn start(&self) -> Result<(), LifecycleError> {
        // One synchronous lifecycle lock linearizes start versus shutdown.
        // done=false is visible before shutdown can return for a started loop.
        let mut lifecycle = self
            .inner
            .lifecycle
            .lock()
            .map_err(|_| LifecycleError::Lifecycle)?;
        if lifecycle.closed || lifecycle.started {
            return Err(LifecycleError::Lifecycle);
        }
        lifecycle.started = true;
        self.inner.done.send_replace(false);
        self.inner.state.send_replace(State::Starting);
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let mut stop = inner.stop.subscribe();
            let mut fault = false;
            while !*stop.borrow() {
                let observation = match inner.worker.run_once(Arc::new(())).await {
                    Ok(Run::Empty) => State::Empty,
                    Ok(Run::Completed(_) | Run::AlreadySucceeded(_) | Run::Revoked(_, _)) => {
                        State::Completed
                    }
                    Ok(Run::Unsupported(_)) => State::UnsupportedDomain,
                    Ok(Run::UnsupportedHead) => State::UnsupportedHead,
                    Err(Error::Closed) if *stop.borrow() => break,
                    Err(error) => {
                        fault = true;
                        State::Fault(Arc::new(error))
                    }
                };
                inner.state.send_replace(observation);
                // Operational failure may follow an early ACK: no guessed
                // retries, terminal SUCCESS or silent healthy-looking restart.
                if fault {
                    break;
                }
                tokio::select! {_=stop.changed()=>{},_=time::sleep(inner.poll)=>{}}
            }
            if !fault {
                inner.state.send_replace(State::Stopped);
            }
            inner.done.send_replace(true);
        });
        Ok(())
    }
    pub async fn shutdown(&self, timeout: Duration) -> Drain {
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            return Drain {
                drained: false,
                state: self.state(),
            };
        };
        {
            let Ok(mut lifecycle) = self.inner.lifecycle.lock() else {
                return Drain {
                    drained: false,
                    state: self.state(),
                };
            };
            lifecycle.closed = true;
            self.inner.stop.send_replace(true);
        }
        // Closing family intake does NOT close shared Jobs. Assembly closes
        // transport only after every family has physically drained.
        let worker = self
            .inner
            .worker
            .shutdown(deadline.saturating_duration_since(Instant::now()))
            .await;
        let mut done = self.inner.done.subscribe();
        let poller = time::timeout_at(deadline, async {
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
        Drain {
            drained: worker && poller,
            state: self.state(),
        }
    }
}
impl State {
    pub fn summary(&self) -> String {
        match self {
            Self::Prepared => "Prepared".into(),
            Self::Starting => "Starting".into(),
            Self::Empty => "Empty".into(),
            Self::Completed => "Completed".into(),
            Self::UnsupportedHead => "UnsupportedHead".into(),
            Self::UnsupportedDomain => "UnsupportedDomain".into(),
            Self::Stopped => "Stopped".into(),
            Self::Fault(error) => format!("Fault({error})"),
        }
    }
}
