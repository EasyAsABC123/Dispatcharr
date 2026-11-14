//! Prepared normal version worker/application lifecycle, with no scheduler.
//! Original Dispatcharr authors retain all application/algorithm/license credit.
use crate::{
    transport::{Config, VersionJobs},
    version_service::{Options as ServiceOptions, Service},
    version_worker::{Error, Run, Worker},
};
use dispatcharr_contracts::UpdateEventSink;
use dispatcharr_store::Store;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::watch;
#[derive(Clone, Debug)]
pub enum State {
    Prepared,
    Starting,
    Observed(Run),
    Fault(Arc<Error>),
    Stopped,
}
struct Lifecycle {
    started: bool,
    closed: bool,
}
struct Inner {
    jobs: VersionJobs,
    worker: Worker,
    lifecycle: Mutex<Lifecycle>,
    stop: watch::Sender<bool>,
    done: watch::Sender<bool>,
    state: watch::Sender<State>,
}
struct Caller {
    _application: Arc<Inner>,
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
    pub async fn prepare(
        store: Store,
        sink: Arc<dyn UpdateEventSink>,
        broker: &str,
        backend: &str,
        options: ServiceOptions,
    ) -> Result<Self, &'static str> {
        let service = Service::prepare(store.pool().clone(), sink, options)
            .map_err(|_| "Invalid stable-version service configuration")?;
        let jobs = VersionJobs::connect(broker, backend, Config::default())
            .await
            .map_err(|_| "Version task broker initialization failed")?;
        let worker = Worker::prepare(jobs.clone(), service);
        let stop = watch::channel(false).0;
        Ok(Self {
            inner: Arc::new(Inner {
                jobs,
                worker,
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
            .map_err(|_| "Version lifecycle poisoned")?;
        if lifecycle.started || lifecycle.closed {
            return Err("Version intake already started or closed");
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
                match inner.worker.run_once(owner).await {
                    Ok(run) => {
                        inner.state.send_replace(State::Observed(run));
                    }
                    Err(Error::Closed) if *stop.borrow() => break,
                    Err(error) => {
                        inner.state.send_replace(State::Fault(Arc::new(error)));
                        inner.done.send_replace(true);
                        return;
                    }
                }
                tokio::select! {_=stop.changed()=>{},_=tokio::time::sleep(Duration::from_millis(100))=>{}}
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
        if self.inner.worker.shutdown(timeout).await.is_err() {
            return false;
        }
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
        if !poller {
            return false;
        }
        self.inner
            .jobs
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
            .is_ok()
    }
}
