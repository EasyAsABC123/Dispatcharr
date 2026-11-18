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

struct NormalHandler {
    app: Application,
}
impl crate::milestone_dispatch::Lifecycle for NormalHandler {
    fn begin_drain(&self, budget: Duration) {
        self.app.inner.worker.begin_drain(budget);
    }
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
            Ok(Run::Completed(id, _) | Run::AlreadySucceeded(id, _)) => Ok(D::Completed(id)),
            Err(crate::version_worker::Error::Transport(crate::transport::Error::ClaimRace)) => {
                Ok(D::HeadChanged)
            }
            Err(crate::version_worker::Error::Capacity) => Ok(D::Capacity),
            Err(error) => Err(D::Operational(format!("normal version {error:?}"))),
        }
    }
}
