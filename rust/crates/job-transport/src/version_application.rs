//! Prepared normal version worker/application lifecycle, with no scheduler.
//! Original Dispatcharr authors retain all application/algorithm/license credit.
use crate::{
    milestone_dispatch::{Disposition, Handler, Lifecycle as HandlerLifecycle},
    transport::{Config, Jobs, VersionJobs},
    version_service::{Options as ServiceOptions, Service},
    version_worker::{Error, Run, Worker},
};
use dispatcharr_contracts::UpdateEventSink;
use dispatcharr_output_store::transport::{RedisTransport, TransportConfig};
use dispatcharr_store::{ProducerIdentitySource, Store};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::watch;

/// Prepare a version callback for an existing normal queue owner.
/// The queue owner retains responsibility for closing the shared transport.
pub fn prepare_shared_handler(
    store: &Store,
    sink: Arc<dyn UpdateEventSink>,
    jobs: Jobs,
    options: ServiceOptions,
) -> Result<Arc<dyn Handler>, &'static str> {
    let jobs = VersionJobs::from_defaults(jobs).map_err(|_| "Invalid shared version transport")?;
    let service = Service::prepare(store.pool().clone(), sink, options)
        .map_err(|_| "Invalid stable-version service configuration")?;
    Ok(Arc::new(NormalHandler {
        worker: Worker::prepare(jobs, service),
        standalone: None,
    }))
}

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
        Ok(Arc::new(NormalHandler {
            worker: self.inner.worker.clone(),
            standalone: Some(self),
        }))
    }
    pub fn state(&self) -> State {
        self.inner.state.borrow().clone()
    }
    /// The caller has explicitly assigned the default queue to this application.
    /// Keep the old consumer-only entry point for existing callers.
    pub async fn start_with_startup(
        &self,
        cache: &str,
        identity: ProducerIdentitySource,
    ) -> Result<(), &'static str> {
        use crate::version_protocol::{STARTUP_LOCK, STARTUP_LOCK_SECONDS, TASK};

        let guard = RedisTransport::connect(
            cache,
            TransportConfig {
                max_operations: 1,
                max_queued_operations: 1,
                command_timeout: Config::default().command_timeout,
            },
        )
        .await
        .map_err(|_| "Version startup lock initialization failed")?;
        let producer = identity
            .current()
            .map_err(|_| "Version startup producer identity failed")?;
        let raw = crate::normal_recording_publication::startup_frame(
            TASK,
            uuid::Uuid::new_v4(),
            producer.reply_to,
            &producer.origin,
        )
        .map_err(|_| "Version startup frame initialization failed")?;
        self.start()?;
        let owner = Arc::new(Caller {
            _application: self.inner.clone(),
        });
        let ttl = STARTUP_LOCK_SECONDS.to_string();
        // The ordinary opt-in needs positive lock authority before publication.
        // Leave the Source TTL in place after every settled or uncertain publish.
        let (claim, owner): (Option<String>, _) = guard
            .command(
                "return redis.call('SET',KEYS[1],'1','NX','EX',ARGV[1])",
                &[STARTUP_LOCK.to_owned()],
                &[ttl.as_bytes()],
                owner,
            )
            .await
            .map_err(|_| "Version startup lock did not settle")?;
        match claim.as_deref() {
            None => Ok(()),
            Some("OK") => self
                .inner
                .jobs
                .publish_startup(&raw, owner)
                .await
                .map(|_| ())
                .map_err(|_| "Version startup root did not settle"),
            Some(_) => Err("Version startup lock reply invalid"),
        }
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
    worker: Worker,
    standalone: Option<Application>,
}
impl HandlerLifecycle for NormalHandler {
    fn begin_drain(&self, budget: Duration) {
        self.worker.begin_drain(budget);
    }
    fn shutdown(
        &self,
        timeout: Duration,
    ) -> dispatcharr_contracts::BoxFuture<'_, Result<(), Disposition>> {
        Box::pin(async move {
            let drained = match &self.standalone {
                Some(app) => app.shutdown(timeout).await,
                None => self.worker.shutdown(timeout).await.is_ok(),
            };
            if drained {
                Ok(())
            } else {
                Err(Disposition::Operational(
                    "normal family drain incomplete".into(),
                ))
            }
        })
    }
}
impl Handler for NormalHandler {
    fn supports_selected_head(&self) -> bool {
        true
    }
    fn run_once(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> dispatcharr_contracts::BoxFuture<'_, Result<Disposition, Disposition>> {
        Box::pin(self.execute(None, owner))
    }
    fn run_selected(
        &self,
        expected: Arc<[u8]>,
        owner: Arc<dyn Send + Sync>,
    ) -> dispatcharr_contracts::BoxFuture<'_, Result<Disposition, Disposition>> {
        Box::pin(self.execute(Some(expected), owner))
    }
}
impl NormalHandler {
    async fn execute(
        &self,
        expected: Option<Arc<[u8]>>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Disposition, Disposition> {
        use Disposition as D;
        match self.worker.run_expected(expected, owner).await {
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
