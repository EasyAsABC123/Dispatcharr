//! Explicit scanner-only worker. No Main/default consumer registration.
//! Original Dispatcharr/Celery authors retain behavior and integration credit.
use crate::{
    logo_task::Clock,
    scanner_files::{Error, Owner, Service},
    transport::{ScannerFilesPorts, ScannerIoError, ScannerRoot},
};
use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};
use tokio::sync::{oneshot, Semaphore};
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Run {
    Empty,
    UnsupportedHead,
    Succeeded,
    Failed,
    AlreadySucceeded,
}
#[derive(Clone)]
pub struct Worker {
    inner: Arc<Inner>,
}
struct Inner {
    service: Service,
    ports: ScannerFilesPorts,
    clock: Arc<dyn Clock>,
    registry: Option<crate::scanner_parents::Registry>,
    compat_stats: OnceLock<crate::stats_worker::StatsWorker>,
    slot: Arc<Semaphore>,
}
impl Worker {
    pub fn new(service: Service, ports: ScannerFilesPorts, clock: Arc<dyn Clock>) -> Self {
        Self {
            inner: Arc::new(Inner {
                service,
                ports,
                clock,
                registry: None,
                compat_stats: OnceLock::new(),
                slot: Arc::new(Semaphore::new(1)),
            }),
        }
    }
    pub fn new_tracked(
        service: Service,
        ports: ScannerFilesPorts,
        clock: Arc<dyn Clock>,
        registry: crate::scanner_parents::Registry,
    ) -> Self {
        let mut worker = Self::new(service, ports, clock);
        Arc::get_mut(&mut worker.inner)
            .expect("new unique scanner")
            .registry = Some(registry);
        worker
    }
    pub(crate) fn bind_compat_stats(
        &self,
        stats: crate::stats_worker::StatsWorker,
    ) -> Result<(), Error> {
        self.inner
            .compat_stats
            .set(stats)
            .map_err(|_| Error::Operational)
    }

    pub fn stop_admission(&self) {
        self.inner.slot.close();
    }
    pub fn available_slots(&self) -> usize {
        self.inner.slot.available_permits()
    }
    pub fn first_completed(&self) -> bool {
        self.inner.service.first_completed()
    }
    pub async fn run_once(&self, owner: Owner) -> Result<Run, Error> {
        self.run_expected(None, owner).await
    }
    pub async fn run_selected(&self, expected: Arc<[u8]>, owner: Owner) -> Result<Run, Error> {
        self.run_expected(Some(expected), owner).await
    }
    async fn run_expected(&self, expected: Option<Arc<[u8]>>, owner: Owner) -> Result<Run, Error> {
        let permit = Arc::new(self.inner.slot.clone().try_acquire_owned().map_err(|_| {
            if self.inner.slot.is_closed() {
                Error::Closed
            } else {
                Error::Capacity
            }
        })?);
        let this = self.clone();
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let owner: Owner = Arc::new((owner, permit));
            let result = this.owned(expected, owner).await;
            let _ = tx.send(result);
        });
        rx.await.map_err(|_| Error::Operational)?
    }
    async fn owned(&self, expected: Option<Arc<[u8]>>, owner: Owner) -> Result<Run, Error> {
        let peek = match &expected {
            Some(raw) => {
                self.inner
                    .ports
                    .peek_expected(Some(raw.clone()), owner.clone())
                    .await
            }
            None => self.inner.ports.peek(owner.clone()).await,
        };
        let candidate = match peek {
            Ok(None) => return Ok(Run::Empty),
            Err(ScannerIoError::HeadChanged) => return Err(Error::HeadChanged),
            Err(ScannerIoError::Unsupported) => return Ok(Run::UnsupportedHead),
            Err(error) => return Err(Error::Io(error)),
            Ok(Some(value)) => value,
        };
        if expected
            .as_ref()
            .is_some_and(|raw| !candidate.matches_raw(raw))
        {
            return Err(Error::HeadChanged);
        }
        let now = self.inner.clock.now();
        if !now.is_finite() || !(100_000_000.0..10_000_000_000.0).contains(&now) {
            return Ok(Run::UnsupportedHead);
        }
        match self
            .inner
            .service
            .preflight_scanner(candidate.root.task_id(), owner.clone())
            .await
        {
            Err(Error::Unsupported) => return Ok(Run::UnsupportedHead),
            Err(e) => return Err(e),
            Ok(()) => {}
        }
        if let Some(registry) = &self.inner.registry {
            registry.preflight().map_err(|_| Error::Capacity)?;
        }
        let stats = match candidate.root {
            ScannerRoot::Scan(_) => None,
            ScannerRoot::CompatBeat(_) => {
                let Some(worker) = self.inner.compat_stats.get() else {
                    return Ok(Run::UnsupportedHead);
                };
                Some(
                    worker
                        .admit_inline(owner.clone())
                        .map_err(|error| match error {
                            crate::stats_worker::Error::Closed => Error::Closed,
                            crate::stats_worker::Error::Capacity => Error::Capacity,
                            _ => Error::Operational,
                        })?,
                )
            }
        };
        self.inner.ports.begin_run()?;
        let task_id = self.inner.ports.claim(candidate).await?.task_id();
        if let Some(effect) = stats {
            effect.emit().await.map_err(|_| Error::Operational)?;
        }
        let outcome = self.inner.service.run(task_id, now, owner.clone()).await?;
        let failed = outcome.failure.is_some();
        let bytes = outcome.metadata(task_id)?;
        match self
            .inner
            .ports
            .write(task_id, bytes, owner.clone())
            .await?
        {
            false => Ok(Run::AlreadySucceeded),
            true if failed => Ok(Run::Failed),
            true => {
                if let Some(registry) = &self.inner.registry {
                    registry
                        .record(
                            task_id,
                            &outcome.children,
                            self.inner.ports.committed_publications()?,
                            owner,
                        )
                        .await
                        .map_err(|_| Error::Operational)?;
                }
                Ok(Run::Succeeded)
            }
        }
    }
    pub async fn shutdown(&self, timeout: Duration) -> bool {
        self.drain(timeout, true).await
    }
    pub async fn shutdown_shared(&self, timeout: Duration) -> bool {
        self.drain(timeout, false).await
    }
    async fn drain(&self, timeout: Duration, close_ports: bool) -> bool {
        let Some(deadline) = tokio::time::Instant::now().checked_add(timeout) else {
            return false;
        };
        self.inner.slot.close();
        if tokio::time::timeout(
            deadline.saturating_duration_since(tokio::time::Instant::now()),
            async {
                while self.inner.slot.available_permits() != 1 {
                    tokio::time::sleep(Duration::from_millis(1)).await
                }
            },
        )
        .await
        .is_err()
        {
            return false;
        }
        if !self
            .inner
            .service
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
        {
            return false;
        }
        if !close_ports {
            return true;
        }
        self.inner
            .ports
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
            .is_ok()
    }
}
