//! Explicit scanner-only worker. No Main/default consumer registration.
//! Original Dispatcharr/Celery authors retain behavior and integration credit.
use crate::{
    logo_task::Clock,
    scanner_files::{Error, Owner, Service},
    transport::{ScannerFilesPorts, ScannerIoError},
};
use std::{sync::Arc, time::Duration};
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
    slot: Arc<Semaphore>,
}
impl Worker {
    pub fn new(service: Service, ports: ScannerFilesPorts, clock: Arc<dyn Clock>) -> Self {
        Self {
            inner: Arc::new(Inner {
                service,
                ports,
                clock,
                slot: Arc::new(Semaphore::new(1)),
            }),
        }
    }
    pub fn available_slots(&self) -> usize {
        self.inner.slot.available_permits()
    }
    pub fn first_completed(&self) -> bool {
        self.inner.service.first_completed()
    }
    pub async fn run_once(&self, owner: Owner) -> Result<Run, Error> {
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
            let result = this.owned(owner).await;
            let _ = tx.send(result);
        });
        rx.await.map_err(|_| Error::Operational)?
    }
    async fn owned(&self, owner: Owner) -> Result<Run, Error> {
        let candidate = match self.inner.ports.peek(owner.clone()).await {
            Ok(None) => return Ok(Run::Empty),
            Err(ScannerIoError::Unsupported) => return Ok(Run::UnsupportedHead),
            Err(error) => return Err(Error::Io(error)),
            Ok(Some(value)) => value,
        };
        let now = self.inner.clock.now();
        if !now.is_finite() || !(100_000_000.0..10_000_000_000.0).contains(&now) {
            return Ok(Run::UnsupportedHead);
        }
        match self.inner.service.preflight(owner.clone()).await {
            Err(Error::Unsupported) => return Ok(Run::UnsupportedHead),
            Err(e) => return Err(e),
            Ok(()) => {}
        }
        self.inner.ports.begin_run()?;
        let message = self.inner.ports.claim(candidate).await?;
        let outcome = self
            .inner
            .service
            .run(message.task_id, now, owner.clone())
            .await?;
        let failed = outcome.failure.is_some();
        let bytes = outcome.metadata(message.task_id)?;
        match self
            .inner
            .ports
            .write(message.task_id, bytes, owner)
            .await?
        {
            false => Ok(Run::AlreadySucceeded),
            true if failed => Ok(Run::Failed),
            true => Ok(Run::Succeeded),
        }
    }
    pub async fn shutdown(&self, timeout: Duration) -> bool {
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
        self.inner
            .ports
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
            .is_ok()
    }
}
