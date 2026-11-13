//! Explicit scanner-owned M3U groups/single intake. No poller or Main starts.
use crate::{
    scanner_m3u::{Error as DomainError, Owner, Service},
    scanner_m3u_protocol::Scope,
    transport::{Error as TransportError, M3uPorts},
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Closed,
    Capacity,
    DrainTimeout,
    WorkerPanic,
    Transport(TransportError),
    Domain(DomainError),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Run {
    Empty,
    Completed(Uuid),
    ResultAlreadySucceeded(Uuid),
}
struct Retained {
    _permit: OwnedSemaphorePermit,
    _caller: Owner,
}
struct Inner {
    service: Service,
    ports: M3uPorts,
    scope: Scope,
    slots: Arc<Semaphore>,
    closed: AtomicBool,
}
#[derive(Clone)]
pub struct Worker {
    inner: Arc<Inner>,
}
impl Worker {
    pub fn new(service: Service, ports: M3uPorts, scope: Scope) -> Self {
        Self {
            inner: Arc::new(Inner {
                service,
                ports,
                scope,
                slots: Arc::new(Semaphore::new(1)),
                closed: AtomicBool::new(false),
            }),
        }
    }
    pub fn available_slots(&self) -> usize {
        self.inner.slots.available_permits()
    }
    pub async fn run_once(&self, caller: Owner) -> Result<Run, Error> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let permit = self
            .inner
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let owner = Arc::new(Retained {
            _permit: permit,
            _caller: caller,
        });
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let Some(candidate) = inner
                .ports
                .peek(&inner.scope, owner.clone())
                .await
                .map_err(Error::Transport)?
            else {
                return Ok(Run::Empty);
            };
            let prepared = inner
                .service
                .prepare(
                    candidate.message.account,
                    candidate.message.kind,
                    owner.clone(),
                )
                .await
                .map_err(Error::Domain)?;
            if prepared.output_cache_needed(candidate.message.kind) {
                inner
                    .ports
                    .cache_preflight(owner.clone())
                    .await
                    .map_err(Error::Transport)?;
            }
            let message = inner
                .ports
                .claim(candidate, owner.clone())
                .await
                .map_err(Error::Transport)?;
            let value = inner
                .service
                .execute_terminal(message.kind, prepared)
                .await
                .map_err(Error::Domain)?;
            let written = inner
                .ports
                .write_terminal(&message, value, owner)
                .await
                .map_err(Error::Transport)?;
            Ok(if written {
                Run::Completed(message.id)
            } else {
                Run::ResultAlreadySucceeded(message.id)
            })
        })
        .await
        .map_err(|_| Error::WorkerPanic)?
    }
    /// Drain this intake only. The assembly must drain all workers, then services,
    /// then close the shared ports. Closing one worker never closes others' Jobs.
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), Error> {
        self.inner.closed.store(true, Ordering::Release);
        let _slot = tokio::time::timeout(timeout, self.inner.slots.clone().acquire_owned())
            .await
            .map_err(|_| Error::DrainTimeout)?
            .map_err(|_| Error::Closed)?;
        Ok(())
    }
}
