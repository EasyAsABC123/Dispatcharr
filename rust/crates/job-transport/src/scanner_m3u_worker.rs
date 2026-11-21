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
    HeadChanged,
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
    Failed(Uuid),
    ResultAlreadySucceeded(Uuid),
}
struct Retained {
    _permit: OwnedSemaphorePermit,
    _caller: Owner,
}
struct Inner {
    service: Service,
    ports: M3uPorts,
    scope: Option<Scope>,
    registry: Option<crate::scanner_parents::Registry>,
    slots: Arc<Semaphore>,
    closed: AtomicBool,
}
impl Inner {
    async fn run_periodic_local(&self, expected: &[u8], owner: Owner) -> Result<Run, Error> {
        let jobs = self.ports.shared_jobs();
        let admission = jobs
            .peek_xc_live(Some(expected), owner.clone())
            .await
            .map_err(periodic_head_error)?
            .ok_or(Error::HeadChanged)?;
        if !admission.message.configured_interval_root {
            return Err(Error::Transport(TransportError::UnsupportedTask));
        }
        let kind = crate::scanner_m3u_protocol::Kind::Single;
        let prepared = self
            .service
            .prepare_periodic(admission.message.account, owner.clone())
            .await
            .map_err(Error::Domain)?;
        if prepared.output_cache_needed(kind) {
            self.ports
                .cache_preflight(owner.clone())
                .await
                .map_err(Error::Transport)?;
        }
        let message = jobs
            .claim_xc_live(admission, owner.clone())
            .await
            .map_err(periodic_head_error)?;
        let outcome = self
            .service
            .execute_terminal(kind, prepared)
            .await
            .map_err(Error::Domain)?;
        let crate::scanner_m3u::Terminal::Normal(result) = outcome else {
            return Err(Error::Domain(DomainError::Operational));
        };
        let wrote = jobs
            .write_xc_live(&message, result, Vec::new(), owner)
            .await
            .map_err(Error::Transport)?;
        Ok(if wrote {
            Run::Completed(message.id)
        } else {
            Run::ResultAlreadySucceeded(message.id)
        })
    }
}

fn periodic_head_error(error: TransportError) -> Error {
    match error {
        TransportError::ClaimRace => Error::HeadChanged,
        other => Error::Transport(other),
    }
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
                scope: Some(scope),
                registry: None,
                slots: Arc::new(Semaphore::new(1)),
                closed: AtomicBool::new(false),
            }),
        }
    }
    pub fn new_normal(
        service: Service,
        ports: M3uPorts,
        registry: crate::scanner_parents::Registry,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                service,
                ports,
                scope: None,
                registry: Some(registry),
                slots: Arc::new(Semaphore::new(1)),
                closed: AtomicBool::new(false),
            }),
        }
    }
    /// One actual slot shared by normal STD and XC adapters.
    pub(crate) fn admission(&self) -> Arc<Semaphore> {
        self.inner.slots.clone()
    }
    pub fn stop_admission(&self) {
        self.inner.closed.store(true, Ordering::Release);
    }
    pub fn available_slots(&self) -> usize {
        self.inner.slots.available_permits()
    }
    pub async fn run_once(&self, caller: Owner) -> Result<Run, Error> {
        self.run_expected(None, caller).await
    }
    pub async fn run_selected(&self, expected: Arc<[u8]>, caller: Owner) -> Result<Run, Error> {
        self.run_expected(Some(expected), caller).await
    }
    async fn run_expected(&self, expected: Option<Arc<[u8]>>, caller: Owner) -> Result<Run, Error> {
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
            let scope = match &inner.scope {
                Some(scope) => scope.clone(),
                None => {
                    let registry = inner
                        .registry
                        .as_ref()
                        .ok_or(Error::Transport(TransportError::UnsupportedTask))?;
                    let Some(raw) = registry
                        .head(owner.clone())
                        .await
                        .map_err(Error::Transport)?
                    else {
                        return if expected.is_some() {
                            Err(Error::HeadChanged)
                        } else {
                            Ok(Run::Empty)
                        };
                    };
                    if expected
                        .as_ref()
                        .is_some_and(|selected| selected.as_ref() != raw.as_slice())
                    {
                        return Err(Error::HeadChanged);
                    }
                    if crate::normal_xc_live::parse(&raw)
                        .is_ok_and(|message| message.configured_interval_root)
                    {
                        return inner.run_periodic_local(&raw, owner).await;
                    }
                    registry
                        .broker_m3u_scope(&raw, owner.clone())
                        .await
                        .map_err(|error| {
                            if error == TransportError::ClaimRace {
                                Error::HeadChanged
                            } else {
                                Error::Transport(error)
                            }
                        })?
                        .ok_or(Error::Transport(TransportError::UnsupportedTask))?
                }
            };
            let peek = match &expected {
                Some(raw) => {
                    inner
                        .ports
                        .peek_expected(&scope, Some(raw.clone()), owner.clone())
                        .await
                }
                None => inner.ports.peek(&scope, owner.clone()).await,
            };
            let Some(candidate) = peek.map_err(|e| {
                if expected.is_some() && e == TransportError::ClaimRace {
                    Error::HeadChanged
                } else {
                    Error::Transport(e)
                }
            })?
            else {
                return Ok(Run::Empty);
            };
            if expected
                .as_ref()
                .is_some_and(|selected| !candidate.matches_raw(selected))
            {
                return Err(Error::HeadChanged);
            }
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
            let failed = matches!(&value, crate::scanner_m3u::Terminal::Raised(_));
            let written = inner
                .ports
                .write_terminal(&message, value, owner)
                .await
                .map_err(Error::Transport)?;
            if let Some(registry) = &inner.registry {
                registry
                    .retire_child(message.id)
                    .map_err(Error::Transport)?;
            }
            Ok(if written && failed && inner.registry.is_some() {
                Run::Failed(message.id)
            } else if written {
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
