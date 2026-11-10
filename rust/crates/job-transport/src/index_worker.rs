//! Fixture-only immediate index execution and retained child publication.
//! Original Dispatcharr authors retain task/algorithm credit and AGPL licensing.
//! Typed terminal results are supported; application202 remains separately gated.
use crate::{
    index_protocol::{IndexMessage, IndexTask},
    index_terminal::Terminal,
    transport::{Error as TransportError, Jobs},
};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Closed,
    Capacity,
    Transport(TransportError),
    InvalidDefer,
    Execution,
    WorkerPanic,
    DrainTimeout,
}
pub type ExecuteFuture<'a> = Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>;
pub type TerminalFuture<'a> = Pin<Box<dyn Future<Output = Result<Terminal, Error>> + Send + 'a>>;
/// Implementations must retain owner through actual parser/SQL/file-lock and
/// publisher I/O; a successful result means the original normal null outcome.
pub trait IndexExecutor: Send + Sync + 'static {
    fn execute<'a>(
        &'a self,
        message: IndexMessage,
        publisher: ChildPublisher,
        owner: Arc<dyn Send + Sync>,
    ) -> ExecuteFuture<'a>;
    /// Existing executors preserve their normal null/control-error behavior.
    /// A classified adapter may explicitly return a known raised terminal value;
    /// an operational error never becomes a fabricated Source exception.
    fn execute_terminal<'a>(
        &'a self,
        message: IndexMessage,
        publisher: ChildPublisher,
        owner: Arc<dyn Send + Sync>,
    ) -> TerminalFuture<'a> {
        Box::pin(async move {
            self.execute(message, publisher, owner).await?;
            Ok(Terminal::NullSuccess)
        })
    }
}
#[derive(Clone, Copy)]
enum ChildState {
    Empty,
    Publishing,
    Committed(Uuid),
}
struct PhysicalChild {
    _worker: Arc<dyn Send + Sync>,
    _caller: Arc<dyn Send + Sync>,
}
#[derive(Clone)]
pub struct ChildPublisher {
    jobs: Jobs,
    parent: IndexMessage,
    owner: Arc<dyn Send + Sync>,
    state: Arc<Mutex<ChildState>>,
}
impl ChildPublisher {
    pub(crate) fn context(jobs: Jobs, parent: IndexMessage, owner: Arc<dyn Send + Sync>) -> Self {
        Self {
            jobs,
            parent,
            owner,
            state: Arc::new(Mutex::new(ChildState::Empty)),
        }
    }
    /// Exact observed manual busy deferral only. The caller can retain an
    /// IndexService SwapPolicyContext in its Arc through the real broker reply.
    pub async fn defer(
        &self,
        source_id: i64,
        retry: u32,
        seconds: u32,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Uuid, Error> {
        if source_id != self.parent.source_id
            || retry != self.parent.retry.saturating_add(1)
            || retry > 480
            || seconds != 5
        {
            return Err(Error::InvalidDefer);
        }
        {
            let mut state = self.state.lock().unwrap();
            if !matches!(*state, ChildState::Empty) {
                return Err(Error::InvalidDefer);
            }
            *state = ChildState::Publishing;
        }
        let id = Uuid::new_v4();
        let task = IndexTask::Deferred {
            source_id,
            retry,
            root: self.parent.root,
            parent: self.parent.task_id,
            eta: chrono::Utc::now() + chrono::Duration::seconds(5),
        };
        let retained = Arc::new(PhysicalChild {
            _worker: self.owner.clone(),
            _caller: caller,
        });
        self.jobs
            .publish_index(&task, id, retained)
            .await
            .map_err(Error::Transport)?;
        *self.state.lock().unwrap() = ChildState::Committed(id);
        Ok(id)
    }
    pub(crate) fn completed_children(&self) -> Result<Vec<Uuid>, Error> {
        match *self.state.lock().unwrap() {
            ChildState::Empty => Ok(Vec::new()),
            ChildState::Committed(id) => Ok(vec![id]),
            ChildState::Publishing => Err(Error::Execution),
        }
    }
}
struct Owner {
    _slot: OwnedSemaphorePermit,
    _caller: Arc<dyn Send + Sync>,
}
struct Inner {
    jobs: Jobs,
    executor: Arc<dyn IndexExecutor>,
    slots: Arc<Semaphore>,
    closed: AtomicBool,
}
#[derive(Clone)]
pub struct IndexWorker {
    inner: Arc<Inner>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Run {
    Empty,
    Completed(Uuid),
    /// Handler executed; only rewriting an existing SUCCESS result was skipped.
    ResultAlreadySucceeded(Uuid),
}
impl IndexWorker {
    pub fn new_fixture(jobs: Jobs, executor: Arc<dyn IndexExecutor>) -> Self {
        Self::new(jobs, executor)
    }
    pub fn new(jobs: Jobs, executor: Arc<dyn IndexExecutor>) -> Self {
        Self {
            inner: Arc::new(Inner {
                jobs,
                executor,
                slots: Arc::new(Semaphore::new(1)),
                closed: AtomicBool::new(false),
            }),
        }
    }
    pub fn available_slots(&self) -> usize {
        self.inner.slots.available_permits()
    }
    pub async fn run_once(&self, caller: Arc<dyn Send + Sync>) -> Result<Run, Error> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let slot = self
            .inner
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let owner = Arc::new(Owner {
            _slot: slot,
            _caller: caller,
        });
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let Some(message) = inner
                .jobs
                .claim_index(owner.clone())
                .await
                .map_err(Error::Transport)?
            else {
                return Ok(Run::Empty);
            };
            let publisher = ChildPublisher {
                jobs: inner.jobs.clone(),
                parent: message.clone(),
                owner: owner.clone(),
                state: Arc::new(Mutex::new(ChildState::Empty)),
            };
            let terminal = inner
                .executor
                .execute_terminal(message.clone(), publisher.clone(), owner.clone())
                .await?;
            let children = publisher.completed_children()?;
            match inner
                .jobs
                .write_index_terminal(message.task_id, message.parent, children, terminal, owner)
                .await
                .map_err(Error::Transport)?
            {
                true => Ok(Run::Completed(message.task_id)),
                false => Ok(Run::ResultAlreadySucceeded(message.task_id)),
            }
        })
        .await
        .map_err(|_| Error::WorkerPanic)?
    }
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), Error> {
        self.inner.closed.store(true, Ordering::Release);
        let drain = self.inner.slots.clone().acquire_owned();
        let _held = tokio::time::timeout(timeout, drain)
            .await
            .map_err(|_| Error::DrainTimeout)?
            .map_err(|_| Error::Closed)?;
        // Shared transport remains open for other active worker families.
        // The owner closes Jobs once all producer/worker drains complete.
        Ok(())
    }
}

/// Concrete adapter using the independently accepted IndexService. Per-task
/// child lineage lives in its immutable dispatcher, never service-global state.
pub struct ServiceExecutor {
    service: dispatcharr_ingest_epg::index_service::IndexService,
}
impl ServiceExecutor {
    pub fn new(service: dispatcharr_ingest_epg::index_service::IndexService) -> Self {
        Self { service }
    }
}
struct ServiceDispatcher(ChildPublisher);
impl dispatcharr_ingest_epg::index_service::IndexDispatcher for ServiceDispatcher {
    fn defer<'a>(
        &'a self,
        id: i64,
        retry: u32,
        seconds: u32,
        context: dispatcharr_ingest_epg::SwapPolicyContext,
    ) -> Pin<Box<dyn Future<Output = Result<(), dispatcharr_ingest_epg::ImportError>> + Send + 'a>>
    {
        Box::pin(async move {
            context.ensure_active()?;
            self.0
                .defer(id, retry, seconds, Arc::new(context))
                .await
                .map_err(|_| dispatcharr_ingest_epg::ImportError::Worker)?;
            Ok(())
        })
    }
}
impl IndexExecutor for ServiceExecutor {
    fn execute<'a>(
        &'a self,
        message: IndexMessage,
        publisher: ChildPublisher,
        owner: Arc<dyn Send + Sync>,
    ) -> ExecuteFuture<'a> {
        Box::pin(async move {
            self.service
                .build_with_dispatcher(
                    dispatcharr_ingest_epg::index_service::IndexRequest {
                        source_id: message.source_id,
                        retry: message.retry,
                        cancelled: Arc::new(AtomicBool::new(false)),
                    },
                    owner,
                    Arc::new(ServiceDispatcher(publisher)),
                )
                .await
                .map_err(|_| Error::Execution)?;
            Ok(())
        })
    }
}
