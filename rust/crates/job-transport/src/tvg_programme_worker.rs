//! PRIVATE uncompiled whole-task composition for the M6 integrated candidate.
//! Source actual whole-task extension and integrated matrix remain REQUIRED.
//! No Main/registry registration, generic failure serializer or retry authority.
use crate::{transport::TvgJobs, tvg_programme_cache::Cache, tvg_programme_files::LocalFiles};
use dispatcharr_coordination::{TaskLockDetailedError, TaskLockError, TaskLocks};
use dispatcharr_ingest_epg::{cache_policy::CachedSwapPolicy, SwapPolicyContext};
use dispatcharr_store::Store;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};
#[derive(Debug)]
pub enum Error {
    Closed,
    Capacity,
    Unsupported,
    Transport(crate::transport::Error),
    Store(dispatcharr_store::tvg_programme::Error),
    Cache(crate::tvg_programme_cache::Error),
    File(String),
    Lock(String),
    Policy(String),
    Supervisor,
    Drain,
    AfterBody {
        body: Option<Box<Error>>,
        release: Option<Box<Error>>,
        leave: Option<Box<Error>>,
    },
}
#[derive(Debug, PartialEq, Eq)]
pub enum Run {
    Empty,
    UnsupportedHead,
    Completed(uuid::Uuid),
}
struct Owner {
    _caller: Arc<dyn Send + Sync>,
    _slot: OwnedSemaphorePermit,
}
struct Receipt {
    owner: Option<Arc<Owner>>,
    done: Option<oneshot::Sender<()>>,
}
impl Drop for Receipt {
    fn drop(&mut self) {
        drop(self.owner.take());
        if let Some(done) = self.done.take() {
            let _ = done.send(());
        }
    }
}
struct Inner {
    jobs: TvgJobs,
    store: Store,
    locks: TaskLocks,
    cache: Cache,
    files: Arc<LocalFiles>,
    policy: Arc<CachedSwapPolicy>,
    slots: Arc<Semaphore>,
    closed: AtomicBool,
}
#[derive(Clone)]
pub struct Worker {
    inner: Arc<Inner>,
}
impl Worker {
    pub fn prepare(
        jobs: TvgJobs,
        store: Store,
        locks: TaskLocks,
        cache: Cache,
        files: Arc<LocalFiles>,
        policy: Arc<CachedSwapPolicy>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                jobs,
                store,
                locks,
                cache,
                files,
                policy,
                slots: Arc::new(Semaphore::new(1)),
                closed: AtomicBool::new(false),
            }),
        }
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
            _caller: caller,
            _slot: slot,
        });
        let inner = self.inner.clone();
        tokio::spawn(async move { run(inner, owner).await })
            .await
            .map_err(|_| Error::Supervisor)?
    }
    pub fn available_slots(&self) -> usize {
        self.inner.slots.available_permits()
    }
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), Error> {
        self.inner.closed.store(true, Ordering::Release);
        let permit = tokio::time::timeout(timeout, self.inner.slots.clone().acquire_owned())
            .await
            .map_err(|_| Error::Drain)?
            .map_err(|_| Error::Drain)?;
        drop(permit);
        Ok(())
    }
}
async fn run(inner: Arc<Inner>, owner: Arc<Owner>) -> Result<Run, Error> {
    let candidate = match inner.jobs.peek(owner.clone()).await {
        Ok(Some(v)) => v,
        Ok(None) => return Ok(Run::Empty),
        Err(crate::transport::Error::UnsupportedTask) => return Ok(Run::UnsupportedHead),
        Err(e) => return Err(Error::Transport(e)),
    };
    let id = candidate.message.epg_data_id;
    let task = candidate.message.task_id;
    let context = inner
        .store
        .tvg_programme_context(id, owner.clone())
        .await
        .map_err(Error::Store)?;
    let mut input = None;
    if let Some(context) = context.as_ref() {
        if context.source_type != "dummy" {
            if context.source_type != "xmltv" {
                return Ok(Run::UnsupportedHead);
            }
            if inner
                .cache
                .source_refresh_held(context.source_id, owner.clone())
                .await
                .map_err(Error::Cache)?
            {
                // Source deferred publication is a required batch extension; do not eat
                // that legitimate FIFO head until its actual child/result policy exists.
                return Ok(Run::UnsupportedHead);
            }
            if context.mapped {
                let path = context
                    .extracted_file_path
                    .as_ref()
                    .filter(|s| !s.is_empty())
                    .or(context.file_path.as_ref().filter(|s| !s.is_empty()))
                    .ok_or(Error::Unsupported)?;
                input = Some(
                    inner
                        .files
                        .open(path)
                        .map_err(|e| Error::File(format!("{e:?}")))?,
                );
            }
        }
    }
    let claimed = inner
        .jobs
        .claim(candidate, owner.clone())
        .await
        .map_err(Error::Transport)?;
    use crate::transport::TvgNormal as Normal;
    let normal = if let Some(context) = context {
        if context.source_type == "dummy" {
            Normal::Null
        } else {
            inner
                .cache
                .enter(context.source_id, owner.clone())
                .await
                .map_err(Error::Cache)?;
            let lease = inner
                .locks
                .acquire_retained_detailed("parse_epg_programs", id, owner.clone())
                .await;
            let (body, release) = match lease {
                Err(TaskLockDetailedError::Operational(TaskLockError::Held)) => {
                    (Ok(Normal::Busy), Ok(()))
                }
                Err(error) => (Err(Error::Lock(format!("{error:?}"))), Ok(())),
                Ok(lease) => {
                    let body = async {
                        if let Some(file) = input {
                            let rows = dispatcharr_ingest_epg::tvg_programme_file::read(
                                file,
                                id,
                                context.tvg_id,
                                owner.clone(),
                            )
                            .await
                            .map_err(|e| Error::File(format!("{e:?}")))?;
                            // Source samples its retention clock before settings I/O, only when
                            // the initial parse has at least one programme. Empty parse preserves
                            // every stored row, including historical rows past cutoff.
                            if !rows.is_empty() {
                                let (tx, rx) = oneshot::channel();
                                let retained = Arc::new(Receipt {
                                    owner: Some(owner.clone()),
                                    done: Some(tx),
                                });
                                let policy = inner
                                    .policy
                                    .retained_tvg_history_policy(SwapPolicyContext {
                                        cancelled: Arc::new(AtomicBool::new(false)),
                                        retained,
                                    })
                                    .await;
                                rx.await.map_err(|_| Error::Supervisor)?;
                                let policy = policy.map_err(|e| Error::Policy(format!("{e:?}")))?;
                                let cutoff = inner
                                    .store
                                    .tvg_retention_cutoff(
                                        id,
                                        policy.now,
                                        policy.history,
                                        owner.clone(),
                                    )
                                    .await
                                    .map_err(Error::Store)?;
                                inner
                                    .store
                                    .swap_tvg_programmes(id, cutoff, rows, owner.clone())
                                    .await
                                    .map_err(Error::Store)?;
                            }
                            // Source catches output-generation invalidation failures. Physical
                            // cache completion is fenced; raw operator diagnostic remains TODO.
                            let _caught_invalidation =
                                inner.cache.invalidate_output(owner.clone()).await;
                        }
                        Ok(Normal::Null)
                    }
                    .await;
                    let release = lease
                        .release_owned_detailed()
                        .await
                        .map_err(|e| Error::Lock(format!("{e:?}")))
                        .and_then(|owned| {
                            if owned {
                                Ok(())
                            } else {
                                Err(Error::Lock("owner lost".into()))
                            }
                        });
                    (body, release)
                }
            };
            // Source finally order is stop/release first, source counter decrement next.
            // Attempt decrement even after body/release error; retain all diagnostic
            // branches in the eventual whole-batch peer, not a fake SUCCESS policy.
            let leave = inner
                .cache
                .leave(context.source_id, owner.clone())
                .await
                .map_err(Error::Cache);
            match (body, release, leave) {
                (Ok(normal), Ok(()), Ok(())) => normal,
                (body, release, leave) => {
                    return Err(Error::AfterBody {
                        body: body.err().map(Box::new),
                        release: release.err().map(Box::new),
                        leave: leave.err().map(Box::new),
                    })
                }
            }
        }
    } else {
        Normal::MissingEpg
    };
    inner
        .jobs
        .write_normal(claimed, normal, owner)
        .await
        .map_err(Error::Transport)?;
    Ok(Run::Completed(task))
}
