//! Bounded reserved index scheduling; app/driver acceptance remains separately gated.
//! Original Dispatcharr/Celery/Kombu authors retain task/transport algorithm credit.
use crate::{
    eta::Reservation,
    index_worker::{ChildPublisher, Error, IndexExecutor},
    transport::Jobs,
};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;
#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub active: usize,
    pub prefetch: usize,
    pub pending_eta: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            active: 1,
            prefetch: 4,
            pending_eta: 16,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Completed(Uuid),
    /// Handler executed; only rewriting an existing SUCCESS result was skipped.
    AlreadySucceeded(Uuid),
    Restored(Uuid),
}
struct Entry {
    seq: u64,
    reservation: Reservation,
    _owned: OwnedSemaphorePermit,
    _kind: OwnedSemaphorePermit,
    _caller: Arc<dyn Send + Sync>,
    done: watch::Sender<Option<Result<Outcome, Error>>>,
}
struct Inner {
    jobs: Jobs,
    executor: Arc<dyn IndexExecutor>,
    config: Config,
    owned: Arc<Semaphore>,
    active: Arc<Semaphore>,
    prefetch: Arc<Semaphore>,
    eta: Arc<Semaphore>,
    closed: AtomicBool,
    stop: watch::Sender<bool>,
    next: AtomicU64,
    pending: Mutex<BTreeMap<u64, Arc<Entry>>>,
    restore_failures: Mutex<Vec<Error>>,
}
#[derive(Clone)]
pub struct ScheduledIndexWorker {
    inner: Arc<Inner>,
}
pub struct Job {
    done: watch::Receiver<Option<Result<Outcome, Error>>>,
}
impl Job {
    pub async fn wait(mut self) -> Result<Outcome, Error> {
        let value = self
            .done
            .wait_for(Option::is_some)
            .await
            .map_err(|_| Error::WorkerPanic)?;
        value.expect("completed watch value")
    }
}
impl ScheduledIndexWorker {
    pub fn new(
        jobs: Jobs,
        executor: Arc<dyn IndexExecutor>,
        config: Config,
    ) -> Result<Self, Error> {
        if config.active == 0
            || config.active > 4
            || config.prefetch < config.active
            || config.prefetch > 16
            || config.pending_eta == 0
            || config.pending_eta > 128
        {
            return Err(Error::Capacity);
        }
        let (stop, _) = watch::channel(false);
        Ok(Self {
            inner: Arc::new(Inner {
                jobs,
                executor,
                config,
                owned: Arc::new(Semaphore::new(config.prefetch + config.pending_eta)),
                active: Arc::new(Semaphore::new(config.active)),
                prefetch: Arc::new(Semaphore::new(config.prefetch)),
                eta: Arc::new(Semaphore::new(config.pending_eta)),
                closed: AtomicBool::new(false),
                stop,
                next: AtomicU64::new(1),
                pending: Mutex::new(BTreeMap::new()),
                restore_failures: Mutex::new(Vec::new()),
            }),
        })
    }
    pub fn available_eta(&self) -> usize {
        self.inner.eta.available_permits()
    }
    pub fn available_prefetch(&self) -> usize {
        self.inner.prefetch.available_permits()
    }
    /// Caller retains the original receipt on admission failure. Accepted work
    /// is supervised; dropping Job never cancels an accepted task.
    pub fn start(
        &self,
        reservation: &Reservation,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Job, Error> {
        self.inner
            .jobs
            .owns_reservation(reservation)
            .map_err(Error::Transport)?;
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let owned = self
            .inner
            .owned
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        let kind = if reservation.message().eta.is_some() {
            self.inner.eta.clone()
        } else {
            self.inner.prefetch.clone()
        };
        let kind = kind.try_acquire_owned().map_err(|_| Error::Capacity)?;
        let seq = self.inner.next.fetch_add(1, Ordering::Relaxed);
        let (done, receiver) = watch::channel(None);
        let entry = Arc::new(Entry {
            seq,
            reservation: reservation.clone(),
            _owned: owned,
            _kind: kind,
            _caller: caller,
            done,
        });
        {
            let mut pending = self.inner.pending.lock().unwrap();
            if self.inner.closed.load(Ordering::Acquire) {
                return Err(Error::Closed);
            }
            pending.insert(seq, entry.clone());
        }
        let inner = self.inner.clone();
        tokio::spawn(async move {
            execute(inner, entry).await;
        });
        Ok(Job { done: receiver })
    }
    /// Restore unstarted reservations in arrival order (Source119 warm order),
    /// drain active tasks, and leave final shared Jobs close to the caller.
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), Error> {
        let deadline = tokio::time::Instant::now() + timeout;
        let pending = {
            let mut pending = self.inner.pending.lock().unwrap();
            self.inner.closed.store(true, Ordering::Release);
            self.inner.stop.send_replace(true);
            std::mem::take(&mut *pending)
        };
        if !pending.is_empty() {
            let inner = self.inner.clone();
            tokio::spawn(async move {
                for entry in pending.into_values() {
                    let outcome = inner
                        .jobs
                        .restore_index_eta(&entry.reservation, entry.clone())
                        .await
                        .map(|()| Outcome::Restored(entry.reservation.message().task_id))
                        .map_err(Error::Transport);
                    if let Err(error) = outcome {
                        inner.restore_failures.lock().unwrap().push(error);
                    }
                    entry.done.send_replace(Some(outcome));
                }
            });
        }
        let all = self.inner.owned.clone().acquire_many_owned(
            (self.inner.config.prefetch + self.inner.config.pending_eta) as u32,
        );
        let _held = tokio::time::timeout(
            deadline.saturating_duration_since(tokio::time::Instant::now()),
            all,
        )
        .await
        .map_err(|_| Error::DrainTimeout)?
        .map_err(|_| Error::Closed)?;
        if let Some(error) = self.inner.restore_failures.lock().unwrap().first().copied() {
            return Err(error);
        }
        Ok(())
    }
}
async fn stopped(entry: &Entry) {
    let mut done = entry.done.subscribe();
    let _ = done.wait_for(Option::is_some).await;
}
async fn execute(inner: Arc<Inner>, entry: Arc<Entry>) {
    let mut stop = inner.stop.subscribe();
    if *stop.borrow() {
        stopped(&entry).await;
        return;
    }
    if let Some(eta) = entry.reservation.message().eta {
        let wait = (eta - chrono::Utc::now())
            .to_std()
            .unwrap_or(Duration::ZERO);
        tokio::select! {_ = tokio::time::sleep(wait)=>{},_ = stop.changed()=>{stopped(&entry).await;return}}
    }
    let active = tokio::select! {p=inner.active.clone().acquire_owned()=>match p{Ok(p)=>p,Err(_)=>{entry.done.send_replace(Some(Err(Error::Closed)));return}},_ = stop.changed()=>{stopped(&entry).await;return}};
    let restore = {
        let mut pending = inner.pending.lock().unwrap();
        if inner.closed.load(Ordering::Acquire) {
            true
        } else {
            pending.remove(&entry.seq);
            false
        }
    };
    if restore {
        drop(active);
        stopped(&entry).await;
        return;
    }
    let result = run_active(&inner, &entry).await;
    entry.done.send_replace(Some(result));
    drop(active);
}
async fn run_active(inner: &Inner, entry: &Arc<Entry>) -> Result<Outcome, Error> {
    // The queue item remains unacked while awaiting ETA/pool capacity. Pool
    // acceptance confirms ACK before Source's track_startedFalse handler runs.
    inner
        .jobs
        .ack_index_eta(&entry.reservation, entry.clone())
        .await
        .map_err(Error::Transport)?;
    let message = entry.reservation.message().clone();
    let publisher = ChildPublisher::context(inner.jobs.clone(), message.clone(), entry.clone());
    inner
        .executor
        .execute(message.clone(), publisher.clone(), entry.clone())
        .await?;
    match inner
        .jobs
        .write_index_success(
            message.task_id,
            message.parent,
            publisher.completed_children()?,
            entry.clone(),
        )
        .await
        .map_err(Error::Transport)?
    {
        true => Ok(Outcome::Completed(message.task_id)),
        false => Ok(Outcome::AlreadySucceeded(message.task_id)),
    }
}
