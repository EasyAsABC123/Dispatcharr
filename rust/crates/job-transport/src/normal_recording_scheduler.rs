//! Actual bounded clocked-recording tick/reserve/publish/SQL-sync worker.
//! Original Dispatcharr/Celery/django-celery-beat authors retain all credit.
//! Explicit clocked-recording subset only: this does not install/claim all Beat
//! schedules, interval/crontab families, cross-process election or heap tie order.
use crate::normal_recording_publication::{Error as PublicationError, NormalRecordingPublisher};
use chrono::{DateTime, Utc};
use sqlx::{Connection, PgPool, Row};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore};
#[derive(Debug)]
pub enum Error {
    Configuration,
    Closed,
    Capacity,
    Publication(PublicationError),
    Database {
        primary: Option<Box<sqlx::Error>>,
        ready: Option<Box<sqlx::Error>>,
        close: Option<Box<sqlx::Error>>,
    },
    Supervisor,
    Drain,
}
#[derive(Clone, Debug)]
pub enum State {
    Prepared,
    Running,
    Observed(Tick),
    Fault(Arc<Error>),
    Stopped,
}
#[derive(Clone, Debug)]
pub struct Tick {
    pub disabled_completed_one_off: u64,
    pub published: Option<uuid::Uuid>,
    pub synchronized_rows: u64,
    pub schedule_removed_before_sync: bool,
}
#[derive(Clone, Copy)]
struct Dirty {
    periodic: i64,
    last_run_at: DateTime<Utc>,
}
struct Inner {
    pool: PgPool,
    publisher: NormalRecordingPublisher,
    slots: Arc<Semaphore>,
    dirty: Mutex<BTreeMap<i64, Dirty>>,
    closed: AtomicBool,
    started: AtomicBool,
    stop: watch::Sender<bool>,
    done: watch::Sender<bool>,
    state: watch::Sender<State>,
    poll: Duration,
}
struct Owner {
    _caller: Arc<dyn Send + Sync>,
    _slot: OwnedSemaphorePermit,
    _scheduler: Arc<Inner>,
}
/// One explicitly configured scheduler instance. Root must ensure the original
/// single-Beat-process topology; this module does not invent distributed locks.
#[derive(Clone)]
pub struct Scheduler {
    inner: Arc<Inner>,
}
impl Scheduler {
    pub fn prepare(
        pool: PgPool,
        publisher: NormalRecordingPublisher,
        poll: Duration,
    ) -> Result<Self, Error> {
        if !publisher.uses_pool(&pool)
            || !(Duration::from_millis(25)..=Duration::from_secs(5)).contains(&poll)
        {
            return Err(Error::Configuration);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                pool,
                publisher,
                slots: Arc::new(Semaphore::new(1)),
                dirty: Mutex::new(BTreeMap::new()),
                closed: AtomicBool::new(false),
                started: AtomicBool::new(false),
                stop: watch::channel(false).0,
                done: watch::channel(true).0,
                state: watch::channel(State::Prepared).0,
                poll,
            }),
        })
    }
    pub fn state(&self) -> State {
        self.inner.state.borrow().clone()
    }
    pub fn start(&self, caller: Arc<dyn Send + Sync>) -> Result<(), Error> {
        if self.inner.closed.load(Ordering::Acquire)
            || self.inner.started.swap(true, Ordering::AcqRel)
        {
            return Err(Error::Closed);
        }
        self.inner.done.send_replace(false);
        self.inner.state.send_replace(State::Running);
        let scheduler = self.clone();
        let mut stop = self.inner.stop.subscribe();
        tokio::spawn(async move {
            while !*stop.borrow() {
                match scheduler.tick_once(caller.clone()).await {
                    Ok(tick) => {
                        scheduler.inner.state.send_replace(State::Observed(tick));
                    }
                    Err(error) => {
                        scheduler
                            .inner
                            .state
                            .send_replace(State::Fault(Arc::new(error)));
                        scheduler.inner.done.send_replace(true);
                        return;
                    }
                }
                tokio::select! {_=stop.changed()=>{},_=tokio::time::sleep(scheduler.inner.poll)=>{}}
            }
            scheduler.inner.state.send_replace(State::Stopped);
            scheduler.inner.done.send_replace(true);
        });
        Ok(())
    }
    pub async fn tick_once(&self, caller: Arc<dyn Send + Sync>) -> Result<Tick, Error> {
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
            _scheduler: self.inner.clone(),
        });
        let inner = self.inner.clone();
        // Cancelling the observer does not erase an actual broker publication or let
        // shared ports close before its SQL sync/Ready/physical connection close.
        tokio::spawn(async move {
            let pending: Vec<Dirty> = inner
                .dirty
                .lock()
                .map_err(|_| Error::Supervisor)?
                .values()
                .copied()
                .collect();
            let mut synchronized = 0;
            let mut removed = false;
            for dirty in pending {
                let count = sync(inner.pool.clone(), dirty, owner.clone()).await?;
                synchronized += count;
                removed |= count == 0;
                inner
                    .dirty
                    .lock()
                    .map_err(|_| Error::Supervisor)?
                    .remove(&dirty.periodic);
            }
            let (disabled, periodic) = next(inner.pool.clone(), owner.clone()).await?;
            let Some(periodic) = periodic else {
                return Ok(Tick {
                    disabled_completed_one_off: disabled,
                    published: None,
                    synchronized_rows: synchronized,
                    schedule_removed_before_sync: removed,
                });
            };
            // Source ModelEntry.__next__ reserves cached run state before apply_async.
            // Real SQL is synchronized only after the publication receipt. Ambiguous
            // publication keeps dirty state and faults; never publish that head again.
            let dirty = Dirty {
                periodic,
                last_run_at: Utc::now(),
            };
            inner
                .dirty
                .lock()
                .map_err(|_| Error::Supervisor)?
                .insert(periodic, dirty);
            let published = inner
                .publisher
                .publish_due(periodic, owner.clone())
                .await
                .map_err(Error::Publication)?;
            let count = sync(inner.pool.clone(), dirty, owner).await?;
            synchronized += count;
            removed |= count == 0;
            inner
                .dirty
                .lock()
                .map_err(|_| Error::Supervisor)?
                .remove(&periodic);
            Ok(Tick {
                disabled_completed_one_off: disabled,
                published: Some(published.task_id),
                synchronized_rows: synchronized,
                schedule_removed_before_sync: removed,
            })
        })
        .await
        .map_err(|_| Error::Supervisor)?
    }
    pub fn stop_admission(&self) {
        self.inner.closed.store(true, Ordering::Release);
        self.inner.stop.send_replace(true);
    }
    /// Root stops this producer before consumer drain and closes the shared normal
    /// publisher/ports last. A timeout is not successful physical settlement.
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), Error> {
        self.stop_admission();
        let deadline = tokio::time::Instant::now() + timeout;
        let mut done = self.inner.done.subscribe();
        tokio::time::timeout_at(deadline, async {
            while !*done.borrow_and_update() {
                done.changed().await.map_err(|_| Error::Drain)?;
            }
            Ok::<(), Error>(())
        })
        .await
        .map_err(|_| Error::Drain)??;
        let _quiet = tokio::time::timeout_at(deadline, self.inner.slots.clone().acquire_owned())
            .await
            .map_err(|_| Error::Drain)?
            .map_err(|_| Error::Drain)?;
        if !self
            .inner
            .dirty
            .lock()
            .map_err(|_| Error::Supervisor)?
            .is_empty()
        {
            return Err(Error::Drain);
        }
        Ok(())
    }
}
fn database(error: sqlx::Error) -> Error {
    Error::Database {
        primary: Some(Box::new(error)),
        ready: None,
        close: None,
    }
}
async fn next(pool: PgPool, owner: Arc<dyn Send + Sync>) -> Result<(u64, Option<i64>), Error> {
    tokio::spawn(async move {let _owner=owner;let mut connection=pool.acquire().await.map_err(database)?.detach();
  let observed=async {
   // Source completed one-off disables/reset count, preserving last_run_at.
   // The bounded CTE keeps each tick constant-size without dropping other rows.
   let disabled=sqlx::query("WITH chosen AS (SELECT id FROM django_celery_beat_periodictask WHERE enabled AND one_off AND total_run_count>0 AND task='apps.channels.tasks.run_recording' AND clocked_id IS NOT NULL ORDER BY id LIMIT 64) UPDATE django_celery_beat_periodictask p SET enabled=false,total_run_count=0,date_changed=$1 FROM chosen WHERE p.id=chosen.id").bind(Utc::now()).execute(&mut connection).await.map_err(database)?.rows_affected();
   if disabled>0 {sqlx::query("INSERT INTO django_celery_beat_periodictasks (ident,last_update) VALUES (1,$1) ON CONFLICT (ident) DO UPDATE SET last_update=EXCLUDED.last_update").bind(Utc::now()).execute(&mut connection).await.map_err(database)?;}
   let row=sqlx::query("SELECT p.id::bigint FROM django_celery_beat_periodictask p JOIN django_celery_beat_clockedschedule c ON c.id=p.clocked_id WHERE p.enabled AND p.one_off AND p.total_run_count=0 AND p.task='apps.channels.tasks.run_recording' AND c.clocked_time<=$1 AND (p.start_time IS NULL OR p.start_time<=$1) ORDER BY c.clocked_time,p.id LIMIT 1").bind(Utc::now()).fetch_optional(&mut connection).await.map_err(database)?;
   Ok((disabled,row.map(|row|row.try_get::<i64,_>("id")).transpose().map_err(database)?))
  }.await;
  let ready=connection.ping().await.err().map(Box::new);let close=connection.close().await.err().map(Box::new);
  match observed {Err(Error::Database{primary,..})=>Err(Error::Database{primary,ready,close}),other=>if ready.is_some()||close.is_some(){Err(Error::Database{primary:None,ready,close})}else{other}}
 }).await.map_err(|_|Error::Supervisor)?
}
async fn sync(pool: PgPool, dirty: Dirty, owner: Arc<dyn Send + Sync>) -> Result<u64, Error> {
    tokio::spawn(async move {let _owner=owner;let mut connection=pool.acquire().await.map_err(database)?.detach();
  let observed=sqlx::query("UPDATE django_celery_beat_periodictask SET last_run_at=$1,total_run_count=1 WHERE id=$2 AND task='apps.channels.tasks.run_recording' AND one_off AND clocked_id IS NOT NULL").bind(dirty.last_run_at).bind(dirty.periodic).execute(&mut connection).await;
  let ready=connection.ping().await.err().map(Box::new);let close=connection.close().await.err().map(Box::new);
  match observed {Err(error)=>Err(Error::Database{primary:Some(Box::new(error)),ready,close}),Ok(value)=>if ready.is_some()||close.is_some(){Err(Error::Database{primary:None,ready,close})}else{Ok(value.rows_affected())}}
 }).await.map_err(|_|Error::Supervisor)?
}
