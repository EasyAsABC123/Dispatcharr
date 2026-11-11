//! Known single-enabled-interval tick, SQL sync and actual typed publication.
//! Original Dispatcharr/Celery/django-celery-beat authors retain credit/licenses.
//! No timer/default intake, heap tie ordering or calendar execution authority.
use super::*;
use crate::{
    interval_schedule::{Interval, IntervalEntry, IntervalUnit, PersistenceEffect},
    protocol::Published,
    refresh_protocol::{RefreshCall, RefreshMessage},
    series_protocol::SeriesMessage,
    transport::{Error as PublishError, Jobs},
};
use std::{future::Future, pin::Pin};
use uuid::Uuid;
#[derive(Clone, Debug)]
pub enum KnownTask {
    Refresh(RefreshMessage),
    Series(SeriesMessage),
}
pub type PublishFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Published, PublishError>> + Send + 'a>>;
pub trait PeriodicPublisher: Send + Sync {
    fn publish<'a>(
        &'a self,
        task: KnownTask,
        name: &'a str,
        caller: Arc<dyn Send + Sync>,
    ) -> PublishFuture<'a>;
}
/// Each constructor is separately owned; closing a family consumer must not
/// close this producer while a supervised tick still needs its broker reply.
pub struct KnownPublisher {
    pub refresh: Jobs,
    pub series: Jobs,
}
impl PeriodicPublisher for KnownPublisher {
    fn publish<'a>(
        &'a self,
        task: KnownTask,
        name: &'a str,
        caller: Arc<dyn Send + Sync>,
    ) -> PublishFuture<'a> {
        Box::pin(async move {
            match task {
                KnownTask::Refresh(v) => {
                    self.refresh
                        .publish_periodic_refresh(&v, name, caller)
                        .await
                }
                KnownTask::Series(v) => self.series.publish_periodic_series(&v, name, caller).await,
            }
        })
    }
}
#[derive(Clone, Debug, Default)]
pub struct SyncReport {
    pub saved: usize,
    pub missing: usize,
    pub server_error: Option<String>,
}
#[derive(Clone, Debug)]
pub struct Tick {
    pub delay_seconds: f64,
    pub publication: Option<Result<Published, PublishError>>,
    pub sync: Option<SyncReport>,
}
fn task(entry: &Entry, id: Uuid) -> Result<KnownTask, Error> {
    if entry.queue.is_some()
        || entry.exchange.is_some()
        || entry.routing_key.is_some()
        || entry.priority.is_some()
        || entry.expire_seconds.is_some()
        || entry.expires.is_some()
        || !entry.headers.as_object().is_some_and(|v| v.is_empty())
    {
        return Err(Error::Unsupported);
    }
    match entry.task.as_str() {
        "apps.epg.tasks.refresh_epg_data" => {
            let call = RefreshCall {
                args: entry.args.as_array().ok_or(Error::Unsupported)?.clone(),
                kwargs: entry.kwargs.as_object().ok_or(Error::Unsupported)?.clone(),
                list_args: true,
            };
            let (source_id, force, retry) = call.resolve().map_err(|_| Error::Unsupported)?;
            Ok(KnownTask::Refresh(RefreshMessage {
                task_id: id,
                source_id,
                force,
                retry,
                root: id,
                parent: None,
                eta: None,
                call,
            }))
        }
        "apps.channels.tasks.evaluate_series_rules"
            if entry.args == serde_json::json!([]) && entry.kwargs == serde_json::json!({}) =>
        {
            Ok(KnownTask::Series(SeriesMessage {
                task_id: id,
                root: id,
                parent: None,
            }))
        }
        _ => Err(Error::Unsupported),
    }
}
fn delay(value: f64) -> f64 {
    (if value > 0.0 { value - 0.010 } else { value }).min(5.0)
}
impl DatabaseSchedule {
    /// Captured Source supports one publication per tick. This first native lane
    /// requires exactly zero/one enabled interval entry; arbitrary heap ordering,
    /// unsupported families/options and calendar entries are explicit failures.
    pub async fn tick(
        &self,
        now: DateTime<Utc>,
        publisher: Arc<dyn PeriodicPublisher>,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Tick, Error> {
        let this = self.clone();
        self.owned(caller,move |retained| async move{
   let mut state=this.inner.state.lock().await;this.refresh_locked(&mut state,now).await?;
   if state.entries.is_empty(){return Ok(Tick{delay_seconds:5.0,publication:None,sync:None})}
   if state.entries.len()!=1{return Err(Error::Unsupported)}
   let mut entry=state.entries.values().next().unwrap().clone();let(every,unit)=entry.interval.as_ref().ok_or(Error::Unsupported)?;
   let unit=match unit.as_str(){"days"=>IntervalUnit::Days,"hours"=>IntervalUnit::Hours,"minutes"=>IntervalUnit::Minutes,"seconds"=>IntervalUnit::Seconds,"microseconds"=>IntervalUnit::Microseconds,_=>return Err(Error::Unsupported)};
   let interval=Interval::new((*every).try_into().map_err(|_|Error::Unsupported)?,unit).map_err(|_|Error::Capacity)?;
   let mut bootstrap=!state.heap_initialized;state.heap_initialized=true;
   loop {
   let decision=IntervalEntry{enabled:entry.enabled,last_run_at_micros:entry.last_run_at.timestamp_micros(),start_at_micros:entry.start_time.map(|v|v.timestamp_micros()),expires_at_micros:entry.expires.map(|v|v.timestamp_micros()),one_off:entry.one_off,total_run_count:entry.total_run_count.try_into().map_err(|_|Error::Unsupported)?,interval}.evaluate(now.timestamp_micros()).map_err(|_|Error::Capacity)?;
   if decision.persistence!=PersistenceEffect::Unchanged {
    let completed=decision.persistence==PersistenceEffect::DisableCompletedOneOff;let count=if completed{0}else{entry.total_run_count};let mut tx=this.inner.pool.begin().await?;
    let saved=sqlx::query("UPDATE django_celery_beat_periodictask SET enabled=false,last_run_at=NULL,total_run_count=$2,date_changed=$3 WHERE id=$1").bind(entry.id).bind(count).bind(now).execute(&mut *tx).await;
    if let Err(error)=saved{tx.rollback().await?;return Err(Error::Database(error))}
    if completed {let changed=sqlx::query("INSERT INTO django_celery_beat_periodictasks(ident,last_update) VALUES(1,$1) ON CONFLICT(ident) DO UPDATE SET last_update=EXCLUDED.last_update").bind(now).execute(&mut *tx).await;if let Err(error)=changed{tx.rollback().await?;return Err(Error::Database(error))}}
    tx.commit().await?;entry.enabled=false;entry.total_run_count=count;state.entries.insert(entry.name.clone(),entry.clone());if bootstrap{bootstrap=false;continue}return Ok(Tick{delay_seconds:delay(decision.next_seconds),publication:None,sync:None})
   }
   // Actual Celery populate_heap calls is_due once before the tick itself.
   // This matters for first-read expired/completed-oneoff: the second check
   // sees disabled and returns5, yielding4.99 after the captured drift.
   if bootstrap{bootstrap=false;continue}
   if !decision.due{return Ok(Tick{delay_seconds:delay(decision.next_seconds),publication:None,sync:None})}
   let id=Uuid::new_v4();let request=task(&entry,id)?;
   // ModelEntry.next mutates the cached model before any broker work. Persist
   // the cached count, never SQL count+1; independent schedulers remain stale.
   entry.total_run_count=entry.total_run_count.checked_add(1).ok_or(Error::Capacity)?;entry.last_run_at=now;
   state.dirty.insert(entry.name.clone(),(entry.id,now,entry.total_run_count));state.entries.insert(entry.name.clone(),entry.clone());
   let publication=publisher.publish(request,&entry.name,retained).await;
   let sync=if state.last_sync.is_none_or(|last|last.elapsed()>Duration::from_secs(180)){let report=this.sync_locked(&mut state).await?;state.last_sync=Some(std::time::Instant::now());Some(report)}else{None};
   return Ok(Tick{delay_seconds:0.0,publication:Some(publication),sync})
   }
  }).await
    }
    pub async fn sync(&self, caller: Arc<dyn Send + Sync>) -> Result<SyncReport, Error> {
        let this = self.clone();
        self.owned(caller, move |_retained| async move {
            let mut state = this.inner.state.lock().await;
            this.sync_locked(&mut state).await
        })
        .await
    }
    pub(super) async fn sync_locked(&self, state: &mut State) -> Result<SyncReport, Error> {
        let mut report = SyncReport::default();
        let mut missing = BTreeMap::new();
        // Only the single-entry lane is supported; Source multi-name set-pop order
        // and delete/recreate-name races require their own captured execution gates.
        if state.dirty.len() > 1 {
            return Err(Error::Unsupported);
        }
        while let Some((name, (id, last, count))) = state.dirty.pop_first() {
            let mut tx = self.inner.pool.begin().await?;
            let result=sqlx::query("UPDATE django_celery_beat_periodictask SET last_run_at=CASE WHEN enabled THEN $2 ELSE NULL END,total_run_count=$3,date_changed=$4 WHERE id=$1").bind(id).bind(last).bind(count).bind(Utc::now()).execute(&mut *tx).await;
            match result {
                Ok(rows) => {
                    tx.commit().await?;
                    if rows.rows_affected() == 0 {
                        missing.insert(name, (id, last, count));
                        report.missing += 1
                    } else {
                        report.saved += 1
                    }
                }
                Err(sqlx::Error::Database(error)) => {
                    tx.rollback().await?;
                    report.server_error = error.code().map(|v| v.chars().take(64).collect());
                    break;
                }
                Err(error) => {
                    let _ = tx.rollback().await;
                    return Err(Error::Database(error));
                }
            }
        }
        state.dirty.extend(missing);
        Ok(report)
    }
}
