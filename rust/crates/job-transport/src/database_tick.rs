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

fn adjust(value: f64) -> f64 {
    if value > 0.0 {
        value - 0.010
    } else {
        value
    }
}
fn delay(value: f64) -> f64 {
    adjust(value).min(5.0)
}
fn interval(entry: &Entry) -> Result<Interval, Error> {
    let (every, unit) = entry.interval.as_ref().ok_or(Error::Unsupported)?;
    let unit = match unit.as_str() {
        "days" => IntervalUnit::Days,
        "hours" => IntervalUnit::Hours,
        "minutes" => IntervalUnit::Minutes,
        "seconds" => IntervalUnit::Seconds,
        "microseconds" => IntervalUnit::Microseconds,
        _ => return Err(Error::Unsupported),
    };
    Interval::new((*every).try_into().map_err(|_| Error::Unsupported)?, unit)
        .map_err(|_| Error::Capacity)
}
fn validate_schedule(entry: &Entry) -> Result<(), Error> {
    if entry.interval.is_some() {
        interval(entry)?;
    } else {
        calendar_schedule::Calendar::parse(entry.crontab.as_ref().ok_or(Error::Unsupported)?)?;
    }
    Ok(())
}
fn decision(
    entry: &Entry,
    now: DateTime<Utc>,
) -> Result<crate::interval_schedule::DueDecision, Error> {
    let calendar = entry.interval.is_none();
    if calendar && entry.enabled {
        if let Some(start) = entry.start_time.filter(|start| now < *start) {
            let parsed = calendar_schedule::Calendar::parse(
                entry.crontab.as_ref().ok_or(Error::Unsupported)?,
            )?;
            let aligned = parsed.due_start(start, now)?;
            let micros =
                i128::from(aligned.timestamp_micros()) - i128::from(now.timestamp_micros());
            if micros.unsigned_abs() > 9_007_199_254_740_992 {
                return Err(Error::Capacity);
            }
            return Ok(crate::interval_schedule::DueDecision {
                due: false,
                next_seconds: (micros as f64 / 1_000_000.0).ceil(),
                persistence: PersistenceEffect::Unchanged,
            });
        }
    }
    let value = IntervalEntry {
        enabled: entry.enabled,
        // A zero interval with last=now extracts only the already-proven
        // ModelEntry start/expiry/oneoff guards before calendar calculation.
        last_run_at_micros: if calendar {
            now.timestamp_micros()
        } else {
            entry.last_run_at.timestamp_micros()
        },
        start_at_micros: entry.start_time.map(|v| v.timestamp_micros()),
        expires_at_micros: entry.expires.map(|v| v.timestamp_micros()),
        one_off: entry.one_off,
        total_run_count: entry
            .total_run_count
            .try_into()
            .map_err(|_| Error::Unsupported)?,
        interval: if calendar {
            Interval::new(0, IntervalUnit::Seconds).map_err(|_| Error::Capacity)?
        } else {
            interval(entry)?
        },
    }
    .evaluate(now.timestamp_micros())
    .map_err(|_| Error::Capacity)?;
    if !calendar || !value.due || value.persistence != PersistenceEffect::Unchanged {
        return Ok(value);
    }
    let parsed =
        calendar_schedule::Calendar::parse(entry.crontab.as_ref().ok_or(Error::Unsupported)?)?;
    let (due, next_seconds) = parsed.due(entry.last_run_at, now)?;
    Ok(crate::interval_schedule::DueDecision {
        due,
        next_seconds,
        persistence: PersistenceEffect::Unchanged,
    })
}

impl DatabaseSchedule {
    fn when(&self, next: f64) -> Result<f64, Error> {
        let now = (self.inner.heap_clock)();
        let value = now.timestamp() as f64
            + f64::from(now.timestamp_subsec_micros()) / 1_000_000.0
            + adjust(next);
        if !value.is_finite() {
            return Err(Error::Capacity);
        }
        Ok(value)
    }
    async fn evaluate(
        &self,
        state: &mut State,
        name: &str,
        now: DateTime<Utc>,
    ) -> Result<crate::interval_schedule::DueDecision, Error> {
        let mut entry = state.entries.get(name).cloned().ok_or(Error::Unsupported)?;
        let value = decision(&entry, now)?;
        if value.persistence != PersistenceEffect::Unchanged {
            let completed = value.persistence == PersistenceEffect::DisableCompletedOneOff;
            let count = if completed { 0 } else { entry.total_run_count };
            let mut transaction = self.inner.pool.begin().await?;
            let saved = sqlx::query("UPDATE django_celery_beat_periodictask SET enabled=false,last_run_at=NULL,total_run_count=$2,date_changed=$3 WHERE id=$1").bind(entry.id).bind(count).bind(now).execute(&mut *transaction).await;
            if let Err(error) = saved {
                transaction.rollback().await?;
                return Err(Error::Database(error));
            }
            if completed {
                let marker = sqlx::query("INSERT INTO django_celery_beat_periodictasks(ident,last_update) VALUES(1,$1) ON CONFLICT(ident) DO UPDATE SET last_update=EXCLUDED.last_update").bind(now).execute(&mut *transaction).await;
                if let Err(error) = marker {
                    transaction.rollback().await?;
                    return Err(Error::Database(error));
                }
            }
            transaction.commit().await?;
            entry.enabled = false;
            entry.total_run_count = count;
            state.entries.insert(name.to_owned(), entry);
        }
        Ok(value)
    }
    pub(super) async fn populate_locked(
        &self,
        state: &mut State,
        now: DateTime<Utc>,
    ) -> Result<(), Error> {
        // Fail before heap/model effects if this cohort exceeds the known
        // task authority. Installing defaults never grants their execution.
        for entry in state.entries.values() {
            validate_schedule(entry)?;
            if entry.enabled {
                if !matches!(
                    entry.task.as_str(),
                    "apps.epg.tasks.refresh_epg_data" | "apps.channels.tasks.evaluate_series_rules"
                ) {
                    return Err(Error::Unsupported);
                }
                // Expired/completed known rows are disabled by is_due without
                // publishing. Do not reject their unused publication options
                // before that captured Source state transition can settle.
                let value = decision(entry, now)?;
                if value.due && value.persistence == PersistenceEffect::Unchanged {
                    task(entry, Uuid::nil())?;
                }
            }
        }
        if !state.heap_initialized {
            state.heap.clear();
            let order = state.load_order.clone();
            for name in order {
                let value = self.evaluate(state, &name, now).await?;
                let when = self.when(if value.due { 0.0 } else { value.next_seconds })?;
                state.heap.push(heap::Event::new(when, name));
            }
            state.heap_initialized = true;
        }
        Ok(())
    }
    /// Draft bounded known-interval heap. Unknown task/options/calendar remain
    /// unsupported, and no application/default intake starts here.
    pub async fn tick(
        &self,
        now: DateTime<Utc>,
        publisher: Arc<dyn PeriodicPublisher>,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Tick, Error> {
        let this = self.clone();
        self.owned(caller, move |retained| async move {
            let mut state = this.inner.state.lock().await;
            this.refresh_locked(&mut state, now).await?;
            this.populate_locked(&mut state, now).await?;
            let Some(index) = heap::first(&state.heap) else {
                return Ok(Tick {
                    delay_seconds: 5.0,
                    publication: None,
                    sync: None,
                });
            };
            let name = state.heap[index].name.clone();
            let value = this.evaluate(&mut state, &name, now).await?;
            if !value.due {
                return Ok(Tick {
                    delay_seconds: delay(value.next_seconds),
                    publication: None,
                    sync: None,
                });
            }
            let _old_event = state.heap.swap_remove(index);
            let mut entry = state
                .entries
                .get(&name)
                .cloned()
                .ok_or(Error::Unsupported)?;
            let request = task(&entry, Uuid::new_v4())?;
            entry.total_run_count = entry
                .total_run_count
                .checked_add(1)
                .ok_or(Error::Capacity)?;
            entry.last_run_at = now;
            state
                .dirty
                .insert(name.clone(), (entry.id, now, entry.total_run_count));
            state.entries.insert(name.clone(), entry);
            let publication = publisher.publish(request, &name, retained).await;
            // Source applies the entry (including its sync-finally) before
            // sampling the clock to reinsert the newly reserved entry.
            let sync = if state
                .last_sync
                .is_none_or(|last| last.elapsed() > Duration::from_secs(180))
            {
                let report = this.sync_locked(&mut state).await?;
                state.last_sync = Some(std::time::Instant::now());
                Some(report)
            } else {
                None
            };
            let when = this.when(value.next_seconds)?;
            state.heap.push(heap::Event::new(when, name));
            Ok(Tick {
                delay_seconds: 0.0,
                publication: Some(publication),
                sync,
            })
        })
        .await
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
        let mut missing = HashMap::new();
        // Source uses an unordered set. No lexical-name ordering is invented.
        // Multi-dirty SQL-error partial-order equivalence is a separate gate.
        while let Some(name) = state.dirty.keys().next().cloned() {
            let snapshot = state.dirty.remove(&name).unwrap();
            let Some(current) = state.entries.get(&name) else {
                missing.insert(name, snapshot);
                report.missing += 1;
                continue;
            };
            if current.id != snapshot.0 {
                state.dirty.insert(name, snapshot);
                state.dirty.extend(missing);
                return Err(Error::Unsupported);
            }
            // ModelEntry.save reads the shared model, so a completed one-off's
            // count reset cannot be overwritten by its older dirty snapshot.
            let (id, last, count) = (current.id, current.last_run_at, current.total_run_count);
            let mut transaction = self.inner.pool.begin().await?;
            let result=sqlx::query("UPDATE django_celery_beat_periodictask SET last_run_at=CASE WHEN enabled THEN $2 ELSE NULL END,total_run_count=$3,date_changed=$4 WHERE id=$1").bind(id).bind(last).bind(count).bind(Utc::now()).execute(&mut *transaction).await;
            match result {
                Ok(rows) => {
                    transaction.commit().await?;
                    if rows.rows_affected() == 0 {
                        missing.insert(name, (id, last, count));
                        report.missing += 1
                    } else {
                        report.saved += 1
                    }
                }
                Err(sqlx::Error::Database(error)) => {
                    transaction.rollback().await?;
                    report.server_error = error.code().map(|v| v.chars().take(64).collect());
                    break;
                }
                Err(error) => {
                    let _ = transaction.rollback().await;
                    return Err(Error::Database(error));
                }
            }
        }
        state.dirty.extend(missing);
        Ok(report)
    }
}
