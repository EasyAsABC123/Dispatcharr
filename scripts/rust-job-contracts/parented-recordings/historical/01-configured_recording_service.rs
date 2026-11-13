//! PRIVATE uncompiled configured-Series/UTC-recurring service draft.
//! Normal inert library path; no task consumer or Main constructor is changed.
//! Original Dispatcharr authors retain rule/signal/event behavior credit.
use crate::configured_recordings::{self as decisions, Existing, Intent, Programme, Recurring};
use crate::recording_artwork::DirectTask;
use chrono::{DateTime, Utc};
use dispatcharr_contracts::{BoxFuture, UpdateEventError, UpdateEventSink};
use dispatcharr_store::recording_schedule::{self, FutureRecording, SeriesScope};
use serde_json::{json, Value};
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::oneshot;
use uuid::Uuid;

#[derive(Debug)]
pub enum Error {
    Unsupported,
    Snapshot(recording_schedule::SnapshotError),
    Sql(recording_schedule::Error),
    ArtworkUncertain,
    Event,
    Supervisor,
}
#[derive(Clone, Copy, Debug)]
pub enum ArtworkError {
    SettledUnavailable,
    Uncertain,
}
/// Only captured prefetch_recording_artwork([positive Recording id],countdown=1)
/// authority. Implementation must await physical publication settlement and
/// retain owner through actual reply. Arbitrary task names/options are absent.
pub trait ArtworkSink: Send + Sync {
    fn publish<'a>(
        &'a self,
        recording: i64,
        task: DirectTask,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'a, Result<Uuid, ArtworkError>>;
}
pub struct Service {
    pool: PgPool,
    artwork: Arc<dyn ArtworkSink>,
    events: Arc<dyn UpdateEventSink>,
    identity: Arc<()>,
}
pub struct PreparedSeries {
    origin: Arc<()>,
    task: DirectTask,
    title: String,
    tvg: String,
    intents: Vec<Intent>,
}
#[derive(Debug)]
pub struct SeriesReport {
    task: Uuid,
    root: Uuid,
    parent: Option<Uuid>,
    title: String,
    tvg: String,
    created: u32,
    children: Vec<Uuid>,
    caught_insert_errors: Vec<String>,
}
impl SeriesReport {
    pub(crate) fn matches(&self, message: &crate::series_protocol::SeriesMessage) -> bool {
        self.task_id() == message.task_id
            && self.root == message.root
            && self.parent == message.parent
    }

    pub(crate) fn task_id(&self) -> Uuid {
        self.task
    }
    pub(crate) fn children(&self) -> Vec<Uuid> {
        self.children.clone()
    }
    pub fn value(&self) -> Value {
        let mut details: Vec<Value> = self
            .caught_insert_errors
            .iter()
            .map(|error| json!({"tvg_id":self.tvg,"status":"error","error":error}))
            .collect();
        details.push(
            json!({"tvg_id":self.tvg,"title":self.title,"status":"ok","created":self.created}),
        );
        json!({"scheduled":self.created,"details":details})
    }
}
#[derive(Debug)]
pub struct RecurringReport {
    task: Uuid,
    created: u32,
    children: Vec<Uuid>,
}
impl RecurringReport {
    pub(crate) fn task_id(&self) -> Uuid {
        self.task
    }
    pub(crate) fn children(&self) -> Vec<Uuid> {
        self.children.clone()
    }
    pub fn value(&self) -> Value {
        json!(self.created)
    }
}
struct IntentOutcome {
    created: u32,
    children: Vec<Uuid>,
    caught_insert_errors: Vec<recording_schedule::KnownInsertFailure>,
}
pub struct PreparedRecurring {
    origin: Arc<()>,
    task: DirectTask,
    decision_now: DateTime<Utc>,
    rule: Option<Recurring>,
}
struct EventReceipt {
    finished: Option<oneshot::Sender<()>>,
    _owner: Arc<dyn Send + Sync>,
}
impl Drop for EventReceipt {
    fn drop(&mut self) {
        if let Some(tx) = self.finished.take() {
            let _ = tx.send(());
        }
    }
}
fn small(v: &Value) -> Result<&str, Error> {
    v.as_str()
        .filter(|s| {
            !s.is_empty()
                && s.len() <= 255
                && s.is_ascii()
                && !s.bytes().any(|b| b.is_ascii_control())
        })
        .ok_or(Error::Unsupported)
}
fn moment(v: &Value) -> Result<DateTime<Utc>, Error> {
    v.as_str()
        .filter(|s| s.len() <= 40)
        .ok_or(Error::Unsupported)?
        .parse()
        .map_err(|_| Error::Unsupported)
}
fn number(v: &Value) -> Result<u32, Error> {
    v.as_u64()
        .and_then(|v| u32::try_from(v).ok())
        .filter(|v| *v > 0)
        .ok_or(Error::Unsupported)
}
impl Service {
    /// Inert assembly only. The worker must use a matching retained policy/SQL
    /// pool and keep its global lease until all service/result work settles.
    pub fn new(
        pool: PgPool,
        artwork: Arc<dyn ArtworkSink>,
        events: Arc<dyn UpdateEventSink>,
    ) -> Self {
        Self {
            pool,
            artwork,
            events,
            identity: Arc::new(()),
        }
    }
    pub async fn prepare_series(
        &self,
        group: &Value,
        now: DateTime<Utc>,
        task: DirectTask,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<PreparedSeries, Error> {
        if group.get("pre_offset_minutes").and_then(Value::as_i64) != Some(0)
            || group.get("post_offset_minutes").and_then(Value::as_i64) != Some(0)
        {
            return Err(Error::Unsupported);
        }
        let rules = group
            .get("series_rules")
            .and_then(Value::as_array)
            .filter(|v| v.len() == 1)
            .ok_or(Error::Unsupported)?;
        let rule = rules[0].as_object().ok_or(Error::Unsupported)?;
        let keys = [
            "title",
            "tvg_id",
            "title_mode",
            "mode",
            "channel_id",
            "epg_source_id",
        ];
        if rule.len() != keys.len()
            || rule.keys().any(|k| !keys.contains(&k.as_str()))
            || rule.get("title_mode") != Some(&json!("exact"))
            || rule.get("mode") != Some(&json!("all"))
        {
            return Err(Error::Unsupported);
        }
        let title = small(&rules[0]["title"])?.to_owned();
        let tvg = small(&rules[0]["tvg_id"])?.to_owned();
        // Source strips these rule values; this first domain refuses noncanonical
        // spacing rather than silently accepting a broader normalization domain.
        if title.trim() != title || tvg.trim() != tvg {
            return Err(Error::Unsupported);
        }
        let channel = rules[0]["channel_id"].as_i64().ok_or(Error::Unsupported)?;
        let source = rules[0]["epg_source_id"]
            .as_i64()
            .ok_or(Error::Unsupported)?;
        let scope = SeriesScope::new(channel, source, tvg.clone(), title.clone())
            .ok_or(Error::Unsupported)?;
        let snapshot = recording_schedule::series_snapshot(self.pool.clone(), scope, now, owner)
            .await
            .map_err(Error::Snapshot)?;
        let mut programmes = Vec::new();
        for row in snapshot.programmes {
            let cp = row.custom_properties.ok_or(Error::Unsupported)?;
            let cp = cp.as_object().ok_or(Error::Unsupported)?;
            if cp.len() != 2
                || !cp.contains_key("season")
                || !cp.contains_key("episode")
                || row.description.is_some()
            {
                return Err(Error::Unsupported);
            }
            programmes.push(Programme {
                id: row.id,
                source_id: source,
                channel_id: channel,
                tvg_id: row.tvg_id.ok_or(Error::Unsupported)?,
                title: row.title.ok_or(Error::Unsupported)?,
                subtitle: row.sub_title.ok_or(Error::Unsupported)?,
                start: row.start_time,
                end: row.end_time,
                season: number(&cp["season"])?,
                episode: number(&cp["episode"])?,
            });
        }
        let mut existing = Vec::new();
        for cp in snapshot.existing_properties {
            if cp.is_null() {
                continue;
            }
            let object = cp.as_object().ok_or(Error::Unsupported)?;
            let Some(program) = object.get("program") else {
                continue;
            };
            let program = program.as_object().ok_or(Error::Unsupported)?;
            // Recurring Source snapshots have no tvg_id; Source ignores them
            // for programme/episode dedup. Unknown non-string keys are refused.
            let Some(key) = program.get("tvg_id") else {
                continue;
            };
            if key.is_null() {
                continue;
            }
            let key = small(key)?;
            if key != tvg {
                continue;
            }
            existing.push(Existing {
                tvg_id: key.to_owned(),
                title: small(program.get("title").ok_or(Error::Unsupported)?)?.to_owned(),
                start: moment(program.get("start_time").ok_or(Error::Unsupported)?)?,
                end: moment(program.get("end_time").ok_or(Error::Unsupported)?)?,
                season: number(program.get("season").ok_or(Error::Unsupported)?)?,
                episode: number(program.get("episode").ok_or(Error::Unsupported)?)?,
            });
        }
        let intents = decisions::series(now, &title, &tvg, channel, source, &programmes, &existing)
            .map_err(|_| Error::Unsupported)?;
        Ok(PreparedSeries {
            origin: self.identity.clone(),
            task,
            title,
            tvg,
            intents,
        })
    }
    /// `system` must be the actual immutable group returned by retained_system_settings
    /// under the SAME worker owner. No SQL/default fallback is performed here.
    /// This scoped implementation accepts an explicit cached UTC value only.
    pub async fn prepare_recurring(
        &self,
        system: &Value,
        now: DateTime<Utc>,
        task: DirectTask,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<PreparedRecurring, Error> {
        if system.get("time_zone").and_then(Value::as_str) != Some("UTC") {
            return Err(Error::Unsupported);
        }
        let row = recording_schedule::recurring_snapshot(self.pool.clone(), owner)
            .await
            .map_err(Error::Snapshot)?;
        let Some(row) = row else {
            return Ok(PreparedRecurring {
                origin: self.identity.clone(),
                task,
                decision_now: now,
                rule: None,
            });
        };
        let days = row
            .days
            .as_ref()
            .and_then(Value::as_array)
            .filter(|v| v.len() == 1)
            .ok_or(Error::Unsupported)?;
        let weekday = days[0]
            .as_u64()
            .filter(|v| *v <= 6)
            .ok_or(Error::Unsupported)? as u32;
        let day = row.start_date.ok_or(Error::Unsupported)?;
        if row.end_date != Some(day) {
            return Err(Error::Unsupported);
        }
        let name = row.name.ok_or(Error::Unsupported)?;
        let rule = Recurring {
            id: row.id,
            channel_id: row.channel_id,
            name,
            enabled: true,
            day,
            weekday,
            start: row.start,
            end: row.end,
        };
        // Validate the entire admitted date/title/clock domain before claim;
        // actual exact DB dedup is still repeated immediately before INSERT.
        decisions::recurring(now, &rule, false).map_err(|_| Error::Unsupported)?;
        Ok(PreparedRecurring {
            origin: self.identity.clone(),
            task,
            decision_now: now,
            rule: Some(rule),
        })
    }
    pub async fn execute_prepared_recurring(
        self: Arc<Self>,
        prepared: PreparedRecurring,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<RecurringReport, Error> {
        if !Arc::ptr_eq(&self.identity, &prepared.origin) {
            return Err(Error::Unsupported);
        }
        let task = prepared.task;
        match prepared.rule {
            Some(rule) => {
                self.execute_recurring(prepared.decision_now, rule, task, owner)
                    .await
            }
            None => Ok(RecurringReport {
                task: task.id(),
                created: 0,
                children: Vec::new(),
            }),
        }
    }
    async fn execute_intents(
        &self,
        intents: Vec<Intent>,
        task: DirectTask,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<IntentOutcome, Error> {
        // Source11 captures one eligible failing programme/rule. Preserve the
        // existing32-cap normal domain, without inferring multi-error parity.
        let one_eligible = intents.len() == 1;
        let mut created = 0;
        let mut children = Vec::new();
        let mut caught_insert_errors = Vec::new();
        for intent in intents {
            let input = FutureRecording::new(
                intent.channel_id,
                intent.start,
                intent.end,
                intent.properties,
                Utc::now(),
            )
            .ok_or(Error::Unsupported)?;
            let row =
                match recording_schedule::create_future(self.pool.clone(), input, owner.clone())
                    .await
                {
                    Ok(Some(row)) => row,
                    Ok(None) => continue,
                    Err(error) => {
                        if one_eligible {
                            if let Some(observed) = error.known_source_insert_failure() {
                                caught_insert_errors.push(observed);
                                continue;
                            }
                        }
                        return Err(Error::Sql(error));
                    }
                };
            // Source signal publishes once; Series body publishes a SECOND time.
            // Keep both attempts, even if the first has a settled failure.
            for _ in 0..intent.artwork_publications {
                match self
                    .artwork
                    .publish(row.recording_id, task, owner.clone())
                    .await
                {
                    Ok(id) => children.push(id),
                    Err(ArtworkError::SettledUnavailable) => {}
                    Err(ArtworkError::Uncertain) => return Err(Error::ArtworkUncertain),
                }
            }
            created += 1;
        }
        Ok(IntentOutcome {
            created,
            children,
            caught_insert_errors,
        })
    }
    async fn event(
        &self,
        scheduled: Option<u32>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<(), Error> {
        let data = match scheduled {
            Some(count) => json!({"success":true,"type":"recordings_refreshed","scheduled":count}),
            None => json!({"success":true,"type":"recordings_refreshed"}),
        };
        let (tx, rx) = oneshot::channel();
        let receipt = Arc::new(EventReceipt {
            finished: Some(tx),
            _owner: owner,
        });
        let result = self
            .events
            .send_retained(
                "updates",
                json!({"type":"update","data":data}),
                receipt.clone(),
            )
            .await;
        drop(receipt);
        rx.await.map_err(|_| Error::Event)?;
        match result {
            Ok(()) | Err(UpdateEventError::Unavailable) => Ok(()),
            Err(_) => Err(Error::Event),
        }
    }
    /// Must be called after actual CAS/ACK under the SAME global lease retained
    /// by `owner`. Detached caller cannot drop SQL/event/publication ownership.
    pub async fn execute_series(
        self: Arc<Self>,
        prepared: PreparedSeries,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<SeriesReport, Error> {
        if !Arc::ptr_eq(&self.identity, &prepared.origin) {
            return Err(Error::Unsupported);
        }
        let task = prepared.task;
        tokio::spawn(async move {
            let outcome = self
                .execute_intents(prepared.intents, task, owner.clone())
                .await?;
            self.event(Some(outcome.created), owner).await?;
            Ok(SeriesReport {
                task: task.id(),
                root: task.root(),
                parent: task.parent(),
                title: prepared.title,
                tvg: prepared.tvg,
                created: outcome.created,
                children: outcome.children,
                caught_insert_errors: outcome
                    .caught_insert_errors
                    .into_iter()
                    .map(|e| e.message().to_owned())
                    .collect(),
            })
        })
        .await
        .map_err(|_| Error::Supervisor)?
    }
    /// Explicit typed rule is not a DB-admission substitute. A future default
    /// worker adapter must snapshot and reject unsupported enabled rules BEFORE
    /// ACK; it must not call this with only the first of several actual rules.
    async fn execute_recurring(
        self: Arc<Self>,
        now: DateTime<Utc>,
        rule: Recurring,
        task: DirectTask,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<RecurringReport, Error> {
        tokio::spawn(async move {
            let intent = decisions::recurring(now, &rule, false).map_err(|_| Error::Unsupported)?;
            let outcome = self
                .execute_intents(intent.into_iter().collect(), task, owner.clone())
                .await?;
            if outcome.created > 0 {
                self.event(None, owner).await?;
            }
            Ok(RecurringReport {
                task: task.id(),
                created: outcome.created,
                children: outcome.children,
            })
        })
        .await
        .map_err(|_| Error::Supervisor)?
    }
}

#[cfg(test)]
mod caught_insert_tests {
    use super::*;
    #[test]
    fn captured_source_error_is_normal_ordered_result_not_traceback() {
        let source: Value = serde_json::from_str(include_str!(
            "../datasets/source-configured-recordings-insert-errors.json"
        ))
        .unwrap();
        assert_eq!(source["captured_SQLSTATE_verified"], json!(false));
        for case in source["cases"].as_array().unwrap() {
            assert!(case["raw_captured_SQLSTATE"].is_null());
            if case["name"] == "series-SQL-P0001" {
                let value = &case["backend"]["result"];
                let report = SeriesReport {
                    task: Uuid::parse_str(case["backend"]["task_id"].as_str().unwrap()).unwrap(),
                    root: Uuid::parse_str(case["backend"]["task_id"].as_str().unwrap()).unwrap(),
                    parent: None,
                    title: "Owned Exact Series".into(),
                    tvg: "rust.owned.configured".into(),
                    created: 0,
                    children: Vec::new(),
                    caught_insert_errors: vec![value["details"][0]["error"]
                        .as_str()
                        .unwrap()
                        .to_owned()],
                };
                assert_eq!(report.value(), *value);
                assert_eq!(case["event_delta"].as_array().unwrap().len(), 1);
            } else {
                let report = RecurringReport {
                    task: Uuid::parse_str(case["backend"]["task_id"].as_str().unwrap()).unwrap(),
                    created: 0,
                    children: Vec::new(),
                };
                assert_eq!(report.value(), case["backend"]["result"]);
                assert_eq!(case["event_delta"], json!([]));
            }
            assert_eq!(case["recording_delta"], json!(0));
            assert_eq!(case["backend"]["status"], json!("SUCCESS"));
            assert!(case["backend"]["traceback"].is_null());
        }
    }
}
