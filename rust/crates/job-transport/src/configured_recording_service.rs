//! PRIVATE uncompiled configured-Series/UTC-recurring service draft.
//! Normal inert library path; no task consumer or Main constructor is changed.
//! Original Dispatcharr authors retain rule/signal/event behavior credit.
use crate::configured_recordings::{self as decisions, Existing, Intent, Programme, Recurring};
use crate::recording_artwork::DirectTask;
use chrono::{DateTime, Utc};
use dispatcharr_contracts::{BoxFuture, UpdateEventError, UpdateEventSink};
use dispatcharr_store::{
    recording_schedule::{self, FutureRecording, ProgrammeRow, SeriesScope, SeriesTitle},
    series_evaluate::{is_new, original_air_date},
};
use serde_json::{json, Value};
use sqlx::PgPool;
use std::{collections::HashSet, sync::Arc};
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
    /// Optional sealed publication proof for the explicit normal Main graph.
    /// Legacy sinks and existing tests retain their original behavior.
    fn take_publication(&self, _id: Uuid) -> Option<crate::recording_artwork_protocol::Message> {
        None
    }

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
    rules: Vec<PreparedRule>,
}
struct PreparedRule {
    title: String,
    tvg: String,
    intents: Vec<Intent>,
}

const MAX_SERIES_RULES: usize = 8;
// A Series booking can publish two artwork children. The existing lineage
// verifier and result writer accept at most 64 child/recording pairs.
const MAX_SERIES_INTENTS: usize = 32;

struct NormalRule {
    value: Value,
    mode: SeriesRuleMatch,
}
#[derive(Debug, PartialEq, Eq)]
struct SeriesRuleMatch {
    new_only: Option<bool>,
    title_match: SeriesTitle,
    description_match: Option<String>,
}

#[derive(Debug)]
pub struct SeriesReport {
    task: Uuid,
    root: Uuid,
    parent: Option<Uuid>,
    created: u32,
    children: Vec<Uuid>,
    artwork_targets: Vec<(Uuid, i64)>,
    artwork_frames: Vec<crate::recording_artwork_protocol::Message>,
    details: Vec<Value>,
}
impl SeriesReport {
    fn new(task: DirectTask) -> Self {
        Self {
            task: task.id(),
            root: task.root(),
            parent: task.parent(),
            created: 0,
            children: vec![],
            artwork_targets: vec![],
            artwork_frames: vec![],
            details: vec![],
        }
    }

    fn add_rule(&mut self, tvg: String, title: String, outcome: IntentOutcome) {
        self.created += outcome.created;
        self.children.extend(outcome.children);
        self.artwork_targets.extend(outcome.artwork_targets);
        self.artwork_frames.extend(outcome.artwork_frames);
        self.details.extend(
            outcome
                .caught_insert_errors
                .into_iter()
                .map(|error| json!({"tvg_id":tvg,"status":"error","error":error.message()})),
        );
        self.details
            .push(json!({"tvg_id":tvg,"title":title,"status":"ok","created":outcome.created}));
    }

    #[cfg(test)]
    pub(crate) fn provenance_control(
        parent: &crate::series_protocol::SeriesMessage,
        title: String,
        tvg: String,
        frames: Vec<crate::recording_artwork_protocol::Message>,
    ) -> Self {
        let created = frames.len() as u32;
        Self {
            task: parent.task_id,
            root: parent.root,
            parent: parent.parent,
            created,
            children: frames.iter().map(|m| m.task_id).collect(),
            artwork_targets: frames.iter().map(|m| (m.task_id, m.recording)).collect(),
            artwork_frames: frames,
            details: vec![json!({"tvg_id":tvg,"title":title,"status":"ok","created":created})],
        }
    }
    pub(crate) fn artwork_frames(&self) -> &[crate::recording_artwork_protocol::Message] {
        &self.artwork_frames
    }
    pub(crate) fn artwork_targets(&self) -> &[(Uuid, i64)] {
        &self.artwork_targets
    }
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
        json!({"scheduled":self.created,"details":self.details})
    }
}
#[derive(Debug)]
pub struct RecurringReport {
    task: Uuid,
    created: u32,
    children: Vec<Uuid>,
    artwork_targets: Vec<(Uuid, i64)>,
    artwork_frames: Vec<crate::recording_artwork_protocol::Message>,
}
impl RecurringReport {
    pub(crate) fn artwork_targets(&self) -> &[(Uuid, i64)] {
        &self.artwork_targets
    }
    pub(crate) fn artwork_frames(&self) -> &[crate::recording_artwork_protocol::Message] {
        &self.artwork_frames
    }
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
    artwork_targets: Vec<(Uuid, i64)>,
    artwork_frames: Vec<crate::recording_artwork_protocol::Message>,
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
/// The signed editor persists description fields even for title-only exact
/// rules. Normal Main carries new-only selection separately from the
/// existing preclaim shape. Legacy configured callers stay exact/all.
fn normalize_normal_series_rule(rule: &mut Value) -> Result<SeriesRuleMatch, Error> {
    let rule = rule.as_object_mut().ok_or(Error::Unsupported)?;
    let strip = |value: &str| {
        value
            .trim_matches(|c: char| c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c))
            .to_owned()
    };
    let description_match = match rule.remove("description") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) => {
            let value = strip(&value);
            if value.is_empty() {
                None
            } else if value.len() <= 255
                && value.is_ascii()
                && !value.bytes().any(|byte| byte.is_ascii_control())
            {
                Some(value)
            } else {
                return Err(Error::Unsupported);
            }
        }
        _ => return Err(Error::Unsupported),
    };
    match rule.remove("description_mode") {
        None | Some(Value::Null) => {}
        Some(Value::String(value))
            if value.is_empty()
                || value.eq_ignore_ascii_case("contains")
                || (description_match.is_none()
                    && ["search", "regex"]
                        .iter()
                        .any(|mode| value.eq_ignore_ascii_case(mode))) => {}
        _ => return Err(Error::Unsupported),
    }
    let normalize = |key, default: &str| match rule.get(key) {
        None | Some(Value::Null) => Ok(default.to_owned()),
        Some(Value::String(value)) if value.is_empty() => Ok(default.to_owned()),
        Some(Value::String(value)) => Ok(value.to_ascii_lowercase()),
        _ => Err(Error::Unsupported),
    };
    let mode = normalize("mode", "all")?;
    let title_mode = normalize("title_mode", "exact")?;
    let title_match = match title_mode.as_str() {
        "exact" => SeriesTitle::Exact,
        "contains" => SeriesTitle::Contains,
        _ => return Err(Error::Unsupported),
    };
    if !matches!(mode.as_str(), "all" | "new") {
        return Err(Error::Unsupported);
    }
    let untagged = rule.remove("untagged_is_new");
    let new_only = if mode == "new" {
        Some(match untagged {
            None | Some(Value::Null) => false,
            Some(Value::Bool(enabled)) => enabled,
            _ => return Err(Error::Unsupported),
        })
    } else {
        None
    };
    rule.insert("mode".into(), json!(mode));
    rule.insert("title_mode".into(), json!(title_mode));
    for key in ["title", "tvg_id"] {
        if let Some(Value::String(value)) = rule.get_mut(key) {
            *value = strip(value);
        }
    }
    Ok(SeriesRuleMatch {
        new_only,
        title_match,
        description_match,
    })
}

fn normal_rules(group: &Value) -> Result<Vec<NormalRule>, Error> {
    if !group.is_object() {
        return Err(Error::Unsupported);
    }
    let mut stations = HashSet::new();
    let mut selected = Vec::new();
    for mut value in group["series_rules"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|entry| entry.is_object())
        .cloned()
    {
        if selected.len() == MAX_SERIES_RULES {
            return Err(Error::Unsupported);
        }
        let mode = normalize_normal_series_rule(&mut value)?;
        let tvg = small(&value["tvg_id"])?;
        // Across distinct stations, neither Original's exact-airing key nor
        // its stable episode key can collide across prepared rule lists.
        if !stations.insert(tvg.to_owned()) {
            return Err(Error::Unsupported);
        }
        selected.push(NormalRule { value, mode });
    }
    Ok(selected)
}

fn series_offsets(group: &Value) -> Result<(), Error> {
    if group.get("pre_offset_minutes").and_then(Value::as_i64) != Some(0)
        || group.get("post_offset_minutes").and_then(Value::as_i64) != Some(0)
    {
        return Err(Error::Unsupported);
    }
    Ok(())
}

fn optional_episode(value: Option<&Value>) -> Result<Option<u32>, Error> {
    value
        .filter(|value| !value.is_null())
        .map(number)
        .transpose()
}

fn programme(
    row: ProgrammeRow,
    channel: i64,
    source: i64,
    new_only: Option<bool>,
    description_selected: bool,
) -> Result<Option<Programme>, Error> {
    let properties = row.custom_properties.unwrap_or(Value::Null);
    let original = properties.as_object();
    let (season, episode, subtitle, air_date) = if let Some(untagged) = new_only {
        if !(properties.is_null() || original.is_some()) || !is_new(&properties, untagged) {
            return Ok(None);
        }
        if original.is_some_and(|props| {
            props.keys().any(|key| {
                !matches!(
                    key.as_str(),
                    "season" | "episode" | "new" | "previously_shown" | "previously_shown_details"
                )
            })
        }) || (!description_selected && row.description.is_some())
        {
            return Err(Error::Unsupported);
        }
        let season = optional_episode(properties.get("season"))?;
        let episode = optional_episode(properties.get("episode"))?;
        (
            season,
            episode,
            row.sub_title,
            original_air_date(&properties),
        )
    } else {
        let props = original.ok_or(Error::Unsupported)?;
        if props.len() != 2 || (!description_selected && row.description.is_some()) {
            return Err(Error::Unsupported);
        }
        (
            Some(number(&properties["season"])?),
            Some(number(&properties["episode"])?),
            Some(row.sub_title.ok_or(Error::Unsupported)?),
            String::new(),
        )
    };
    Ok(Some(Programme {
        id: row.id,
        source_id: source,
        channel_id: channel,
        tvg_id: row.tvg_id.ok_or(Error::Unsupported)?,
        title: row.title.ok_or(Error::Unsupported)?,
        subtitle,
        original_air_date: air_date,
        description: row.description,
        start: row.start_time,
        end: row.end_time,
        season,
        episode,
    }))
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
    /// Normal Main rule preparation follows Original CoreSettings filtering.
    /// No rules return before offset parsing/SQL/events; legacy prepare is unchanged.
    pub(crate) async fn prepare_series_normal(
        &self,
        group: &Value,
        now: DateTime<Utc>,
        task: DirectTask,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<PreparedSeries, Error> {
        let selected = normal_rules(group)?;
        let mut rules = Vec::with_capacity(selected.len());
        if !selected.is_empty() {
            series_offsets(group)?;
        }
        let mut intents = 0;
        for rule in selected {
            let prepared = self
                .prepare_series_rule(&rule.value, now, owner.clone(), rule.mode)
                .await?;
            intents += prepared.intents.len();
            if intents > MAX_SERIES_INTENTS {
                return Err(Error::Unsupported);
            }
            rules.push(prepared);
        }
        Ok(PreparedSeries {
            origin: self.identity.clone(),
            task,
            rules,
        })
    }
    pub async fn prepare_series(
        &self,
        group: &Value,
        now: DateTime<Utc>,
        task: DirectTask,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<PreparedSeries, Error> {
        series_offsets(group)?;
        let rules = group["series_rules"]
            .as_array()
            .filter(|v| v.len() == 1)
            .ok_or(Error::Unsupported)?;
        let rule = self
            .prepare_series_rule(
                &rules[0],
                now,
                owner,
                SeriesRuleMatch {
                    new_only: None,
                    title_match: SeriesTitle::Exact,
                    description_match: None,
                },
            )
            .await?;
        Ok(PreparedSeries {
            origin: self.identity.clone(),
            task,
            rules: vec![rule],
        })
    }

    async fn prepare_series_rule(
        &self,
        value: &Value,
        now: DateTime<Utc>,
        owner: Arc<dyn Send + Sync>,
        SeriesRuleMatch {
            new_only,
            title_match,
            description_match,
        }: SeriesRuleMatch,
    ) -> Result<PreparedRule, Error> {
        let title_mode = match title_match {
            SeriesTitle::Exact => "exact",
            SeriesTitle::Contains => "contains",
        };
        let expected_mode = if new_only.is_some() { "new" } else { "all" };
        let rule = value.as_object().ok_or(Error::Unsupported)?;
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
            || rule.get("title_mode").and_then(Value::as_str) != Some(title_mode)
            || rule.get("mode").and_then(Value::as_str) != Some(expected_mode)
        {
            return Err(Error::Unsupported);
        }
        let title = small(&value["title"])?.to_owned();
        let tvg = small(&value["tvg_id"])?.to_owned();
        // Source strips these rule values; this first domain refuses noncanonical
        // spacing rather than silently accepting a broader normalization domain.
        if title.trim() != title || tvg.trim() != tvg {
            return Err(Error::Unsupported);
        }
        let channel = value["channel_id"].as_i64().ok_or(Error::Unsupported)?;
        let source = value["epg_source_id"].as_i64().ok_or(Error::Unsupported)?;
        let mut scope = SeriesScope::new(channel, source, tvg.clone(), title.clone(), title_match)
            .ok_or(Error::Unsupported)?;
        let description_selected = description_match.is_some();
        if let Some(description_match) = description_match {
            scope = scope.with_description_match(description_match);
        }
        let snapshot = recording_schedule::series_snapshot(self.pool.clone(), scope, now, owner)
            .await
            .map_err(Error::Snapshot)?;
        let mut programmes = Vec::new();
        for row in snapshot.programmes {
            if let Some(programme) =
                programme(row, channel, source, new_only, description_selected)?
            {
                programmes.push(programme);
            }
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
                season: if new_only.is_some() {
                    optional_episode(program.get("season"))?
                } else {
                    Some(number(program.get("season").ok_or(Error::Unsupported)?)?)
                },
                episode: if new_only.is_some() {
                    optional_episode(program.get("episode"))?
                } else {
                    Some(number(program.get("episode").ok_or(Error::Unsupported)?)?)
                },
                subtitle: new_only
                    .and_then(|_| program.get("sub_title")?.as_str().map(str::to_owned)),
            });
        }
        let intents = decisions::series(
            now,
            (&title, title_match),
            &tvg,
            channel,
            source,
            &programmes,
            &existing,
        )
        .map_err(|_| Error::Unsupported)?;
        if description_selected
            && intents.iter().any(|intent| {
                FutureRecording::new(
                    intent.channel_id,
                    intent.start,
                    intent.end,
                    intent.properties.clone(),
                    now,
                )
                .is_none()
            })
        {
            return Err(Error::Unsupported);
        }
        Ok(PreparedRule {
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
                artwork_targets: Vec::new(),
                artwork_frames: Vec::new(),
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
        let mut artwork_targets = Vec::new();
        let mut artwork_frames = Vec::new();
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
                    Ok(id) => {
                        children.push(id);
                        artwork_targets.push((id, row.recording_id));
                        if let Some(frame) = self.artwork.take_publication(id) {
                            artwork_frames.push(frame);
                        }
                    }
                    Err(ArtworkError::SettledUnavailable) => {}
                    Err(ArtworkError::Uncertain) => return Err(Error::ArtworkUncertain),
                }
            }
            created += 1;
        }
        Ok(IntentOutcome {
            created,
            children,
            artwork_targets,
            artwork_frames,
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
            let mut report = SeriesReport::new(task);
            if prepared.rules.is_empty() {
                return Ok(report);
            }
            for PreparedRule {
                title,
                tvg,
                intents,
            } in prepared.rules
            {
                let outcome = self.execute_intents(intents, task, owner.clone()).await?;
                report.add_rule(tvg, title, outcome);
            }
            self.event(Some(report.created), owner).await?;
            Ok(report)
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
                artwork_targets: outcome.artwork_targets,
                artwork_frames: outcome.artwork_frames,
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
                    created: 0,
                    children: Vec::new(),
                    artwork_targets: Vec::new(),
                    artwork_frames: Vec::new(),
                    details: vec![
                        json!({"tvg_id":"rust.owned.configured","status":"error",
                            "error":value["details"][0]["error"].as_str().unwrap()}),
                        json!({"tvg_id":"rust.owned.configured","title":"Owned Exact Series",
                            "status":"ok","created":0}),
                    ],
                };
                assert_eq!(report.value(), *value);
                assert_eq!(case["event_delta"].as_array().unwrap().len(), 1);
            } else {
                let report = RecurringReport {
                    artwork_targets: Vec::new(),
                    artwork_frames: Vec::new(),
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

#[cfg(test)]
mod normal_defaults_controls {
    use super::*;
    struct NoEffects;
    impl ArtworkSink for NoEffects {
        fn publish<'a>(
            &'a self,
            _recording: i64,
            _task: DirectTask,
            _owner: Arc<dyn Send + Sync>,
        ) -> BoxFuture<'a, Result<Uuid, ArtworkError>> {
            Box::pin(async { panic!("no-rules must not publish artwork") })
        }
    }
    impl UpdateEventSink for NoEffects {
        fn send<'a>(
            &'a self,
            _group: &'a str,
            _message: Value,
        ) -> BoxFuture<'a, Result<(), UpdateEventError>> {
            Box::pin(async { panic!("Source no-rules returns before events") })
        }
    }
    #[test]
    fn normal_series_admits_exact_title_only_rules_saved_by_the_signed_editor() {
        let canonical = json!({
            "tvg_id":"station", "mode":"all", "title":"Evening News",
            "title_mode":"exact", "channel_id":7, "epg_source_id":13
        });
        for fields in [
            json!({"description":"", "description_mode":"contains"}),
            json!({"description":" \t", "description_mode":"REGEX", "mode":"ALL",
                   "title_mode":"EXACT", "title":" Evening News ", "tvg_id":" station "}),
            json!({"description":null, "description_mode":null, "mode":null, "title_mode":""}),
        ] {
            let mut saved = canonical.clone();
            saved
                .as_object_mut()
                .unwrap()
                .extend(fields.as_object().unwrap().clone());
            normalize_normal_series_rule(&mut saved).unwrap();
            assert_eq!(saved, canonical);
        }

        let mut imported = canonical.clone();
        imported.as_object_mut().unwrap().remove("mode");
        imported.as_object_mut().unwrap().remove("title_mode");
        normalize_normal_series_rule(&mut imported).unwrap();
        assert_eq!(imported, canonical);

        for unsupported in [
            json!({"description":"world news","description_mode":"regex"}),
            json!({"description":false}),
            json!({"description_mode":"unknown"}),
            json!({"title_mode":"regex"}),
            json!({"mode":"new","untagged_is_new":"unqualified"}),
        ] {
            let mut saved = canonical.clone();
            saved
                .as_object_mut()
                .unwrap()
                .extend(unsupported.as_object().unwrap().clone());
            assert!(matches!(
                normalize_normal_series_rule(&mut saved),
                Err(Error::Unsupported)
            ));
        }
    }

    fn guide_row(id: i64, hour: i64, properties: Value) -> ProgrammeRow {
        let now: DateTime<Utc> = "2026-11-24T00:00:00+00:00".parse().unwrap();
        ProgrammeRow {
            id,
            start_time: now + chrono::Duration::hours(hour),
            end_time: now + chrono::Duration::hours(hour + 1),
            title: Some("Test Show".into()),
            sub_title: Some("Episode 1".into()),
            description: None,
            custom_properties: Some(properties),
            tvg_id: Some("test.channel.1".into()),
            supported: true,
        }
    }

    #[test]
    fn configured_series_new_mode_follows_source_flags_and_selects_before_dedup() {
        let mut signed = json!({
            "tvg_id":"test.channel.1", "mode":"NEW", "title":"Test Show",
            "title_mode":"exact", "channel_id":2, "epg_source_id":1,
            "description":"", "description_mode":"contains", "untagged_is_new":true
        });
        let policy = normalize_normal_series_rule(&mut signed).unwrap();
        assert_eq!(
            policy,
            SeriesRuleMatch {
                new_only: Some(true),
                title_match: SeriesTitle::Exact,
                description_match: None
            }
        );
        assert_eq!(signed["mode"], "new");

        for (properties, default_new, opted_new) in [
            (json!({"new":true}), true, true),
            (json!({"season":22,"episode":13}), false, true),
            (json!({"previously_shown":true}), false, false),
            (json!({"new":true,"previously_shown":true}), true, true),
            (
                json!({"previously_shown_details":{"start":"2026-06-24"}}),
                false,
                true,
            ),
        ] {
            for (untagged, admitted) in [(false, default_new), (true, opted_new)] {
                assert_eq!(
                    programme(
                        guide_row(1, 2, properties.clone()),
                        2,
                        1,
                        Some(untagged),
                        false
                    )
                    .unwrap()
                    .is_some(),
                    admitted,
                    "{properties} {untagged}"
                );
            }
        }

        let inputs = [
            guide_row(1, 2, json!({"previously_shown":true})),
            guide_row(
                2,
                3,
                json!({"new":true,"previously_shown":true,
                "previously_shown_details":{"start":"2026-06-24"}}),
            ),
            guide_row(3, 4, json!({"new":true})),
        ];
        let now = inputs[0].start_time - chrono::Duration::hours(2);
        let programmes: Vec<_> = inputs
            .into_iter()
            .filter_map(|row| programme(row, 2, 1, Some(false), false).unwrap())
            .collect();
        let intents = decisions::series(
            now,
            ("Test Show", SeriesTitle::Exact),
            "test.channel.1",
            2,
            1,
            &programmes,
            &[],
        )
        .unwrap();
        assert_eq!(intents.len(), 1);
        let snapshot = &intents[0].properties["program"];
        assert_eq!(snapshot["id"], 2);
        assert_eq!(snapshot["original_air_date"], "2026-06-24");
        assert!(snapshot["season"].is_null() && snapshot["episode"].is_null());
        assert_eq!(intents[0].artwork_publications, 2);

        let booking = Existing {
            tvg_id: "test.channel.1".into(),
            title: "Test Show".into(),
            start: programmes[0].start,
            end: programmes[0].end,
            season: None,
            episode: None,
            subtitle: Some(" EPISODE 1 ".into()),
        };
        assert!(decisions::series(
            now,
            ("Test Show", SeriesTitle::Exact),
            "test.channel.1",
            2,
            1,
            &programmes[1..],
            &[booking]
        )
        .unwrap()
        .is_empty());
        assert!(
            matches!(
                programme(guide_row(4, 5, json!({"new":true})), 2, 1, None, false),
                Err(Error::Unsupported)
            ),
            "legacy preclaim shape stays unchanged"
        );
    }

    #[test]
    fn description_selected_rule_preserves_original_programme_summary_in_booking() {
        let raw = "The latest news from around the world.";
        let mut rule = pinned_rule("test.channel.1", "Test Show", 2);
        rule["description"] = json!(" latest AND news ");
        assert_eq!(
            normalize_normal_series_rule(&mut rule)
                .unwrap()
                .description_match
                .as_deref(),
            Some("latest AND news")
        );

        let with_description = || {
            let mut row = guide_row(1, 2, json!({"season":1,"episode":2}));
            row.description = Some(raw.into());
            row
        };
        assert!(matches!(
            programme(with_description(), 2, 1, None, false),
            Err(Error::Unsupported)
        ));
        let selected = programme(with_description(), 2, 1, None, true)
            .unwrap()
            .unwrap();
        let now = selected.start - chrono::Duration::hours(2);
        let [intent] = decisions::series(
            now,
            ("Test Show", SeriesTitle::Exact),
            "test.channel.1",
            2,
            1,
            &[selected],
            &[],
        )
        .unwrap()
        .try_into()
        .unwrap();
        assert_eq!(intent.properties["program"]["description"], raw);
        assert!(FutureRecording::new(
            intent.channel_id,
            intent.start,
            intent.end,
            intent.properties,
            now
        )
        .is_some());
    }

    fn pinned_rule(station: &str, title: &str, channel_id: i64) -> Value {
        json!({"tvg_id":station,"title":title,"mode":"all","title_mode":"exact",
            "channel_id":channel_id,"epg_source_id":1,
            "description":"","description_mode":"contains"})
    }

    #[test]
    fn disjoint_same_source_rules_preserve_order_and_reject_overlaps_before_snapshot() {
        let first = pinned_rule("test.channel.1", "Show A", 1);
        let second = pinned_rule("test.channel.2", "Show B", 2);
        let group = json!({"series_rules":[null,first,second]});
        let selected = normal_rules(&group).unwrap();
        assert_eq!(selected.len(), 2);
        assert_eq!(
            selected[0].value["epg_source_id"],
            selected[1].value["epg_source_id"]
        );
        assert_eq!(selected[0].value["channel_id"], 1);
        assert_eq!(selected[1].value["channel_id"], 2);
        assert_eq!(selected[0].value["title"], "Show A");
        assert_eq!(selected[1].value["title"], "Show B");

        let repeated_station = pinned_rule(" test.channel.1 ", "Unqualified overlap", 9);
        assert!(matches!(
            normal_rules(&json!({"series_rules":[first,repeated_station]})),
            Err(Error::Unsupported)
        ));
        let too_many = (0..=MAX_SERIES_RULES)
            .map(|i| pinned_rule(&format!("station.{i}"), "Show", 1))
            .collect::<Vec<_>>();
        assert!(matches!(
            normal_rules(&json!({"series_rules":too_many})),
            Err(Error::Unsupported)
        ));
    }

    #[test]
    fn aggregate_series_keeps_result_details_and_child_order_under_one_parent() {
        let root = Uuid::new_v4();
        let parent = Uuid::new_v4();
        let message = crate::series_protocol::SeriesMessage {
            task_id: parent,
            root,
            parent: Some(root),
        };
        let mut report = SeriesReport::new(DirectTask::observed_series(&message).unwrap());
        let mut expected_children = Vec::new();
        for (tv, title, created) in [
            ("test.channel.1", "Show A", 1),
            ("test.channel.2", "Show B", 1),
            ("test.channel.3", "Repeat", 0),
        ] {
            let children = (0..(2 * created))
                .map(|_| Uuid::new_v4())
                .collect::<Vec<_>>();
            expected_children.extend_from_slice(&children);
            let artwork_targets = children
                .iter()
                .map(|id| (*id, i64::from(created)))
                .collect();
            report.add_rule(
                tv.into(),
                title.into(),
                IntentOutcome {
                    created,
                    children,
                    artwork_targets,
                    artwork_frames: vec![],
                    caught_insert_errors: vec![],
                },
            );
        }
        assert!(report.matches(&message));
        assert_eq!(report.children(), expected_children);
        assert_eq!(report.artwork_targets().len(), 4);
        assert_eq!(
            report.value(),
            json!({"scheduled":2,"details":[
                {"tvg_id":"test.channel.1","title":"Show A","status":"ok","created":1},
                {"tvg_id":"test.channel.2","title":"Show B","status":"ok","created":1},
                {"tvg_id":"test.channel.3","title":"Repeat","status":"ok","created":0}
            ]})
        );
    }

    #[tokio::test]
    async fn normal_no_rules_uses_source_filter_before_offsets_sql_or_events() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect_lazy("postgres://unreached:unreached@127.0.0.1:1/unreached")
            .unwrap();
        let service = Arc::new(Service::new(
            pool.clone(),
            Arc::new(NoEffects),
            Arc::new(NoEffects),
        ));
        let id = Uuid::new_v4();
        let message = crate::series_protocol::SeriesMessage {
            task_id: id,
            root: id,
            parent: None,
        };
        let task = DirectTask::series(&message).unwrap();
        for rules in [
            None,
            Some(json!([])),
            Some(Value::Null),
            Some(json!("not-a-list")),
            Some(json!([1, null, "ignored"])),
        ] {
            let mut group =
                json!({"pre_offset_minutes":"ignored before no-rules","post_offset_minutes":null});
            if let Some(rules) = rules {
                group["series_rules"] = rules;
            }
            let prepared = service
                .prepare_series_normal(&group, Utc::now(), task, Arc::new(()))
                .await
                .unwrap();
            let report = service
                .clone()
                .execute_series(prepared, Arc::new(()))
                .await
                .unwrap();
            assert_eq!(
                report.value(),
                crate::series_protocol::KnownReturn::NoRules.value()
            );
            assert!(report.matches(&message));
            assert!(report.children().is_empty());
            assert!(report.artwork_targets().is_empty());
            assert!(
                matches!(
                    service
                        .prepare_series(&group, Utc::now(), task, Arc::new(()))
                        .await,
                    Err(Error::Unsupported)
                ),
                "legacy configured preparation unchanged"
            );
        }
        assert!(matches!(service.prepare_series_normal(&json!({"series_rules":[{},false],"pre_offset_minutes":0,"post_offset_minutes":0}),Utc::now(),task,Arc::new(())).await,Err(Error::Unsupported)),"dictionary rules must reach existing configured preflight");
        assert_eq!(pool.size(), 0, "all no-rules branches avoid SQL entirely");
    }
}

#[cfg(test)]
mod selected_recurring_provenance_tests {
    use super::*;
    use crate::{
        default_protocol::{Kind, Message},
        recording_artwork_protocol, ArtworkCompletions,
    };

    #[test]
    fn successful_selected_recurring_artwork_keeps_no_legacy_child_permit() {
        let completions = ArtworkCompletions::new();
        // Selected children use normal broker ownership rather than the legacy
        // in-memory capability. A lifetime of completed parents stays bounded.
        for index in 1..=65 {
            let task = Uuid::new_v4();
            let child = Uuid::new_v4();
            let now = Utc::now();
            let publication = recording_artwork_protocol::Message {
                task_id: child,
                parent: task,
                root: task,
                recording: index,
                eta: now,
            };
            let message = Message {
                task_id: task,
                kind: Kind::RecurringMaintenance,
                expires: None,
            };
            let report = RecurringReport {
                task,
                created: 1,
                children: vec![child],
                artwork_targets: vec![(child, index)],
                artwork_frames: vec![publication.clone()],
            };
            let backend = serde_json::to_vec(&json!({
                "status": "SUCCESS",
                "result": 1,
                "traceback": null,
                "children": [[[child, null], null]],
                "date_done": now.to_rfc3339(),
                "task_id": task,
            }))
            .unwrap();
            completions
                .verify_recurring(message, report, &backend)
                .unwrap();
            assert!(completions.admit(&publication).unwrap().is_none());
        }
    }
}
