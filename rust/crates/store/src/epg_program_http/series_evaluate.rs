//! Synchronous series evaluation from Dispatcharr 1241b3f7.
//! Original Dispatcharr, Django and dependency contributors retain credit.
use super::{is_new, mapped_epg_ids, parse_text, programme_query_error, Expression, Match};
use crate::{
    channels::truthy,
    recording_http::{python_int, python_iso, settle},
    Store, StoreError,
};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use dispatcharr_contracts::BoxFuture;
use serde_json::{json, Value};
use sqlx::{postgres::PgDatabaseError, PgConnection, Postgres, QueryBuilder, Row};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, LazyLock},
};

pub type Owner = Arc<dyn Send + Sync>;

pub enum SignalOutcome {
    Settled,
    SourceCaught,
}

/// The normal jobs graph implements these with real committed-row publishers.
pub trait Effects: Send + Sync {
    fn admit_booking(&self) -> Result<(), StoreError>;
    fn booking(
        &self,
        recording: i64,
        owner: Owner,
    ) -> BoxFuture<'_, Result<SignalOutcome, StoreError>>;
    fn explicit_artwork(
        &self,
        recording: i64,
        owner: Owner,
    ) -> BoxFuture<'_, Result<SignalOutcome, StoreError>>;
    fn refresh(&self, scheduled: usize, owner: Owner) -> BoxFuture<'_, ()>;
}

#[derive(Default, Debug, PartialEq, serde::Serialize)]
pub struct Report {
    pub scheduled: usize,
    pub details: Vec<Value>,
}
impl Report {
    pub fn busy() -> Self {
        Self {
            scheduled: 0,
            details: vec![json!({"status":"skipped","reason":"concurrent evaluation in progress"})],
        }
    }

    fn finish(&mut self, rule: &Rule, created: usize) {
        self.scheduled += created;
        self.details.push(json!({
            "tvg_id":rule.tvg,"title":rule.title,"status":"ok","created":created
        }));
    }

    fn insert_error(&mut self, rule: &Rule, error: String) {
        self.details
            .push(json!({"tvg_id":rule.tvg,"status":"error","error":error}));
    }
}

enum Selection<'a> {
    None,
    NoMatch,
    Rules(Vec<&'a Value>),
}

fn string(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Null => "None".to_owned(),
        Value::Bool(true) => "True".to_owned(),
        Value::Bool(false) => "False".to_owned(),
        Value::Number(_) => crate::json_compat::compact_json_bytes(value)
            .ok()
            .and_then(|bytes| String::from_utf8(bytes).ok())
            .unwrap_or_else(|| value.to_string()),
        _ => value.to_string(),
    }
}

fn strip(text: &str) -> &str {
    text.trim_matches(|c: char| c.is_whitespace() || matches!(c, '\u{1c}'..='\u{1f}'))
}

fn select_rules<'a>(group: &'a Value, filter: Option<&str>) -> Selection<'a> {
    let rules: Vec<_> = group
        .get("series_rules")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|rule| rule.is_object())
        .collect();
    if rules.is_empty() {
        return Selection::None;
    }
    let Some(filter) = filter.filter(|value| !value.is_empty()) else {
        return Selection::Rules(rules);
    };
    let matching: Vec<_> = rules
        .into_iter()
        .filter(|rule| string(rule.get("tvg_id").unwrap_or(&Value::Null)) == filter)
        .collect();
    if matching.is_empty() {
        Selection::NoMatch
    } else {
        Selection::Rules(matching)
    }
}

fn optional_integer(value: Option<&Value>) -> Result<Option<i64>, StoreError> {
    if let Some(Value::String(text)) = value {
        return dispatcharr_ingest_core::source_integer_literal(text)
            .map(|digits| {
                digits
                    .parse()
                    .map_err(|_| StoreError::Unsupported("Series Python integer exceeds SQL range"))
            })
            .transpose();
    }
    python_int(value)
}

struct Rule {
    tvg: String,
    title: String,
    description: String,
    title_mode: String,
    description_mode: String,
    source: Option<i64>,
    pinned: Option<i64>,
    new_only: bool,
    untagged: bool,
}
impl Rule {
    fn parse(value: &Value) -> Result<Self, StoreError> {
        if !value.is_object() {
            return Err(StoreError::UnhandledRestFault);
        }
        let text = |key| -> Result<&str, StoreError> {
            value
                .get(key)
                .filter(|v| truthy(Some(v)))
                .map_or(Ok(""), |v| v.as_str().ok_or(StoreError::UnhandledRestFault))
        };
        let tvg = value
            .get("tvg_id")
            .filter(|v| truthy(Some(v)))
            .map(string)
            .unwrap_or_default();
        Ok(Self {
            tvg: strip(&tvg).to_owned(),
            title: strip(text("title")?).to_owned(),
            description: strip(text("description")?).to_owned(),
            title_mode: text("title_mode")?.to_owned(),
            description_mode: text("description_mode")?.to_owned(),
            source: optional_integer(value.get("epg_source_id"))?.filter(|id| *id > 0),
            pinned: optional_integer(value.get("channel_id"))?,
            new_only: text("mode")?.eq_ignore_ascii_case("new"),
            untagged: truthy(value.get("untagged_is_new")),
        })
    }

    fn invalid_detail(&self) -> Option<Value> {
        (self.title.is_empty() && self.description.is_empty())
            .then(|| json!({"tvg_id":self.tvg,"status":"invalid_rule"}))
    }
}

type Pair = (String, String);
type Airing = (String, String, String);
type Episode = (Pair, String);
type Times = (DateTime<Utc>, Option<DateTime<Utc>>);
type Live = (String, DateTime<Utc>, DateTime<Utc>);

fn props_type_error(props: &Value) -> Option<String> {
    if !truthy(Some(props)) {
        return None;
    }
    let kind = match props {
        Value::Array(_) => "list",
        Value::String(_) => "str",
        Value::Bool(_) => "bool",
        Value::Number(number) if number.is_f64() => "float",
        Value::Number(_) => "int",
        Value::Object(_) | Value::Null => return None,
    };
    Some(format!("'{kind}' object has no attribute 'get'"))
}

fn identity(props: &Value, subtitle: Option<&Value>) -> Option<String> {
    if truthy(Some(props)) && !props.is_object() {
        return None;
    }
    if let (Some(season), Some(episode)) = (
        props.get("season").filter(|v| !v.is_null()),
        props.get("episode").filter(|v| !v.is_null()),
    ) {
        return Some(format!("s{}e{}", string(season), string(episode)));
    }
    props
        .get("onscreen_episode")
        .filter(|v| truthy(Some(v)))
        .or_else(|| subtitle.filter(|v| truthy(Some(v))))
        .map(|v| strip(&string(v)).to_lowercase())
        .filter(|v| !v.is_empty())
}

#[derive(Default)]
struct Existing {
    exact: HashSet<Airing>,
    episodes: HashSet<Episode>,
    unidentified: HashMap<Pair, Vec<Times>>,
}
impl Existing {
    fn load(&mut self, value: Value) {
        let p = &value["program"];
        let Some(tvg) = p.get("tvg_id").filter(|v| truthy(Some(v))).map(string) else {
            return;
        };
        let start = p.get("start_time").filter(|v| truthy(Some(v))).map(string);
        let end = p.get("end_time").filter(|v| truthy(Some(v))).map(string);
        if let (Some(start), Some(end)) = (&start, &end) {
            self.exact.insert((tvg.clone(), start.clone(), end.clone()));
        }
        let title = p
            .get("title")
            .filter(|v| truthy(Some(v)))
            .map(string)
            .unwrap_or_default();
        let pair = (tvg, strip(&title).to_lowercase());
        if let Some(episode) = identity(p, p.get("sub_title")) {
            self.episodes.insert((pair, episode));
        } else if let Some(start) = start.and_then(|v| v.parse().ok()) {
            let end = end.and_then(|v| v.parse().ok());
            self.unidentified
                .entry(pair)
                .or_default()
                .push((start, end));
        }
    }

    fn add(&mut self, programme: &Programme) {
        self.exact.insert(programme.airing());
        if let Some(episode) = programme.identity() {
            self.episodes.insert((programme.pair(), episode));
        } else {
            self.unidentified
                .entry(programme.pair())
                .or_default()
                .push((programme.start, Some(programme.end)));
        }
    }

    fn duplicate(
        &self,
        p: &Programme,
        live: &HashSet<Live>,
        claimed: &mut HashSet<(Pair, Times)>,
    ) -> bool {
        let pair = p.pair();
        if self.exact.contains(&p.airing())
            || p.identity()
                .is_some_and(|id| self.episodes.contains(&(pair.clone(), id)))
        {
            return true;
        }
        let mut best = None;
        for &times in self.unidentified.get(&pair).into_iter().flatten() {
            let (start, end) = times;
            let delta = (start - p.start).abs();
            if delta > Duration::minutes(15)
                || claimed.contains(&(pair.clone(), times))
                || end.is_some_and(|end| live.contains(&(p.tvg(), start, end)))
            {
                continue;
            }
            if best.is_none_or(|(_, closest)| delta < closest) {
                best = Some((times, delta));
            }
        }
        best.is_some_and(|(times, _)| claimed.insert((pair, times)))
    }
}

struct Programme {
    id: i64,
    epg_id: i64,
    source_id: Option<i64>,
    tvg_id: Option<String>,
    title: String,
    sub_title: Option<String>,
    description: Option<String>,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    properties: Option<Value>,
}
impl Programme {
    fn tvg(&self) -> String {
        self.tvg_id.clone().unwrap_or_else(|| "None".to_owned())
    }

    fn props(&self) -> &Value {
        self.properties.as_ref().unwrap_or(&Value::Null)
    }

    fn identity(&self) -> Option<String> {
        let subtitle = self.sub_title.as_ref().map(|s| Value::String(s.clone()));
        identity(self.props(), subtitle.as_ref())
    }

    fn pair(&self) -> Pair {
        (self.tvg(), strip(&self.title).to_lowercase())
    }

    fn airing(&self) -> Airing {
        (self.tvg(), python_iso(self.start), python_iso(self.end))
    }

    fn snapshot(&self) -> Value {
        let props = self.props();
        let mut value = json!({
            "id":self.id,"tvg_id":self.tvg_id,"title":self.title,
            "sub_title":self.sub_title,"description":self.description,
            "original_air_date":original_air_date(props),
            "start_time":python_iso(self.start),"end_time":python_iso(self.end),
            "season":props.get("season"),"episode":props.get("episode"),
            "onscreen_episode":props.get("onscreen_episode"),
        });
        if let Some(source) = self.source_id.filter(|id| *id != 0) {
            value["epg_source_id"] = json!(source);
        }
        json!({"program":value})
    }
}

fn original_air_date(props: &Value) -> String {
    static SHAPE: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(
        r"^(?:[0-9]{4}-[0-9]{2}-[0-9]{2}(?:T.+|\s+[0-9]{2}:[0-9]{2}:[0-9]{2}.*)?|[0-9]{8}|[0-9]{14}(?:\s+[+-][0-9]{4})?)$",
    ).expect("static Original air date pattern")
    });
    let Some(value) = props
        .pointer("/previously_shown_details/start")
        .filter(|v| !v.is_null())
    else {
        return String::new();
    };
    let raw = string(value);
    let text = strip(&raw);
    if SHAPE.is_match(text) {
        let (date, format) = if text.as_bytes()[4] == b'-' {
            (&text[..10], "%Y-%m-%d")
        } else {
            (&text[..8], "%Y%m%d")
        };
        if let Ok(date) = NaiveDate::parse_from_str(date, format) {
            return date.format("%Y-%m-%d").to_string();
        }
    }
    let safe: String = text
        .chars()
        .filter(|c| !r#"\/:*?"<>|"#.contains(*c))
        .collect();
    strip(&safe)
        .replace("..", "")
        .trim_matches([' ', '.'])
        .to_owned()
}

async fn programmes(
    connection: &mut PgConnection,
    rule: &Rule,
    ids: Option<Vec<i64>>,
    now: DateTime<Utc>,
) -> Result<Vec<Programme>, StoreError> {
    let mut query = QueryBuilder::<Postgres>::new(
        "SELECT p.id::bigint AS id,p.epg_id::bigint AS epg_id,e.epg_source_id::bigint AS source_id,
         p.tvg_id,p.title,p.sub_title,p.description,p.start_time AS start,p.end_time AS end,
         p.custom_properties AS properties
         FROM epg_programdata p JOIN epg_epgdata e ON e.id=p.epg_id WHERE p.end_time > ",
    );
    query
        .push_bind(now)
        .push(" AND p.start_time <= ")
        .push_bind(now + Duration::days(7));
    if let Some(ids) = ids {
        query.push(" AND p.epg_id = ANY(").push_bind(ids).push(")");
    }
    let expression = if rule.title.is_empty() {
        None
    } else if rule.title_mode.is_empty() || rule.title_mode.eq_ignore_ascii_case("exact") {
        Some(Expression::Term(Match::Exact, rule.title.clone()))
    } else {
        parse_text(&rule.title, &rule.title_mode)?
    };
    for (field, term) in [
        ("p.title", expression),
        (
            "p.description",
            parse_text(&rule.description, &rule.description_mode)?,
        ),
    ] {
        if let Some(term) = term {
            query.push(" AND ");
            term.write_sql(&mut query, field);
        }
    }
    query
        .push(" ORDER BY p.start_time")
        .build()
        .try_map(|row| {
            Ok(Programme {
                id: row.try_get("id")?,
                epg_id: row.try_get("epg_id")?,
                source_id: row.try_get("source_id")?,
                tvg_id: row.try_get("tvg_id")?,
                title: row.try_get("title")?,
                sub_title: row.try_get("sub_title")?,
                description: row.try_get("description")?,
                start: row.try_get("start")?,
                end: row.try_get("end")?,
                properties: row.try_get("properties")?,
            })
        })
        .fetch_all(connection)
        .await
        .map_err(programme_query_error)
}

enum Channels {
    Pinned(i64),
    Effective(HashMap<i64, i64>),
}
impl Channels {
    fn get(&self, p: &Programme) -> Option<i64> {
        match self {
            Self::Pinned(id) => Some(*id),
            Self::Effective(map) => map.get(&p.epg_id).copied(),
        }
    }
}

enum Plan {
    Skip(Value),
    Run(Vec<Programme>, Channels),
}

async fn plan(
    connection: &mut PgConnection,
    rule: &Rule,
    now: DateTime<Utc>,
) -> Result<Plan, StoreError> {
    if let Some(detail) = rule.invalid_detail() {
        return Ok(Plan::Skip(detail));
    }
    let ids = if rule.tvg.is_empty() {
        None
    } else {
        let (found, mapped) = mapped_epg_ids(connection, &rule.tvg, rule.source).await?;
        if mapped.is_empty() {
            let status = if found {
                "no_channel_for_epg"
            } else {
                "no_epg_match"
            };
            return Ok(Plan::Skip(
                json!({"tvg_id":rule.tvg,"epg_source_id":rule.source,"status":status}),
            ));
        }
        Some(mapped)
    };
    let programmes = programmes(connection, rule, ids, now).await?;
    let channels = if let Some(id) = rule.pinned {
        let found: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM dispatcharr_channels_channel WHERE id=$1)",
        )
        .bind(id)
        .fetch_one(connection)
        .await?;
        if !found {
            return Ok(Plan::Skip(
                json!({"tvg_id":rule.tvg,"status":"pinned_channel_missing","channel_id":id}),
            ));
        }
        Channels::Pinned(id)
    } else {
        let ids: Vec<_> = programmes
            .iter()
            .map(|p| p.epg_id)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let rows: Vec<(i64, i64)> = sqlx::query_as(
            "SELECT DISTINCT ON (COALESCE(o.epg_data_id,c.epg_data_id))
                    COALESCE(o.epg_data_id,c.epg_data_id)::bigint,c.id::bigint
             FROM dispatcharr_channels_channel c LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id
             WHERE COALESCE(o.epg_data_id,c.epg_data_id)=ANY($1)
             ORDER BY COALESCE(o.epg_data_id,c.epg_data_id),COALESCE(o.channel_number,c.channel_number)",
        ).bind(ids).fetch_all(connection).await?;
        if rows.is_empty() {
            return Ok(Plan::Skip(
                json!({"tvg_id":rule.tvg,"status":"no_channel_for_epg"}),
            ));
        }
        Channels::Effective(rows.into_iter().collect())
    };
    Ok(Plan::Run(programmes, channels))
}

fn live_airings(programmes: &[Programme], existing: &Existing) -> HashSet<Live> {
    let unidentified: HashSet<_> = programmes
        .iter()
        .filter(|p| p.identity().is_none())
        .map(Programme::pair)
        .collect();
    programmes
        .iter()
        .filter(|p| {
            unidentified.contains(&p.pair()) || existing.unidentified.contains_key(&p.pair())
        })
        .map(|p| (p.tvg(), p.start, p.end))
        .collect()
}

fn earliest(programmes: Vec<Programme>, rule: &Rule) -> Vec<Programme> {
    let mut seen = HashSet::new();
    programmes
        .into_iter()
        .filter(|p| {
            if rule.new_only
                && (props_type_error(p.props()).is_some() || !is_new(p.props(), rule.untagged))
            {
                return false;
            }
            // Input is ordered by start time. Keep Original's episode key spelling.
            let key = p.identity().map_or_else(
                || format!("id:{}", p.id),
                |episode| {
                    format!(
                        "{}|{}|{episode}",
                        p.tvg_id.as_deref().unwrap_or(""),
                        p.pair().1
                    )
                },
            );
            seen.insert(key)
        })
        .collect()
}

enum Insert {
    Skipped,
    Caught(String),
    Booked(i64),
}

fn caught_db_error(error: &sqlx::Error) -> Option<String> {
    let pg = error
        .as_database_error()?
        .try_downcast_ref::<PgDatabaseError>()?;
    let mut message = pg.message().to_owned();
    for (label, value) in [
        ("DETAIL", pg.detail()),
        ("HINT", pg.hint()),
        ("CONTEXT", pg.r#where()),
    ] {
        if let Some(value) = value {
            message.push_str(&format!("\n{label}:  {value}"));
        }
    }
    Some(message)
}

fn padding(value: Option<&Value>) -> Duration {
    optional_integer(value)
        .ok()
        .flatten()
        .filter(|v| *v > 0)
        .and_then(Duration::try_minutes)
        .unwrap_or_default()
}

async fn insert(
    store: &Store,
    effects: &dyn Effects,
    channel: i64,
    p: &Programme,
    pre: Duration,
    post: Duration,
) -> Result<Insert, StoreError> {
    let mut connection = store.pool().acquire().await?;
    let result = async {
        let (_, start, end) = p.airing();
        let exact = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM dispatcharr_channels_recording
             WHERE custom_properties#>'{program,tvg_id}'=$1
               AND custom_properties#>'{program,start_time}'=$2
               AND custom_properties#>'{program,end_time}'=$3)",
        ).bind(json!(p.tvg_id)).bind(json!(start)).bind(json!(end))
            .fetch_one(&mut *connection).await;
        match exact {
            Ok(true) => return Ok(Insert::Skipped),
            Err(error) if error.as_database_error().is_some() => return Ok(Insert::Skipped),
            Err(error) => return Err(error.into()),
            Ok(false) => {}
        }
        if let Some(message) = props_type_error(p.props()) {
            return Ok(Insert::Caught(message));
        }
        effects.admit_booking()?;
        let inserted = sqlx::query_scalar::<_, i64>(
            "INSERT INTO dispatcharr_channels_recording (channel_id,start_time,end_time,task_id,custom_properties)
             VALUES ($1,$2,$3,NULL,$4) RETURNING id::bigint",
        ).bind(channel)
            .bind(p.start.checked_sub_signed(pre).unwrap_or(p.start))
            .bind(p.end.checked_add_signed(post).unwrap_or(p.end))
            .bind(p.snapshot())
            .fetch_one(&mut *connection).await;
        match inserted {
            Ok(id) => Ok(Insert::Booked(id)),
            Err(error) => match caught_db_error(&error) {
                Some(message) => Ok(Insert::Caught(message)),
                None => Err(error.into()),
            },
        }
    }.await;
    settle(connection, result).await
}

/// Invoked only inside the normal Series lock owner. No queue or result state
/// is needed. The caller retains a supervised owner until the lock settles.
pub async fn evaluate_locked(
    store: &Store,
    effects: &dyn Effects,
    group: &Value,
    filter: Option<&str>,
    now: DateTime<Utc>,
    owner: Owner,
) -> Result<Report, StoreError> {
    let mut report = Report::default();
    let rules = match select_rules(group, filter) {
        Selection::None => return Ok(report),
        Selection::NoMatch => {
            report
                .details
                .push(json!({"tvg_id":filter,"status":"no_rule"}));
            return Ok(report);
        }
        Selection::Rules(rules) => rules,
    };
    let pre = padding(group.get("pre_offset_minutes"));
    let post = padding(group.get("post_offset_minutes"));
    let mut connection = store.pool().acquire().await?;
    let rows = sqlx::query_scalar::<_, Option<Value>>(
        "SELECT custom_properties FROM dispatcharr_channels_recording WHERE end_time >= $1",
    )
    .bind(now)
    .fetch_all(&mut *connection)
    .await
    .map_err(StoreError::from);
    let mut existing = Existing::default();
    for props in settle(connection, rows).await?.into_iter().flatten() {
        existing.load(props);
    }
    for value in rules {
        let rule = Rule::parse(value)?;
        let mut connection = store.pool().acquire().await?;
        let planned = plan(&mut connection, &rule, now).await;
        let (programmes, channels) = match settle(connection, planned).await? {
            Plan::Skip(detail) => {
                report.details.push(detail);
                continue;
            }
            Plan::Run(programmes, channels) => (programmes, channels),
        };
        let live = live_airings(&programmes, &existing);
        let mut claimed = HashSet::new();
        let mut created = 0;
        for programme in earliest(programmes, &rule) {
            let Some(channel) = channels.get(&programme) else {
                continue;
            };
            if existing.duplicate(&programme, &live, &mut claimed) {
                continue;
            }
            match insert(store, effects, channel, &programme, pre, post).await? {
                Insert::Skipped => {}
                Insert::Caught(message) => report.insert_error(&rule, message),
                Insert::Booked(id) => {
                    effects.booking(id, owner.clone()).await?;
                    existing.add(&programme);
                    created += 1;
                    effects.explicit_artwork(id, owner.clone()).await?;
                }
            }
        }
        report.finish(&rule, created);
    }
    effects.refresh(report.scheduled, owner).await;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn programme(id: i64, minute: i64, properties: Value) -> Programme {
        let start = DateTime::parse_from_rfc3339("2026-11-24T00:00:00+00:00")
            .unwrap()
            .with_timezone(&Utc)
            + Duration::minutes(minute);
        Programme {
            id,
            epg_id: 8,
            source_id: Some(7),
            tvg_id: Some("station".into()),
            title: " A Show ".into(),
            sub_title: None,
            description: Some("Preface".into()),
            start,
            end: start + Duration::minutes(20),
            properties: Some(properties),
        }
    }

    #[test]
    fn distinguishes_empty_rules_and_exact_no_rule_selection() {
        let empty = json!({"series_rules":[null, false, "not a rule"]});
        assert!(matches!(
            select_rules(&empty, Some("station")),
            Selection::None
        ));
        let group = json!({"series_rules":[
            false, {"tvg_id":"station","title":"first"},
            {"tvg_id":18,"title":"other"},
            {"tvg_id":"station","title":"last"}
        ]});
        assert!(matches!(
            select_rules(&group, Some(" station ")),
            Selection::NoMatch
        ));
        let Selection::Rules(selected) = select_rules(&group, Some("station")) else {
            panic!("selection missing")
        };
        assert_eq!(
            selected.iter().map(|r| &r["title"]).collect::<Vec<_>>(),
            vec![&json!("first"), &json!("last")]
        );
        assert!(matches!(select_rules(&group, Some("18")), Selection::Rules(r) if r.len() == 1));
        assert_eq!(
            serde_json::to_value(Report::default()).unwrap(),
            json!({"scheduled":0,"details":[]})
        );
        assert_eq!(
            serde_json::to_value(Report::busy()).unwrap(),
            json!({
                "scheduled":0,"details":[{"status":"skipped","reason":"concurrent evaluation in progress"}]
            })
        );
    }

    #[test]
    fn appends_per_episode_error_before_rule_summary_in_settings_order() {
        let mut report = Report::default();
        let invalid = Rule::parse(&json!({"tvg_id":" x "})).unwrap();
        let first = Rule::parse(&json!({"tvg_id":"station","title":" First "})).unwrap();
        let last = Rule::parse(&json!({"description":"last"})).unwrap();
        report.details.push(invalid.invalid_detail().unwrap());
        report.insert_error(&first,
            "owned configured recording failure\nCONTEXT:  PL/pgSQL function rust_owned_recording_fail() line 1 at RAISE".into());
        report.finish(&first, 1);
        report.finish(&last, 2);
        assert_eq!(
            serde_json::to_value(report).unwrap(),
            json!({
                "scheduled":3,"details":[
                    {"tvg_id":"x","status":"invalid_rule"},
                    {"tvg_id":"station","status":"error","error":
                        "owned configured recording failure\nCONTEXT:  PL/pgSQL function rust_owned_recording_fail() line 1 at RAISE"},
                    {"tvg_id":"station","title":"First","status":"ok","created":1},
                    {"tvg_id":"","title":"","status":"ok","created":2}
                ]
            })
        );
    }

    #[test]
    fn filters_newness_before_choosing_earliest_and_keeps_untagged_option() {
        let new =
            Rule::parse(&json!({"title":"A Show","mode":"new","untagged_is_new":true})).unwrap();
        let programmes = vec![
            programme(
                1,
                0,
                json!({"season":1,"episode":7,"previously_shown":true}),
            ),
            programme(
                2,
                30,
                json!({"season":1,"episode":7,"new":true,"previously_shown":true}),
            ),
            programme(3, 60, json!({"season":1,"episode":7,"new":true})),
            programme(4, 90, json!({})),
        ];
        assert_eq!(
            earliest(programmes, &new)
                .into_iter()
                .map(|p| p.id)
                .collect::<Vec<_>>(),
            vec![2, 4]
        );
    }

    #[test]
    fn drifted_legacy_slot_claims_one_candidate_and_never_claims_a_live_slot() {
        let old = programme(1, 0, json!({}));
        let replacement = programme(2, 10, json!({"season":1,"episode":7}));
        let nearby = programme(3, 15, json!({"season":1,"episode":8}));
        let mut existing = Existing::default();
        existing.load(old.snapshot());
        let mut claimed = HashSet::new();
        assert!(existing.duplicate(&replacement, &HashSet::new(), &mut claimed));
        assert!(!existing.duplicate(&nearby, &HashSet::new(), &mut claimed));
        assert!(!existing.duplicate(
            &replacement,
            &HashSet::from([(old.tvg(), old.start, old.end)]),
            &mut HashSet::new()
        ));
        existing.add(&replacement);
        assert!(existing.duplicate(
            &programme(4, 60, json!({"season":1,"episode":7})),
            &HashSet::new(),
            &mut HashSet::new()
        ));
    }

    #[test]
    fn snapshot_uses_original_times_and_actual_guide_source() {
        let p = programme(
            2,
            30,
            json!({
                "season":1,"episode":"07","onscreen_episode":"S01E07",
                "previously_shown_details":{"start":"20260624000000 +0000"}
            }),
        );
        assert_eq!(
            p.snapshot(),
            json!({"program":{
                "id":2,"tvg_id":"station","title":" A Show ","sub_title":null,
                "description":"Preface","original_air_date":"2026-06-24",
                "start_time":"2026-11-24T00:30:00+00:00","end_time":"2026-11-24T00:50:00+00:00",
                "season":1,"episode":"07","onscreen_episode":"S01E07","epg_source_id":7
            }})
        );
        assert_eq!(padding(Some(&json!("7"))), Duration::minutes(7));
        assert_eq!(padding(Some(&json!(-7))), Duration::zero());
    }
}
