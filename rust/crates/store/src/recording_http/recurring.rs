//! Original Dispatcharr's scoped recurring DVR rule detail routes.
//! Original Dispatcharr, Django and DRF contributors retain behavior credit.
use super::{
    access, admit, delete_periodic_in_collector, effect, field, iso, publish, python_int, reply,
    reply_bytes, settle, Owner, RecordingEffectError, RecordingHttp, RAW, RESPONSE, ROWS,
};
use crate::{
    admin_writes::{body_object, boolean, character},
    channel_memberships::primary_key,
    channels, AuthService, RecordingPlayback, Store, StoreError, User,
};
use axum::{
    body::{to_bytes, Bytes},
    extract::{ConnectInfo, Path, Request, State},
    http::{header, Method},
    response::Response,
    Json,
};
use chrono::{
    DateTime, Datelike, Days, LocalResult, NaiveDate, NaiveTime, SecondsFormat, TimeZone, Timelike,
    Utc,
};
use chrono_tz::Tz;
use dispatcharr_contracts::UpdateEventSink;
use futures_util::TryStreamExt;
use serde_json::{json, Map, Value};
use sqlx::{postgres::PgRow, Connection, PgConnection, Postgres, QueryBuilder, Row};
use std::{collections::BTreeSet, net::SocketAddr, sync::Arc, time::Duration};

pub(crate) const PATH: &str = "/api/channels/recurring-rules/{id}/";
pub(crate) const COLLECTION: &str = "/api/channels/recurring-rules/";
const SELECT: &str = "SELECT r.id::bigint AS id,r.channel_id::bigint AS channel_id,r.name,
    CASE WHEN octet_length(r.days_of_week::text)<=65536 THEN r.days_of_week END AS days,
    r.start_time,r.end_time,r.start_date,r.end_date,r.created_at,r.updated_at,r.enabled
    FROM dispatcharr_channels_recurringrecordingrule r
    JOIN dispatcharr_channels_channel c ON c.id=r.channel_id WHERE true";

#[derive(Clone)]
struct Rule {
    id: i64,
    channel: i64,
    name: String,
    days: Value,
    start: NaiveTime,
    end: NaiveTime,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
    enabled: bool,
    created: DateTime<Utc>,
    updated: DateTime<Utc>,
}

fn missing() -> StoreError {
    StoreError::StatusBody(
        404,
        json!({"detail":"No RecurringRecordingRule matches the given query."}),
    )
}

fn unsupported_rebuild() -> StoreError {
    StoreError::Unsupported(
        "Enabled recurring rule rebuild and maintenance coordination unavailable",
    )
}

fn time_text(value: NaiveTime) -> String {
    if value.nanosecond() == 0 {
        value.format("%H:%M:%S").to_string()
    } else {
        value.format("%H:%M:%S%.6f").to_string()
    }
}

impl Rule {
    fn decode(row: PgRow) -> Result<Self, StoreError> {
        Ok(Self {
            id: row.try_get("id")?,
            channel: row.try_get("channel_id")?,
            name: row.try_get("name")?,
            days: row
                .try_get::<Option<Value>, _>("days")?
                .ok_or(StoreError::Unsupported(
                    "Recurring rule weekdays exceed the response bound",
                ))?,
            start: row.try_get("start_time")?,
            end: row.try_get("end_time")?,
            start_date: row.try_get("start_date")?,
            end_date: row.try_get("end_date")?,
            enabled: row.try_get("enabled")?,
            created: row.try_get("created_at")?,
            updated: row.try_get("updated_at")?,
        })
    }

    fn project(&self) -> Value {
        json!({
            "id":self.id,"channel":self.channel,"name":self.name,"days_of_week":self.days,
            "start_time":time_text(self.start),"end_time":time_text(self.end),
            "start_date":self.start_date,"end_date":self.end_date,"enabled":self.enabled,
            "created_at":iso(self.created),"updated_at":iso(self.updated),
        })
    }
}

fn rule_id(raw_id: &str) -> Result<i64, StoreError> {
    dispatcharr_ingest_core::source_integer_literal(raw_id)
        .ok_or(StoreError::NotFound)?
        .parse::<i64>()
        .map_err(|_| missing())
}

pub(crate) async fn read_value(
    auth: &AuthService,
    user: &User,
    raw_id: &str,
) -> Result<Value, StoreError> {
    let id = rule_id(raw_id)?;
    let mut connection = auth.store.pool().acquire().await?;
    let value = async {
        let mut tx = connection.begin().await?;
        let result = async {
            sqlx::query("SET LOCAL statement_timeout = '5s'")
                .execute(&mut *tx)
                .await?;
            lookup(&mut tx, user, id, false)
                .await
                .map(|rule| rule.project())
        }
        .await;
        if result.is_ok() {
            tx.commit().await?;
        } else {
            tx.rollback().await?;
        }
        result
    }
    .await;
    settle(connection, value).await
}

async fn collection_bytes(auth: &AuthService, user: &User) -> Result<Vec<u8>, StoreError> {
    let mut connection = auth.store.pool().acquire().await?;
    let result = async {
        let mut tx = connection.begin().await?;
        let result = async {
            sqlx::query("SET TRANSACTION READ ONLY")
                .execute(&mut *tx)
                .await?;
            sqlx::query("SET LOCAL statement_timeout = '5s'")
                .execute(&mut *tx)
                .await?;
            let mut query = QueryBuilder::<Postgres>::new(SELECT);
            channels::scope(&mut query, user);
            query.push(" ORDER BY r.channel_id,r.start_time");
            let mut rows = query.build().fetch(&mut *tx);
            let mut body = vec![b'['];
            while let Some(row) = rows.try_next().await? {
                if body.len() > 1 {
                    body.push(b',');
                }
                serde_json::to_writer(&mut body, &Rule::decode(row)?.project())
                    .map_err(|_| StoreError::UnhandledRestFault)?;
                if body.len() >= RESPONSE {
                    return Err(StoreError::Unsupported("Recording response budget"));
                }
            }
            body.push(b']');
            Ok(body)
        }
        .await;
        if result.is_ok() {
            tx.commit().await?;
        } else {
            tx.rollback().await?;
        }
        result
    }
    .await;
    settle(connection, result).await
}

pub(crate) async fn read_collection(
    auth: AuthService,
    owner: Arc<dyn Send + Sync>,
    peer: SocketAddr,
    request: Request,
) -> Result<Response, StoreError> {
    tokio::spawn(async move {
        crate::epg_program_http::json_negotiation(request.headers(), request.uri())?;
        let auth = auth.with_retained_scope(owner.clone());
        let user = auth
            .account_user(peer, request.headers().clone(), false)
            .await?;
        access(&user, true)?;
        let body = collection_bytes(&auth, &user).await?;
        reply_bytes(200, body, owner)
    })
    .await
    .map_err(|_| StoreError::UnhandledRestFault)?
}

pub(super) async fn collection(
    State(api): State<RecordingHttp>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Result<Response, StoreError> {
    let owner = admit(&api)?;
    read_collection(owner.state.auth.clone(), owner, peer, request).await
}

pub(super) async fn collection_with_playback(
    State((_, playback)): State<(RecordingHttp, RecordingPlayback)>,
    peer: ConnectInfo<SocketAddr>,
    request: Request,
) -> Result<Response, StoreError> {
    crate::recording_playback::recurring_rules(State(playback), peer, request).await
}

async fn lookup(
    connection: &mut PgConnection,
    user: &User,
    id: i64,
    write: bool,
) -> Result<Rule, StoreError> {
    let mut query = QueryBuilder::<Postgres>::new(SELECT);
    query.push(" AND r.id=").push_bind(id);
    channels::scope(&mut query, user);
    if write {
        query.push(" FOR UPDATE OF r");
    }
    let row = query
        .build()
        .fetch_optional(connection)
        .await?
        .ok_or_else(missing)?;
    Rule::decode(row)
}

fn weekdays(value: &Value) -> Result<Value, StoreError> {
    let blank = || field("days_of_week", "Select at least one day of the week");
    let invalid = || field("days_of_week", "Days of week must be integers 0-6");
    let entries = match value {
        Value::Null => return Err(field("days_of_week", "This field may not be null.")),
        Value::Array(values) => values.clone(),
        Value::String(value) => value.chars().map(|c| json!(c.to_string())).collect(),
        Value::Object(values) => values.keys().map(|name| json!(name)).collect(),
        _ if !channels::truthy(Some(value)) => return Err(blank()),
        _ => return Err(StoreError::UnhandledRestFault),
    };
    if entries.is_empty() {
        return Err(blank());
    }
    let mut days = BTreeSet::new();
    for value in &entries {
        let day = python_int(Some(value))
            .map_err(|_| invalid())?
            .ok_or_else(invalid)?;
        if !(0..=6).contains(&day) {
            return Err(field(
                "days_of_week",
                "Days of week must be between 0 (Monday) and 6 (Sunday)",
            ));
        }
        days.insert(day);
    }
    Ok(json!(days))
}

fn clock(name: &str, value: &Value) -> Result<NaiveTime, StoreError> {
    if value.is_null() {
        return Err(field(name, "This field may not be null."));
    }
    let parsed = value.as_str().and_then(|raw| {
        ["%H:%M:%S%.f", "%H:%M"]
            .iter()
            .find_map(|pattern| NaiveTime::parse_from_str(raw, pattern).ok())
            .filter(|time| time.nanosecond() < 1_000_000_000)
            .and_then(|time| time.with_nanosecond(time.nanosecond() / 1000 * 1000))
    });
    parsed.ok_or_else(|| {
        field(
            name,
            "Time has wrong format. Use one of these formats instead: hh:mm[:ss[.uuuuuu]].",
        )
    })
}

fn date(name: &str, value: &Value) -> Result<Option<NaiveDate>, StoreError> {
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_str()
        .and_then(|raw| NaiveDate::parse_from_str(raw, "%Y-%m-%d").ok())
        .filter(|date| (1..=9999).contains(&date.year()))
        .map(Some)
        .ok_or_else(|| {
            field(
                name,
                "Date has wrong format. Use one of these formats instead: YYYY-MM-DD.",
            )
        })
}

fn dates(rule: &Rule, prior: &Rule) -> Result<(), StoreError> {
    let error = |message: &str| StoreError::InvalidInput(json!({"non_field_errors":[message]}));
    if rule.start_date.is_none() && prior.start_date.is_none() {
        return Err(error("Start date is required"));
    }
    if rule
        .start_date
        .zip(rule.end_date)
        .is_some_and(|(start, end)| end < start)
    {
        return Err(error("End date must be on or after start date"));
    }
    if rule.end_date.is_none() && prior.end_date.is_none() {
        return Err(error("End date is required"));
    }
    match rule.start_date.zip(rule.end_date) {
        Some((start, end)) if end.and_time(rule.end) <= start.and_time(rule.start) => {
            Err(error("End datetime must be after start datetime"))
        }
        None if rule.start == rule.end => Err(error("End time must be different from start time")),
        _ => Ok(()),
    }
}

async fn accessible(
    connection: &mut PgConnection,
    user: &User,
    channel: i64,
) -> Result<bool, StoreError> {
    let mut query = QueryBuilder::<Postgres>::new(
        "SELECT c.id::bigint FROM dispatcharr_channels_channel c WHERE c.id=",
    );
    query.push_bind(channel);
    channels::scope(&mut query, user);
    Ok(query
        .build_query_scalar::<i64>()
        .fetch_optional(connection)
        .await?
        .is_some())
}

async fn validate_update(
    connection: &mut PgConnection,
    user: &User,
    old: &Rule,
    input: Map<String, Value>,
    partial: bool,
) -> Result<Rule, StoreError> {
    let mut next = old.clone();
    let mut errors = Map::new();
    for (name, required) in [
        ("days_of_week", false),
        ("start_time", true),
        ("end_time", true),
        ("enabled", false),
        ("name", false),
        ("start_date", false),
        ("end_date", false),
        ("channel", true),
    ] {
        let Some(value) = input.get(name) else {
            if required && !partial {
                errors.insert(name.into(), json!(["This field is required."]));
            }
            continue;
        };
        let result = match name {
            "days_of_week" => weekdays(value).map(|days| next.days = days),
            "start_time" => clock(name, value).map(|time| next.start = time),
            "end_time" => clock(name, value).map(|time| next.end = time),
            "enabled" => boolean(name, value).map(|enabled| next.enabled = enabled),
            "name" => character(name, value, Some(255), true).map(|name| next.name = name),
            "start_date" => date(name, value).map(|date| next.start_date = date),
            "end_date" => date(name, value).map(|date| next.end_date = date),
            _ => match primary_key(value) {
                Ok(Some(channel)) if accessible(&mut *connection, user, channel).await? => {
                    next.channel = channel;
                    Ok(())
                }
                Ok(Some(channel)) => Err(field(
                    name,
                    &format!("Invalid pk \"{channel}\" - object does not exist."),
                )),
                Ok(None) => Err(field(name, "Invalid pk - object does not exist.")),
                Err(error) => Err(error),
            },
        };
        match result {
            Ok(()) => {}
            Err(StoreError::InvalidInput(Value::Object(fields))) => errors.extend(fields),
            Err(error) => return Err(error),
        }
    }
    if !errors.is_empty() {
        return Err(StoreError::InvalidInput(Value::Object(errors)));
    }
    dates(&next, old)?;
    Ok(next)
}

async fn save_rule(connection: &mut PgConnection, next: &mut Rule) -> Result<(), StoreError> {
    next.updated = sqlx::query_scalar(
        "UPDATE dispatcharr_channels_recurringrecordingrule SET
        channel_id=$2,name=$3,days_of_week=$4,start_time=$5,end_time=$6,start_date=$7,end_date=$8,
        enabled=$9,updated_at=clock_timestamp() WHERE id=$1 RETURNING updated_at",
    )
    .bind(next.id)
    .bind(next.channel)
    .bind(&next.name)
    .bind(&next.days)
    .bind(next.start)
    .bind(next.end)
    .bind(next.start_date)
    .bind(next.end_date)
    .bind(next.enabled)
    .fetch_one(connection)
    .await?;
    Ok(())
}

struct Appointment {
    start: DateTime<Tz>,
    end: DateTime<Tz>,
    properties: Value,
}

fn wall_clock(zone: Tz, date: NaiveDate, time: NaiveTime) -> Result<DateTime<Tz>, StoreError> {
    match zone.from_local_datetime(&date.and_time(time)) {
        LocalResult::Single(value) => Ok(value),
        LocalResult::Ambiguous(first, second) => Ok(first.min(second)),
        // ZoneInfo allows imaginary clock values using the pre-transition
        // offset. Keep that exceptional domain outside this bounded rebuild.
        LocalResult::None => Err(unsupported_rebuild()),
    }
}

fn local_iso(value: &DateTime<Tz>) -> String {
    value.to_rfc3339_opts(
        if value.timestamp_subsec_micros() == 0 {
            SecondsFormat::Secs
        } else {
            SecondsFormat::Micros
        },
        false,
    )
}

fn rebuild_plan(
    rule: &Rule,
    zone: Tz,
    channel_name: &str,
    now: DateTime<Utc>,
) -> Result<Option<Vec<Appointment>>, StoreError> {
    let days = match &rule.days {
        Value::Array(values) => values
            .iter()
            .map(|value| python_int(Some(value)).ok().flatten())
            .collect::<Option<Vec<_>>>()
            .unwrap_or_default()
            .into_iter()
            .filter(|day| (0..=6).contains(day))
            .collect::<BTreeSet<_>>(),
        Value::Null => BTreeSet::new(),
        _ => return Err(unsupported_rebuild()),
    };
    if days.is_empty() {
        return Ok(None);
    }

    let today = now.with_timezone(&zone).date_naive();
    let mut day = rule.start_date.unwrap_or(today).max(today);
    // Source uses the entire explicitly bounded interval for a rebuild.
    // Fourteen days is only the fallback when there is no explicit end.
    let end_day = match rule.end_date {
        Some(end) => end,
        None => (now + chrono::Duration::days(14))
            .with_timezone(&zone)
            .date_naive(),
    };
    if end_day < day {
        return Ok(None);
    }

    let title = if rule.name.is_empty() {
        channel_name
    } else {
        &rule.name
    };
    let description = if rule.name.is_empty() {
        format!("Recurring recording for {channel_name}")
    } else {
        rule.name.clone()
    };
    let mut bookings = Vec::new();
    loop {
        if days.contains(&i64::from(day.weekday().num_days_from_monday())) {
            let start = wall_clock(zone, day, rule.start)?;
            if start.with_timezone(&Utc) > now {
                let end_date = if rule.end <= rule.start {
                    day.checked_add_days(Days::new(1))
                        .ok_or_else(unsupported_rebuild)?
                } else {
                    day
                };
                let end = wall_clock(zone, end_date, rule.end)?;
                if end <= start || bookings.len() == ROWS as usize {
                    return Err(unsupported_rebuild());
                }
                let properties = json!({
                    "rule": {"type":"recurring","id":rule.id,"days_of_week":days,"name":rule.name},
                    "status":"scheduled",
                    "description":description,
                    "program":{"title":title,"description":description,"start_time":local_iso(&start),"end_time":local_iso(&end)},
                });
                bookings.push(Appointment {
                    start,
                    end,
                    properties,
                });
            }
        }
        if day == end_day {
            break;
        }
        day = day.succ_opt().ok_or_else(unsupported_rebuild)?;
    }
    Ok(Some(bookings))
}

async fn insert_bookings(
    connection: &mut PgConnection,
    rule: &Rule,
    appointments: Vec<Appointment>,
) -> Result<Vec<i64>, StoreError> {
    let mut recordings = Vec::with_capacity(appointments.len());
    for appointment in appointments {
        if let Some(id) = sqlx::query_scalar(
            "INSERT INTO dispatcharr_channels_recording (channel_id,start_time,end_time,task_id,custom_properties)
             SELECT $1,$2,$3,NULL,$4 WHERE NOT EXISTS(
               SELECT 1 FROM dispatcharr_channels_recording
               WHERE channel_id=$1 AND start_time=$2 AND custom_properties #> '{rule,id}' = to_jsonb($5::bigint)
             ) RETURNING id::bigint",
        )
        .bind(rule.channel)
        .bind(appointment.start.with_timezone(&Utc))
        .bind(appointment.end.with_timezone(&Utc))
        .bind(appointment.properties)
        .bind(rule.id)
        .fetch_optional(&mut *connection)
        .await?
        {
            recordings.push(id);
        }
    }
    Ok(recordings)
}

async fn plan_on(
    connection: &mut PgConnection,
    auth: &AuthService,
    rule: &Rule,
) -> Result<Option<Vec<Appointment>>, StoreError> {
    if !rule.enabled {
        return Ok(None);
    }
    let settings = auth.recurring_system_settings(connection).await?;
    // Django's pinned server timezone is UTC, also used for invalid names.
    let zone = settings["time_zone"]
        .as_str()
        .and_then(|value| value.parse::<Tz>().ok())
        .unwrap_or(chrono_tz::UTC);
    let channel_name: String =
        sqlx::query_scalar("SELECT name FROM dispatcharr_channels_channel WHERE id=$1")
            .bind(rule.channel)
            .fetch_one(connection)
            .await?;
    rebuild_plan(rule, zone, &channel_name, Utc::now())
}

async fn purge_rows(connection: &mut PgConnection, rule_id: i64) -> Result<Vec<PgRow>, StoreError> {
    let rows = sqlx::query(
        "SELECT id::bigint AS id,CASE WHEN octet_length(task_id)<=1020 THEN task_id END AS task,
         task_id IS NULL OR octet_length(task_id)<=1020 AS bounded
         FROM dispatcharr_channels_recording WHERE start_time >= statement_timestamp()
           AND custom_properties #> '{rule,id}' = to_jsonb($1::bigint)
         ORDER BY id LIMIT $2 FOR UPDATE",
    )
    .bind(rule_id)
    .bind(ROWS + 1)
    .fetch_all(&mut *connection)
    .await?;
    if rows.len() > ROWS as usize {
        return Err(StoreError::Unsupported(
            "Recurring rule future recording purge exceeds the row bound",
        ));
    }
    Ok(rows)
}

async fn delete_purge_rows(
    connection: &mut PgConnection,
    rows: &[PgRow],
) -> Result<(), StoreError> {
    for row in rows {
        let id: i64 = row.try_get("id")?;
        let task: Option<String> = row.try_get("task")?;
        if !row.try_get::<bool, _>("bounded")? {
            return Err(StoreError::Unsupported(
                "Recurring rule task name exceeds the row bound",
            ));
        }
        sqlx::query("DELETE FROM dispatcharr_channels_recording WHERE id=$1")
            .bind(id)
            .execute(&mut *connection)
            .await?;
        if let Some(task) = task.filter(|task| !task.is_empty()) {
            if !delete_periodic_in_collector(&mut *connection, &task).await? {
                // Existing periodic post-delete is transactional in PostgreSQL.
                // An absent periodic name would require a separate broker revoke.
                return Err(StoreError::Unsupported(
                    "Recurring rule recording has no matching periodic task",
                ));
            }
        }
    }
    Ok(())
}

async fn purge(connection: &mut PgConnection, rule_id: i64) -> Result<bool, StoreError> {
    let rows = purge_rows(connection, rule_id).await?;
    delete_purge_rows(connection, &rows).await?;
    Ok(!rows.is_empty())
}

async fn configured_purge_rows(
    connection: &mut PgConnection,
    rule_id: i64,
) -> Result<Vec<PgRow>, StoreError> {
    let rows = purge_rows(connection, rule_id).await?;
    for row in &rows {
        let id: i64 = row.try_get("id")?;
        let task: Option<String> = row.try_get("task")?;
        let expected = format!("dvr-recording-{id}");
        if task.as_deref() != Some(expected.as_str()) {
            return Err(StoreError::Unsupported(
                "Configured recurring purge requires a scheduled recording task",
            ));
        }
        // The qualified purge owns an undispatched DVR booking. Having a
        // task name alone doesn't qualify an immediate or legacy revoke.
        let booked: Option<i64> = sqlx::query_scalar(
            "SELECT p.id::bigint FROM django_celery_beat_periodictask p
             JOIN dispatcharr_channels_recording r ON r.id=$1
             JOIN django_celery_beat_clockedschedule c ON c.id=p.clocked_id
             WHERE p.name=$2 AND p.task='apps.channels.tasks.run_recording'
               AND p.enabled AND p.one_off AND p.total_run_count=0
               AND p.interval_id IS NULL AND p.crontab_id IS NULL AND p.solar_id IS NULL
               AND c.clocked_time=r.start_time
               AND r.custom_properties #>> '{status}' = 'scheduled'
               AND NOT EXISTS (SELECT 1 FROM epg_epgsource WHERE refresh_task_id=p.id)
               AND NOT EXISTS (SELECT 1 FROM m3u_m3uaccount WHERE refresh_task_id=p.id)
             FOR UPDATE OF p, c",
        )
        .bind(id)
        .bind(expected)
        .fetch_optional(&mut *connection)
        .await?;
        if booked.is_none() {
            return Err(StoreError::Unsupported(
                "Configured recurring purge requires an owned clocked booking",
            ));
        }
    }
    Ok(rows)
}

pub async fn preflight_recurring_purge(store: &Store, rule_id: i64) -> Result<(), StoreError> {
    let mut tx = store.pool().begin().await?;
    let rows = configured_purge_rows(&mut tx, rule_id).await;
    tx.rollback().await?;
    rows.map(|_| ())
}

pub async fn execute_recurring_purge(
    store: &Store,
    auth: &AuthService,
    events: Arc<dyn UpdateEventSink>,
    rule_id: i64,
    owner: Arc<dyn Send + Sync>,
) -> Result<u64, StoreError> {
    let _writer = auth.task_sql_writer()?;
    let mut tx = store.pool().begin().await?;
    let rows = match configured_purge_rows(&mut tx, rule_id).await {
        Ok(rows) => rows,
        Err(error) => {
            tx.rollback().await?;
            return Err(error);
        }
    };
    if delete_purge_rows(&mut tx, &rows).await.is_err() {
        // Django catches a settled failure of its atomic Collector and returns zero.
        tx.rollback().await?;
        return Ok(0);
    }
    tx.commit().await?;
    let removed = rows.len() as u64;
    if removed > 0 {
        crate::core_task_offsets::retained_event(
            events,
            json!({"type":"update","data":{"success":true,"type":"recordings_refreshed"}}),
            owner,
        )
        .await?;
    }
    Ok(removed)
}

fn input(body: &Bytes, media: &str) -> Result<Map<String, Value>, StoreError> {
    if body.is_empty() {
        return Ok(Map::new());
    }
    if media.split(';').next().map(str::trim) != Some("application/json") {
        return Err(StoreError::StatusBody(
            415,
            json!({"detail":format!("Unsupported media type \"{media}\" in request.")}),
        ));
    }
    let body = serde_json::from_slice::<Value>(body).map_err(|error| {
        StoreError::InvalidInput(json!({"detail":format!(
            "JSON parse error - {}", crate::source_json_error(body, error)
        )}))
    })?;
    body_object(Ok(Json(body)))
}

struct Outcome {
    status: u16,
    payload: Value,
    removed: bool,
    bookings: Vec<i64>,
    rebuild: bool,
}

async fn database(
    owner: &Arc<Owner>,
    auth: &AuthService,
    user: &User,
    id: i64,
    method: Method,
    body: Bytes,
    media: &str,
) -> Result<Outcome, StoreError> {
    let mut connection = owner.state.auth.store.pool().acquire().await?;
    let write = !matches!(method, Method::GET | Method::HEAD);
    let result = async {
        let mut tx = connection.begin().await?;
        let outcome = async {
            sqlx::query("SET LOCAL statement_timeout = '5s'")
                .execute(&mut *tx)
                .await?;
            let old = lookup(&mut tx, user, id, write).await?;
            match method {
                Method::GET | Method::HEAD => Ok(Outcome {
                    status: 200,
                    payload: old.project(),
                    removed: false,
                    bookings: Vec::new(),
                    rebuild: false,
                }),
                Method::PATCH | Method::PUT => {
                    let changes = input(&body, media)?;
                    let mut next =
                        validate_update(&mut tx, user, &old, changes, method == Method::PATCH)
                            .await?;
                    let plan = plan_on(&mut tx, auth, &next).await?;
                    if plan.as_ref().is_some_and(|rows| !rows.is_empty()) {
                        if !owner.state.jobs.supports_recurring_booking() {
                            return Err(unsupported_rebuild());
                        }
                        owner
                            .state
                            .jobs
                            .admit_scheduling(owner.clone())
                            .await
                            .map_err(effect)?;
                    }
                    save_rule(&mut tx, &mut next).await?;
                    let removed = purge(&mut tx, id).await?;
                    let rebuild = plan.is_some();
                    let bookings = match plan {
                        Some(appointments) => insert_bookings(&mut tx, &next, appointments).await?,
                        None => Vec::new(),
                    };
                    Ok(Outcome {
                        status: 200,
                        payload: next.project(),
                        removed,
                        bookings,
                        rebuild,
                    })
                }
                Method::DELETE => {
                    sqlx::query(
                        "DELETE FROM dispatcharr_channels_recurringrecordingrule WHERE id=$1",
                    )
                    .bind(id)
                    .execute(&mut *tx)
                    .await?;
                    Ok(Outcome {
                        status: 204,
                        payload: Value::Null,
                        removed: purge(&mut tx, id).await?,
                        bookings: Vec::new(),
                        rebuild: false,
                    })
                }
                _ => unreachable!("recurring method admitted before SQL"),
            }
        }
        .await;
        match outcome {
            Ok(result) => {
                tx.commit().await?;
                Ok(result)
            }
            Err(error) => {
                tx.rollback().await?;
                Err(error)
            }
        }
    }
    .await;
    settle(connection, result).await
}

async fn execute(
    owner: Arc<Owner>,
    peer: SocketAddr,
    raw_id: String,
    request: Request,
) -> Result<Response, StoreError> {
    crate::epg_program_http::json_negotiation(request.headers(), request.uri())?;
    let auth = owner.state.auth.clone().with_retained_scope(owner.clone());
    let user = auth
        .account_user(peer, request.headers().clone(), false)
        .await?;
    access(&user, true)?;
    let method = request.method().clone();
    if !matches!(
        method,
        Method::GET | Method::HEAD | Method::PATCH | Method::PUT | Method::DELETE
    ) {
        return Err(StoreError::StatusBody(
            405,
            json!({"detail":format!("Method \"{method}\" not allowed.")}),
        ));
    }
    let id = rule_id(&raw_id)?;
    let media = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let body = if matches!(method, Method::PATCH | Method::PUT) {
        tokio::time::timeout(Duration::from_secs(5), to_bytes(request.into_body(), RAW))
            .await
            .map_err(|_| StoreError::StatusBody(408, json!({"detail":"Request body timed out."})))?
            .map_err(|_| StoreError::StatusBody(413, json!({"detail":"Request body too large."})))?
    } else {
        Bytes::new()
    };
    let write = matches!(method, Method::PATCH | Method::PUT | Method::DELETE);
    // Exclude a subsequent detail mutation before this request's committed
    // recordings finish their save-signal effects. SQL ownership is released
    // before the ordinary booking port rereads the committed recording.
    let _recurring = if write {
        Some(owner.state.recurring_writer.lock().await)
    } else {
        None
    };
    let _writer = write.then(|| auth.task_sql_writer()).transpose()?;
    let result = database(&owner, &auth, &user, id, method, body, &media).await?;
    if result.removed {
        publish(
            &owner,
            json!({"success":true,"type":"recordings_refreshed"}),
        )
        .await;
    }
    let refreshed = result.rebuild && (result.removed || !result.bookings.is_empty());
    for id in result.bookings {
        match owner.state.jobs.schedule_recurring(id, owner.clone()).await {
            Ok(()) | Err(RecordingEffectError::SettledFailure) => {}
            Err(error) => return Err(effect(error)),
        }
    }
    if refreshed {
        publish(
            &owner,
            json!({"success":true,"type":"recordings_refreshed"}),
        )
        .await;
    }
    reply(result.status, result.payload, owner.clone())
}

pub(super) async fn handle(
    State(api): State<RecordingHttp>,
    Path(id): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Result<Response, StoreError> {
    let owner = admit(&api)?;
    tokio::spawn(execute(owner, peer, id, request))
        .await
        .map_err(|_| StoreError::UnhandledRestFault)?
}

pub(super) async fn with_playback(
    State((api, _)): State<(RecordingHttp, RecordingPlayback)>,
    id: Path<String>,
    peer: ConnectInfo<SocketAddr>,
    request: Request,
) -> Result<Response, StoreError> {
    handle(State(api), id, peer, request).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn calendar_rule() -> Rule {
        let created = Utc.with_ymd_and_hms(2026, 11, 19, 12, 0, 0).unwrap();
        Rule {
            id: 9,
            channel: 15,
            name: String::new(),
            days: json!([0, 3, 0]),
            start: NaiveTime::from_hms_opt(23, 30, 0).unwrap(),
            end: NaiveTime::from_hms_opt(1, 15, 0).unwrap(),
            start_date: NaiveDate::from_ymd_opt(2026, 12, 14),
            end_date: NaiveDate::from_ymd_opt(2026, 12, 17),
            enabled: true,
            created,
            updated: created,
        }
    }

    #[test]
    fn explicit_end_allows_two_weekdays_beyond_14_days_with_local_overnight_times() {
        let rule = calendar_rule();
        let appointments = rebuild_plan(
            &rule,
            chrono_tz::America::New_York,
            "Owned channel",
            rule.created,
        )
        .unwrap()
        .unwrap();
        assert_eq!(appointments.len(), 2);
        for (appointment, (day, next_day)) in appointments.iter().zip([(14, 15), (17, 18)]) {
            assert_eq!(
                appointment.properties["rule"]["days_of_week"],
                json!([0, 3])
            );
            assert_eq!(appointment.properties["program"]["title"], "Owned channel");
            assert_eq!(
                appointment.properties["description"],
                "Recurring recording for Owned channel"
            );
            assert_eq!(
                appointment.properties["program"]["start_time"],
                format!("2026-12-{day}T23:30:00-05:00")
            );
            assert_eq!(
                appointment.properties["program"]["end_time"],
                format!("2026-12-{next_day}T01:15:00-05:00")
            );
            assert_eq!(appointment.start.with_timezone(&Utc).day(), next_day);
        }
    }

    #[test]
    fn missing_end_uses_14_days_and_calendar_admission_is_bounded() {
        let mut rule = calendar_rule();
        rule.end_date = None;
        assert!(rebuild_plan(&rule, chrono_tz::UTC, "Owned", rule.created)
            .unwrap()
            .is_none());
        rule.end_date = NaiveDate::from_ymd_opt(2030, 1, 1);
        rule.days = json!([0, 1, 2, 3, 4, 5, 6]);
        assert!(matches!(
            rebuild_plan(&rule, chrono_tz::UTC, "Owned", rule.created),
            Err(StoreError::Unsupported(_))
        ));
    }

    #[test]
    fn wall_clock_prefers_zoneinfo_fold_zero_and_refuses_imaginary_time() {
        let zone = chrono_tz::America::New_York;
        let clock = NaiveTime::from_hms_opt(1, 30, 0).unwrap();
        let fall = NaiveDate::from_ymd_opt(2026, 11, 1).unwrap();
        assert_eq!(
            local_iso(&wall_clock(zone, fall, clock).unwrap()),
            "2026-11-01T01:30:00-04:00"
        );
        let spring = NaiveDate::from_ymd_opt(2026, 3, 8).unwrap();
        let gap = NaiveTime::from_hms_opt(2, 30, 0).unwrap();
        assert!(matches!(
            wall_clock(zone, spring, gap),
            Err(StoreError::Unsupported(_))
        ));
    }

    #[test]
    fn weekday_validation_sorts_deduplicates_and_rejects_outside_the_week() {
        assert_eq!(weekdays(&json!([4, "2", 4, 0])).unwrap(), json!([0, 2, 4]));
        assert!(
            matches!(weekdays(&json!([7])),Err(StoreError::InvalidInput(value))
            if value["days_of_week"][0]=="Days of week must be between 0 (Monday) and 6 (Sunday)")
        );
        assert!(
            matches!(weekdays(&json!([])),Err(StoreError::InvalidInput(value))
            if value["days_of_week"][0]=="Select at least one day of the week")
        );
    }

    #[test]
    fn ordinary_drf_dates_and_times_have_canonical_renderings() {
        assert_eq!(
            time_text(clock("start_time", &json!("09:15")).unwrap()),
            "09:15:00"
        );
        assert_eq!(
            time_text(clock("start_time", &json!("09:15:01.25")).unwrap()),
            "09:15:01.250000"
        );
        assert!(clock("start_time", &json!("09:15:60")).is_err());
        assert_eq!(
            date("end_date", &json!("2040-01-10"))
                .unwrap()
                .unwrap()
                .to_string(),
            "2040-01-10"
        );
        assert!(date("end_date", &json!("2040-01-32")).is_err());
    }
}
