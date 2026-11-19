//! Original Dispatcharr's scoped recurring DVR rule detail routes.
//! Original Dispatcharr, Django and DRF contributors retain behavior credit.
use super::{
    access, admit, delete_periodic_in_collector, field, iso, publish, python_int, reply, settle,
    Owner, RecordingHttp, RAW, ROWS,
};
use crate::{
    admin_writes::{body_object, boolean, character},
    channel_memberships::primary_key,
    channels, RecordingPlayback, StoreError, User,
};
use axum::{
    body::{to_bytes, Bytes},
    extract::{ConnectInfo, Path, Request, State},
    http::{header, Method},
    response::Response,
    Json,
};
use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, Timelike, Utc};
use serde_json::{json, Map, Value};
use sqlx::{Connection, PgConnection, Postgres, QueryBuilder, Row};
use std::{collections::BTreeSet, net::SocketAddr, sync::Arc, time::Duration};

pub(super) const PATH: &str = "/api/channels/recurring-rules/{id}/";
const SELECT: &str = "SELECT r.id::bigint AS id,r.channel_id::bigint AS channel_id,r.name,
    CASE WHEN octet_length(r.days_of_week::text)<=65536 THEN r.days_of_week END AS days,
    r.start_time,r.end_time,r.start_date,r.end_date,r.created_at,r.updated_at,r.enabled
    FROM dispatcharr_channels_recurringrecordingrule r
    JOIN dispatcharr_channels_channel c ON c.id=r.channel_id WHERE r.id=";

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
    fn project(&self) -> Value {
        json!({
            "id":self.id,"channel":self.channel,"name":self.name,"days_of_week":self.days,
            "start_time":time_text(self.start),"end_time":time_text(self.end),
            "start_date":self.start_date,"end_date":self.end_date,"enabled":self.enabled,
            "created_at":iso(self.created),"updated_at":iso(self.updated),
        })
    }
}

async fn lookup(
    connection: &mut PgConnection,
    user: &User,
    id: i64,
    write: bool,
) -> Result<Rule, StoreError> {
    let mut query = QueryBuilder::<Postgres>::new(SELECT);
    query.push_bind(id);
    channels::scope(&mut query, user);
    if write {
        query.push(" FOR UPDATE OF r");
    }
    let row = query
        .build()
        .fetch_optional(connection)
        .await?
        .ok_or_else(missing)?;
    Ok(Rule {
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

async fn update(
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
    if old.enabled || next.enabled {
        return Err(unsupported_rebuild());
    }
    next.updated = sqlx::query_scalar(
        "UPDATE dispatcharr_channels_recurringrecordingrule SET
        channel_id=$2,name=$3,days_of_week=$4,start_time=$5,end_time=$6,start_date=$7,end_date=$8,
        enabled=$9,updated_at=clock_timestamp() WHERE id=$1 RETURNING updated_at",
    )
    .bind(old.id)
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
    Ok(next)
}

async fn purge(connection: &mut PgConnection, rule_id: i64) -> Result<bool, StoreError> {
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
    for row in &rows {
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
    Ok(!rows.is_empty())
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

async fn database(
    owner: &Owner,
    user: &User,
    id: i64,
    method: Method,
    body: Bytes,
    media: &str,
) -> Result<(u16, Value, bool), StoreError> {
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
                Method::GET | Method::HEAD => Ok((200, old.project(), false)),
                Method::PATCH | Method::PUT => {
                    let changes = input(&body, media)?;
                    let next =
                        update(&mut tx, user, &old, changes, method == Method::PATCH).await?;
                    Ok((200, next.project(), purge(&mut tx, id).await?))
                }
                Method::DELETE => {
                    if old.enabled {
                        return Err(unsupported_rebuild());
                    }
                    sqlx::query(
                        "DELETE FROM dispatcharr_channels_recurringrecordingrule WHERE id=$1",
                    )
                    .bind(id)
                    .execute(&mut *tx)
                    .await?;
                    Ok((204, Value::Null, purge(&mut tx, id).await?))
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
    let id = dispatcharr_ingest_core::source_integer_literal(&raw_id)
        .ok_or(StoreError::NotFound)?
        .parse::<i64>()
        .map_err(|_| missing())?;
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
    let _writer = matches!(method, Method::PATCH | Method::PUT | Method::DELETE)
        .then(|| auth.task_sql_writer())
        .transpose()?;
    let (status, value, removed) = database(&owner, &user, id, method, body, &media).await?;
    if removed {
        publish(
            &owner,
            json!({"success":true,"type":"recordings_refreshed"}),
        )
        .await;
    }
    reply(status, value, owner)
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
