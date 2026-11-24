//! Source-backed EPG detail lifecycle and bounded dummy JSON creation.
//! Original Dispatcharr/Django/Celery/Channels authors retain all behavior credit.
//! Remote XMLTV source creation uses the bounded selected-worker producer.
use crate::{
    admin_writes::{body_object, boolean, character, integer, parse_datetime},
    AuthService, StoreError, TaskSqlSession,
};
use axum::{
    extract::{rejection::JsonRejection, ConnectInfo, Path, State},
    http::{header::ALLOW, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Map, Value};
use sqlx::{Connection, PgConnection, Row};
use std::{net::SocketAddr, sync::Arc};
#[path = "epg_source_writes/xmltv_create.rs"]
mod xmltv_create;
pub(super) use xmltv_create::create_xmltv;
type Body = Result<Json<Value>, JsonRejection>;
type Owner = Arc<dyn Send + Sync>;
fn unsupported() -> StoreError {
    StoreError::Unsupported("EPG detail lifecycle outside Source-qualified bounded positive domain")
}
fn missing() -> StoreError {
    StoreError::StatusBody(
        404,
        json!({"detail":"No EPGSource matches the given query."}),
    )
}
fn key(value: String) -> Result<i64, StoreError> {
    value.parse().map_err(|_| StoreError::NotFound)
}
fn field(name: &str, message: &str) -> StoreError {
    StoreError::InvalidInput(json!({name:[message]}))
}
fn nullable_character(
    name: &str,
    value: &Value,
    maximum: Option<usize>,
) -> Result<Value, StoreError> {
    if value.is_null() {
        Ok(Value::Null)
    } else {
        character(name, value, maximum, true).map(Value::String)
    }
}
fn positive_integer(name: &str, value: &Value) -> Result<Value, StoreError> {
    let minimum = || field(name, "Ensure this value is greater than or equal to 0.");
    let number = integer(name, value).map_err(|error| match error {
        StoreError::InvalidInput(ref v)
            if v[name] == json!(["Ensure this value is greater than or equal to -2147483648."]) =>
        {
            minimum()
        }
        other => other,
    })?;
    if number < 0 {
        Err(minimum())
    } else {
        Ok(json!(number))
    }
}
fn captured_dummy_name_domain(saved: &Value) -> bool {
    saved["name"].as_str().is_some_and(str::is_ascii)
}
fn truth(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(v) => *v,
        Value::String(v) => !v.is_empty(),
        Value::Array(v) => !v.is_empty(),
        Value::Object(v) => !v.is_empty(),
        Value::Number(v) => v.as_f64().is_some_and(|n| n != 0.0),
    }
}
struct Validated {
    saved: Value,
    cron: String,
    url_changed: bool,
}
fn validate(
    mut input: Map<String, Value>,
    old: &Value,
    linked_cron: &str,
    partial: bool,
) -> Result<Validated, StoreError> {
    // Source partial_update mutates request.status using the RAW value first;
    // pre_save then compares the parsed is_active value against the old row.
    if partial {
        if let Some(raw) = input.get("is_active") {
            if !crate::json_compat::python_equal(&old["is_active"], raw) {
                input.insert(
                    "status".into(),
                    json!(if truth(raw) { "idle" } else { "disabled" }),
                );
            }
        }
    }
    let mut saved = old.clone();
    let mut errors = Map::new();
    for name in [
        "name",
        "source_type",
        "url",
        "username",
        "password",
        "is_active",
        "file_path",
        "refresh_interval",
        "priority",
        "status",
        "last_message",
        "updated_at",
        "custom_properties",
    ] {
        let Some(value) = input.get(name) else {
            if !partial && matches!(name, "name" | "source_type") {
                errors.insert(name.into(), json!(["This field is required."]));
            }
            continue;
        };
        let result = match name {
            "name" => character(name, value, Some(255), false).map(Value::String),
            "source_type" | "status" => {
                let choices: &[&str] = if name == "source_type" {
                    &["xmltv", "dummy", "schedules_direct"]
                } else {
                    &[
                        "idle", "fetching", "parsing", "error", "success", "disabled",
                    ]
                };
                match value.as_str() {
                    Some(s) if choices.contains(&s) => Ok(value.clone()),
                    None if value.is_null() => Err(field(name, "This field may not be null.")),
                    _ => Err(field(
                        name,
                        &format!(
                            "\"{}\" is not a valid choice.",
                            value
                                .as_str()
                                .map_or_else(|| value.to_string(), str::to_owned)
                        ),
                    )),
                }
            }
            "is_active" => boolean(name, value).map(Value::Bool),
            "refresh_interval" => integer(name, value).map(|v| json!(v)),
            "priority" => positive_integer(name, value),
            "username" | "password" => nullable_character(name, value, Some(255)),
            "file_path" => nullable_character(name, value, Some(1024)),
            "last_message" => nullable_character(name, value, None),
            "url" if value == &old["url"] => Ok(value.clone()),
            "url" if partial && old["source_type"] == "xmltv" && old["is_active"] == true => {
                xmltv_create::remote_url(value).map(Value::String)
            }
            "url" => Err(unsupported()),
            "updated_at" => parse_datetime(name, value, true).map(|v| json!(v)),
            _ => Ok(value.clone()),
        };
        match result {
            Ok(value) => {
                // Source skips empty/null password updates.
                if name != "password" || truth(&value) {
                    saved[name] = value;
                }
            }
            Err(StoreError::InvalidInput(Value::Object(e))) => errors.extend(e),
            Err(error) => return Err(error),
        }
    }
    let cron = match input.get("cron_expression") {
        Some(value) => character("cron_expression", value, None, true)?,
        None if partial => linked_cron.to_owned(),
        None => String::new(), // Source serializer's explicit default on full PUT.
    };
    if !errors.is_empty() {
        return Err(StoreError::InvalidInput(Value::Object(errors)));
    }
    if saved["source_type"] != old["source_type"]
        || !matches!(
            saved["source_type"].as_str(),
            Some("dummy" | "xmltv" | "schedules_direct")
        )
    {
        return Err(unsupported());
    }
    if saved["username"] != old["username"] || saved["password"] != old["password"] {
        return Err(unsupported());
    }
    if !cron.trim().is_empty() && cron.split_whitespace().count() != 5 {
        return Err(unsupported());
    }
    if saved["source_type"] == "dummy" {
        saved["status"] = json!("idle");
        saved["last_message"] = Value::Null;
    } else if saved["is_active"] != old["is_active"] {
        saved["status"] = json!(if saved["is_active"] == true {
            "idle"
        } else {
            "disabled"
        });
    }
    let url_changed = old["url"] != saved["url"];
    if url_changed {
        let previous = xmltv_create::remote_url(&old["url"])?;
        let next = saved["url"].as_str().ok_or_else(unsupported)?;
        let previous = url::Url::parse(&previous).map_err(|_| unsupported())?;
        let next = url::Url::parse(next).map_err(|_| unsupported())?;
        // The XMLTV editor can rename a source while changing its URL.
        // Preserve the selected origin, other saved fields and recurrence.
        let mut unchanged = saved.clone();
        unchanged["name"] = old["name"].clone();
        unchanged["url"] = old["url"].clone();
        if previous.origin() != next.origin()
            || unchanged != *old
            || cron != linked_cron
            || old["refresh_task_id"].as_i64().is_none()
        {
            return Err(unsupported());
        }
    }
    Ok(Validated {
        saved,
        cron,
        url_changed,
    })
}
async fn no_custom_triggers(c: &mut PgConnection) -> Result<(), StoreError> {
    let custom:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE NOT tgisinternal AND tgenabled<>'D' AND tgrelid IN('epg_epgsource'::regclass,'epg_epgdata'::regclass,'epg_programdata'::regclass,'epg_epgsourceindex'::regclass,'epg_sdprogrammd5'::regclass,'epg_sdschedulemd5'::regclass,'django_celery_beat_periodictask'::regclass,'django_celery_beat_intervalschedule'::regclass,'django_celery_beat_crontabschedule'::regclass,'django_celery_beat_periodictasks'::regclass))").fetch_one(c).await?;
    if custom {
        Err(unsupported())
    } else {
        Ok(())
    }
}
async fn project(
    session: &TaskSqlSession,
    id: i64,
    cron: Option<String>,
) -> Result<Json<Value>, StoreError> {
    let row = session
        .run(move |c| {
            Box::pin(async move {
                sqlx::query(crate::provider_reads::epg_read_query(true))
                    .bind(Some(id))
                    .bind(None::<i64>)
                    .fetch_one(c)
                    .await
            })
        })
        .await??;
    let mut value = crate::provider_reads::epg_read_projection(row, true)?;
    if let Some(cron) = cron {
        value["cron_expression"] = json!(cron);
    }
    Ok(Json(value))
}
async fn schedule_timezone(
    session: &TaskSqlSession,
    auth: AuthService,
    cron: &str,
) -> Result<String, StoreError> {
    if cron.trim().is_empty() {
        return Ok("UTC".into());
    }
    let settings = session.system_settings(auth).await?;
    match &settings["time_zone"] {
        Value::Null => Ok("UTC".into()),
        Value::String(value) if value.is_empty() => Ok("UTC".into()),
        Value::String(value) if value.len() <= 255 => Ok(value.clone()),
        _ => Err(unsupported()),
    }
}

async fn schedule_refresh_on_connection(
    connection: &mut PgConnection,
    id: i64,
    active: bool,
    interval: i32,
    cron: &str,
    zone: &str,
) -> Result<(), StoreError> {
    let name = format!("epg_source-refresh-{id}");
    let kwargs = format!("{{\"source_id\": {id}}}");
    let periodic = crate::provider_schedule::upsert(
        connection,
        crate::provider_schedule::Spec {
            name: &name,
            task: "apps.epg.tasks.refresh_epg_data",
            kwargs: &kwargs,
            interval_hours: interval,
            cron,
            active,
            timezone: zone,
        },
    )
    .await?;
    let periodic = i32::try_from(periodic).map_err(|_| unsupported())?;
    sqlx::query("UPDATE epg_epgsource SET refresh_task_id=$2 WHERE id=$1 AND refresh_task_id IS DISTINCT FROM $2")
        .bind(id)
        .bind(periodic)
        .execute(connection)
        .await?;
    Ok(())
}
async fn schedule_refresh(
    session: &TaskSqlSession,
    id: i64,
    active: bool,
    interval: i32,
    cron: String,
    zone: String,
) -> Result<(), StoreError> {
    session
        .run(move |connection| {
            Box::pin(async move {
                schedule_refresh_on_connection(connection, id, active, interval, &cron, &zone).await
            })
        })
        .await?
}

struct RemoteLocation {
    name: String,
    url: String,
}

async fn update_remote_location(
    session: &TaskSqlSession,
    id: i64,
    old: Value,
    location: RemoteLocation,
    interval: i32,
    cron: String,
    zone: String,
) -> Result<(), StoreError> {
    session
        .run(move |connection| {
            Box::pin(async move {
                let mut transaction = connection.begin().await?;
                no_custom_triggers(&mut transaction).await?;
                // Compare the locked current row with the validated snapshot.
                // A concurrent editor or refresher can keep its newer state.
                let row: Option<Value> = sqlx::query_scalar(
                    "SELECT to_jsonb(e) FROM epg_epgsource e WHERE id=$1 FOR UPDATE",
                )
                .bind(id)
                .fetch_optional(&mut *transaction)
                .await?;
                if row.ok_or_else(missing)? != old {
                    return Err(unsupported());
                }
                let task = old["refresh_task_id"].as_i64().ok_or_else(unsupported)?;
                let current: Option<String> = sqlx::query_scalar(
                    "SELECT COALESCE(concat_ws(' ',ct.minute,ct.hour,ct.day_of_month,ct.month_of_year,ct.day_of_week),'') FROM django_celery_beat_periodictask p LEFT JOIN django_celery_beat_crontabschedule ct ON ct.id=p.crontab_id WHERE p.id=$1 AND p.name=$2 AND p.task='apps.epg.tasks.refresh_epg_data' FOR UPDATE OF p",
                )
                .bind(task)
                .bind(format!("epg_source-refresh-{id}"))
                .fetch_optional(&mut *transaction)
                .await?;
                if current.as_deref() != Some(cron.as_str()) {
                    return Err(unsupported());
                }
                sqlx::query("UPDATE epg_epgsource SET name=$2,url=$3 WHERE id=$1")
                    .bind(id)
                    .bind(location.name)
                    .bind(location.url)
                    .execute(&mut *transaction)
                    .await
                    .map_err(|error| {
                        if error.as_database_error().is_some_and(|error| error.is_unique_violation()) {
                            field("name", "epg source with this name already exists.")
                        } else {
                            error.into()
                        }
                    })?;
                schedule_refresh_on_connection(&mut transaction, id, true, interval, &cron, &zone).await?;
                transaction.commit().await?;
                Ok(())
            })
        })
        .await?
}

fn sd_logo_style_patch(
    mut input: Map<String, Value>,
    old: &Value,
    cron: &str,
    partial: bool,
) -> Result<Map<String, Value>, StoreError> {
    if !partial
        || old["is_active"] != false
        || old["refresh_interval"] != 24
        || !cron.is_empty()
        || input.len() != 1
    {
        return Err(unsupported());
    }
    let Some(Value::Object(properties)) = input.remove("custom_properties") else {
        return Err(unsupported());
    };
    let style = properties.get("logo_style").and_then(Value::as_str);
    if !matches!(style, Some("dark" | "white" | "gray" | "light")) {
        return Err(unsupported());
    }
    let mut expected = old["custom_properties"]
        .as_object()
        .cloned()
        .ok_or_else(unsupported)?;
    expected.insert("logo_style".into(), json!(style));
    if expected != properties {
        return Err(unsupported());
    }
    Ok(properties)
}

enum SdUpdate {
    LogoStyle(Map<String, Value>),
    SetActive(bool),
}

fn sd_update(
    input: Map<String, Value>,
    old: &Value,
    cron: &str,
    partial: bool,
) -> Result<SdUpdate, StoreError> {
    if let Some(active) = input.get("is_active").and_then(Value::as_bool) {
        if partial
            && input.len() == 1
            && old["is_active"].as_bool() == Some(!active)
            && old["refresh_interval"] == 24
            && cron.is_empty()
        {
            return Ok(SdUpdate::SetActive(active));
        }
        return Err(unsupported());
    }
    sd_logo_style_patch(input, old, cron, partial).map(SdUpdate::LogoStyle)
}

async fn update_sd_settings(
    session: &TaskSqlSession,
    id: i64,
    old: Value,
    action: SdUpdate,
) -> Result<(), StoreError> {
    session
        .run(move |connection| {
            Box::pin(async move {
                let mut transaction = connection.begin().await?;
                no_custom_triggers(&mut transaction).await?;
                let row: Option<Value> = sqlx::query_scalar(
                    "SELECT to_jsonb(e) FROM epg_epgsource e WHERE id=$1 FOR UPDATE",
                )
                .bind(id)
                .fetch_optional(&mut *transaction)
                .await?;
                if row.ok_or_else(missing)? != old {
                    return Err(unsupported());
                }
                let task_id = old["refresh_task_id"].as_i64().ok_or_else(unsupported)?;
                // The logo-style operation always starts from an inactive source.
                let expected_enabled = old["is_active"].as_bool().ok_or_else(unsupported)?;
                let enabling = matches!(action, SdUpdate::SetActive(true));
                let task_matches: Option<bool> = sqlx::query_scalar(
                    "SELECT p.enabled = $5 FROM django_celery_beat_periodictask p
                       JOIN django_celery_beat_intervalschedule i ON i.id=p.interval_id
                       WHERE p.id=$1 AND p.name=$2 AND p.task='apps.epg.tasks.refresh_epg_data'
                         AND p.crontab_id IS NULL AND p.solar_id IS NULL AND p.clocked_id IS NULL
                         AND i.every=24 AND i.period='hours'
                         AND p.args=$3 AND p.kwargs=$4
                         AND (NOT $6 OR (p.last_run_at IS NULL AND p.start_time IS NULL AND NOT p.one_off))
                       FOR UPDATE OF p",
                )
                .bind(task_id)
                .bind(format!("epg_source-refresh-{id}"))
                .bind("[]")
                .bind(format!("{{\"source_id\": {id}}}"))
                .bind(expected_enabled)
                .bind(enabling)
                .fetch_optional(&mut *transaction)
                .await?;
                if task_matches != Some(true) {
                    return Err(unsupported());
                }

                match action {
                    SdUpdate::LogoStyle(properties) => {
                        sqlx::query("UPDATE epg_epgsource SET custom_properties=$2 WHERE id=$1")
                            .bind(id)
                            .bind(sqlx::types::Json(properties))
                            .execute(&mut *transaction)
                            .await?;
                    }
                    SdUpdate::SetActive(active) => {
                        let status = if active { "idle" } else { "disabled" };
                        sqlx::query("UPDATE epg_epgsource SET is_active=$2,status=$3 WHERE id=$1")
                            .bind(id)
                            .bind(active)
                            .bind(status)
                            .execute(&mut *transaction)
                            .await?;
                    }
                }
                // The Original model save synchronizes the same existing Beat task.
                schedule_refresh_on_connection(&mut transaction, id, enabling, 24, "", "UTC").await?;
                transaction.commit().await?;
                Ok(())
            })
        })
        .await?
}

fn validate_sd_editor(
    input: Map<String, Value>,
    old: &Value,
    old_cron: &str,
    partial: bool,
) -> Result<Validated, StoreError> {
    // The React editor saves local metadata and its schedule. An empty
    // password from the editor retains the stored value.
    if !partial
        || old["source_type"] != "schedules_direct"
        || input.is_empty()
        || input.keys().any(|key| {
            !matches!(
                key.as_str(),
                "name"
                    | "source_type"
                    | "username"
                    | "password"
                    | "is_active"
                    | "refresh_interval"
                    | "cron_expression"
                    | "priority"
            )
        })
        || input
            .get("is_active")
            .is_some_and(|value| !value.is_boolean())
    {
        return Err(unsupported());
    }
    validate(input, old, old_cron, partial)
}

async fn update_sd_editor(
    session: &TaskSqlSession,
    id: i64,
    old: Value,
    old_cron: String,
    update: Validated,
    zone: String,
) -> Result<(), StoreError> {
    let name = update.saved["name"]
        .as_str()
        .ok_or_else(unsupported)?
        .to_owned();
    let interval = update.saved["refresh_interval"]
        .as_i64()
        .and_then(|value| i32::try_from(value).ok())
        .ok_or_else(unsupported)?;
    let priority = update.saved["priority"]
        .as_i64()
        .and_then(|value| i32::try_from(value).ok())
        .ok_or_else(unsupported)?;
    let active = update.saved["is_active"]
        .as_bool()
        .ok_or_else(unsupported)?;
    let status = update.saved["status"]
        .as_str()
        .ok_or_else(unsupported)?
        .to_owned();
    let task_id = old["refresh_task_id"].as_i64().ok_or_else(unsupported)?;
    let old_interval = old["refresh_interval"]
        .as_i64()
        .and_then(|value| i32::try_from(value).ok())
        .ok_or_else(unsupported)?;
    session
        .run(move |connection| {
            Box::pin(async move {
                let mut transaction = connection.begin().await?;
                no_custom_triggers(&mut transaction).await?;
                let row: Option<Value> = sqlx::query_scalar(
                    "SELECT to_jsonb(e) FROM epg_epgsource e WHERE id=$1 FOR UPDATE",
                )
                .bind(id)
                .fetch_optional(&mut *transaction)
                .await?;
                if row.ok_or_else(missing)? != old {
                    return Err(unsupported());
                }

                // Revalidate the managed schedule that the editor read. Preserve
                // the task ID and its run history through a cron/interval change.
                let cron: Option<String> = sqlx::query_scalar(
                    "SELECT COALESCE(concat_ws(' ',ct.minute,ct.hour,ct.day_of_month,
                                               ct.month_of_year,ct.day_of_week),'')
                       FROM django_celery_beat_periodictask p
                       LEFT JOIN django_celery_beat_crontabschedule ct ON ct.id=p.crontab_id
                       LEFT JOIN django_celery_beat_intervalschedule iv ON iv.id=p.interval_id
                       WHERE p.id=$1 AND p.name=$2 AND p.task='apps.epg.tasks.refresh_epg_data'
                         AND p.args='[]'
                         AND num_nonnulls(p.interval_id,p.crontab_id)=1
                         AND p.solar_id IS NULL AND p.clocked_id IS NULL
                         AND (
                           (p.kwargs=$3 AND
                            (ct.id IS NOT NULL OR (iv.period='hours' AND iv.every=GREATEST($4,1))))
                           OR (p.kwargs=$5 AND p.crontab_id IS NULL
                               AND iv.period='hours' AND iv.every=24 AND $4=24)
                         )
                       FOR UPDATE OF p",
                )
                .bind(task_id)
                .bind(format!("epg_source-refresh-{id}"))
                .bind(format!("{{\"source_id\": {id}}}"))
                .bind(old_interval)
                // EPG migration 0007 wrote account_id on the linked 24-hour
                // task. The same upsert canonicalizes it to source_id.
                .bind(format!("{{\"account_id\": {id}}}"))
                .fetch_optional(&mut *transaction)
                .await?;
                if cron.as_deref() != Some(old_cron.as_str()) {
                    return Err(unsupported());
                }
                let duplicate: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM epg_epgsource WHERE name=$1 AND id<>$2)",
                )
                .bind(&name)
                .bind(id)
                .fetch_one(&mut *transaction)
                .await?;
                if duplicate {
                    return Err(field("name", "epg source with this name already exists."));
                }

                sqlx::query(
                    "UPDATE epg_epgsource SET name=$2,is_active=$3,refresh_interval=$4,
                                              priority=$5,status=$6 WHERE id=$1",
                )
                .bind(id)
                .bind(name)
                .bind(active)
                .bind(interval)
                .bind(priority)
                .bind(status)
                .execute(&mut *transaction)
                .await?;
                schedule_refresh_on_connection(
                    &mut transaction,
                    id,
                    active,
                    interval,
                    &update.cron,
                    &zone,
                )
                .await?;
                transaction.commit().await?;
                Ok(())
            })
        })
        .await?
}

async fn dummy_signal(
    session: &TaskSqlSession,
    auth: AuthService,
    id: i64,
    name: String,
    owner: Owner,
) -> Result<(), StoreError> {
    let friendly = format!(
        "dummy_{}",
        name.replace([' ', '-'], "_")
            .chars()
            .filter(|v| v.is_alphanumeric() || *v == '_')
            .collect::<String>()
            .to_lowercase()
    );
    dummy_signal_value(session, auth, id, name, friendly, owner).await
}
async fn dummy_signal_value(
    session: &TaskSqlSession,
    auth: AuthService,
    id: i64,
    name: String,
    friendly: String,
    owner: Owner,
) -> Result<(), StoreError> {
    let saved_name = name.clone();
    let (epg,created)=session.run(move|c|Box::pin(async move{
        let existing:Option<(i64,String)>=sqlx::query_as("SELECT id::bigint,name FROM epg_epgdata WHERE epg_source_id=$1 AND tvg_id=$2").bind(id).bind(&friendly).fetch_optional(&mut*c).await?;
        if let Some((epg,old_name))=existing {
            if old_name!=name {sqlx::query("UPDATE epg_epgdata SET name=$2 WHERE id=$1").bind(epg).bind(name).execute(c).await?;}
            return Ok::<_,StoreError>((epg,false));
        }
        let inserted:Option<i64>=sqlx::query_scalar("INSERT INTO epg_epgdata(tvg_id,name,icon_url,epg_source_id) VALUES($1,$2,NULL,$3) ON CONFLICT(tvg_id,epg_source_id) DO NOTHING RETURNING id::bigint").bind(&friendly).bind(&name).bind(id).fetch_optional(&mut*c).await?;
        if let Some(epg)=inserted {return Ok((epg,true));}
        let(epg,old_name):(i64,String)=sqlx::query_as("SELECT id::bigint,name FROM epg_epgdata WHERE epg_source_id=$1 AND tvg_id=$2").bind(id).bind(friendly).fetch_one(&mut*c).await?;
        if old_name!=name{sqlx::query("UPDATE epg_epgdata SET name=$2 WHERE id=$1").bind(epg).bind(name).execute(c).await?;}
        Ok((epg,false))
    })).await??;
    if created {
        let sink = auth.event_sink.clone().ok_or_else(unsupported)?;
        crate::core_task_offsets::retained_event(sink,json!({"type":"update","data":{"type":"epg_data_created","source_id":id,"source_name":saved_name,"epg_data_id":epg}}),owner).await?;
    }
    Ok(())
}
// The shared JSON parser retains number lexemes. Source's JSON parser creates
// Python int/float values before JSONField sees nested properties. Preserve that
// distinction before BOTH persistence and the in-memory create representation.
fn dummy_json_numbers(value: &mut Value) -> Result<(), StoreError> {
    match value {
        Value::Array(values) => {
            for value in values {
                dummy_json_numbers(value)?;
            }
        }
        Value::Object(values) => {
            for value in values.values_mut() {
                dummy_json_numbers(value)?;
            }
        }
        Value::Number(number) => {
            let raw = number.to_string();
            if raw.bytes().any(|value| matches!(value, b'.' | b'e' | b'E')) {
                let float = raw.parse::<f64>().map_err(|_| unsupported())?;
                *number = serde_json::Number::from_f64(float).ok_or_else(unsupported)?;
            } else {
                let integer =
                    num_bigint::BigInt::parse_bytes(raw.as_bytes(), 10).ok_or_else(unsupported)?;
                *number = serde_json::from_str::<Value>(&integer.to_string())
                    .map_err(|_| unsupported())?
                    .as_number()
                    .ok_or_else(unsupported)?
                    .clone();
            }
        }
        _ => {}
    }
    Ok(())
}
// Dummy create follows the original serializer/pre-save/post-save sequence.
// No dummy refresh actor or Beat publication exists in the Source signal graph.
pub(super) async fn create_dummy(
    auth: AuthService,
    value: &Value,
    owner: Owner,
    names: &crate::RegexExecutor,
) -> Result<Value, StoreError> {
    let mut input = value.as_object().ok_or_else(unsupported)?.clone();
    // Source-qualified SD CharField strip predicate includes Python's four
    // extra ASCII whitespace controls. Validation still owns length/errors.
    if let Some(Value::String(name)) = input.get_mut("name") {
        *name = name
            .trim_matches(|c: char| c.is_whitespace() || matches!(c, '\u{1c}'..='\u{1f}'))
            .to_owned();
    }
    let defaults = json!({"name":"","source_type":"dummy","url":null,
        "username":null,"password":null,"is_active":true,"file_path":null,
        "refresh_interval":0,"priority":0,"status":"idle","last_message":null,
        "custom_properties":{},"updated_at":null});
    // Create does not share update's empty-password preservation semantics.
    if ["username", "password"]
        .iter()
        .any(|key| input.get(*key).is_some_and(|value| !value.is_null()))
    {
        return Err(unsupported());
    }
    if let Some(properties) = input.get_mut("custom_properties") {
        dummy_json_numbers(properties)?;
    }
    let validated = validate(input, &defaults, "", false)?;
    let saved = validated.saved;
    // URL/file/credential and nonempty cron domains stay separate.
    // Unicode name normalization uses the pinned family dependency below.
    if saved["source_type"] != "dummy"
        || !validated.cron.is_empty()
        || !saved["url"].is_null()
        || !saved["file_path"].is_null()
        || !saved["username"].is_null()
        || !saved["password"].is_null()
        || auth.event_sink.is_none()
    {
        return Err(unsupported());
    }
    let name = saved["name"].as_str().ok_or_else(unsupported)?.to_owned();
    let updated = parse_datetime("updated_at", &saved["updated_at"], true)?;
    let response_properties = saved["custom_properties"].clone();
    let friendly = names
        .dummy_epg_name(&name, owner.clone())
        .await
        .map_err(|_| unsupported())?;
    let session = auth.borrow_task_sql_session(owner.clone()).await?;
    let result = async {
        let saved_name = name.clone();
        let id = session.run(move |c| Box::pin(async move {
            no_custom_triggers(c).await?;
            let collision: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM epg_epgsource WHERE name=$1)",
            ).bind(&name).fetch_one(&mut *c).await?;
            if collision {
                return Err(field("name", "epg source with this name already exists."));
            }
            // Source autocommits its source row before dummy EPGData and WS.
            // Do not make event delivery or the follow-up signal transactional.
            let id: i64 = sqlx::query_scalar(
                "INSERT INTO epg_epgsource(name,source_type,url,username,password,is_active,file_path,extracted_file_path,refresh_interval,refresh_task_id,created_at,updated_at,status,last_message,custom_properties,priority) VALUES($1,'dummy',NULL,NULL,NULL,$2,NULL,NULL,$3,NULL,$4,$5,'idle',NULL,$6,$7) RETURNING id::bigint",
            ).bind(name).bind(saved["is_active"].as_bool())
                .bind(saved["refresh_interval"].as_i64().ok_or_else(unsupported)? as i32)
                .bind(chrono::Utc::now()).bind(updated)
                .bind(if saved["custom_properties"].is_null() { None } else {
                    Some(sqlx::types::Json(saved["custom_properties"].clone()))
                })
                .bind(saved["priority"].as_i64().ok_or_else(unsupported)? as i32)
                .fetch_one(c).await?;
            Ok::<_, StoreError>(id)
        })).await??;
        // No friendly-ID length check before INSERT: Source may commit its
        // source row and then fail EPGData's255-character DB constraint.
        dummy_signal_value(&session, auth.clone(), id, saved_name, friendly, owner).await?;
        let mut reply = project(&session, id, Some(String::new())).await?.0;
        // Source serializes the created model's original JSON value; jsonb's
        // storage key ordering must not replace that public response ordering.
        reply["custom_properties"] = response_properties;
        Ok::<_, StoreError>(reply)
    }.await;
    let closed = session.close().await;
    closed?;
    // Serializer/model save faults escape DRF as Django500. In particular,
    // a long derived tvg_id fails only AFTER the source row was committed.
    result.map_err(|error| match error {
        StoreError::Database(_) => StoreError::UnhandledRestFault,
        error => error,
    })
}
const DETAIL_ALLOW: HeaderValue =
    HeaderValue::from_static("GET, PUT, PATCH, DELETE, HEAD, OPTIONS");

async fn change(
    auth: AuthService,
    peer: SocketAddr,
    headers: HeaderMap,
    raw: String,
    body: Body,
    partial: bool,
) -> Result<(Json<Value>, bool), StoreError> {
    auth.account_user(peer, headers, true).await?;
    let id = key(raw)?;
    let owner: Owner = Arc::new(auth.clone());
    let session = auth.borrow_task_sql_session(owner.clone()).await?;
    let operation = tokio::spawn(async move {
        let outcome=async{
            let row=session.run(move|c|Box::pin(async move{
                sqlx::query("SELECT to_jsonb(e) AS raw,COALESCE(concat_ws(' ',ct.minute,ct.hour,ct.day_of_month,ct.month_of_year,ct.day_of_week),'') AS cron FROM epg_epgsource e LEFT JOIN django_celery_beat_periodictask p ON p.id=e.refresh_task_id LEFT JOIN django_celery_beat_crontabschedule ct ON ct.id=p.crontab_id WHERE e.id=$1").bind(id).fetch_optional(c).await
            })).await??.ok_or_else(missing)?;
            let old:Value=row.try_get("raw")?;let old_cron:String=row.try_get("cron")?;
            let input = body_object(body)?;
            if old["source_type"] == "schedules_direct" {
                if let Ok(action) = sd_update(input.clone(), &old, &old_cron, partial) {
                    update_sd_settings(&session, id, old, action).await?;
                    return Ok((project(&session, id, Some(old_cron)).await?, true));
                }
                let update = validate_sd_editor(input, &old, &old_cron, partial)?;
                let cron = update.cron.clone();
                let zone = schedule_timezone(&session, auth.clone(), &cron).await?;
                update_sd_editor(&session, id, old, old_cron, update, zone).await?;
                return Ok((project(&session, id, Some(cron)).await?, true));
            }
            let Validated {saved,cron,url_changed}=validate(input,&old,&old_cron,partial)?;
            let dummy=saved["source_type"]=="dummy";
            if dummy && (!old["refresh_task_id"].is_null() || auth.event_sink.is_none() || !captured_dummy_name_domain(&saved)){return Err(unsupported());}
            let zone=if dummy {"UTC".into()}else{schedule_timezone(&session,auth.clone(),&cron).await?};
            if url_changed {
                let name = saved["name"].as_str().ok_or_else(unsupported)?.to_owned();
                let url = saved["url"].as_str().ok_or_else(unsupported)?.to_owned();
                let interval = saved["refresh_interval"].as_i64().and_then(|value| i32::try_from(value).ok()).ok_or_else(unsupported)?;
                update_remote_location(&session,id,old,RemoteLocation {name,url},interval,cron.clone(),zone).await?;
                return Ok((project(&session,id,Some(cron)).await?,true));
            }
            let saved_name=saved["name"].as_str().ok_or_else(unsupported)?.to_owned();
            let updated=saved.get("updated_at").cloned().unwrap_or(Value::Null);let updated=parse_datetime("updated_at",&updated,true)?;
            let new_name=saved_name.clone();let new_value=saved.clone();
            session.run(move|c|Box::pin(async move{
                no_custom_triggers(c).await?;
                let collision:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM epg_epgsource WHERE name=$1 AND id<>$2)").bind(&new_name).bind(id).fetch_one(&mut*c).await?;
                if collision{return Err(field("name","epg source with this name already exists."));}
                sqlx::query("UPDATE epg_epgsource SET name=$2,source_type=$3,url=$4,username=$5,password=$6,is_active=$7,file_path=$8,refresh_interval=$9,priority=$10,status=$11,last_message=$12,custom_properties=$13,updated_at=$14 WHERE id=$1")
                    .bind(id).bind(new_name).bind(new_value["source_type"].as_str()).bind(new_value["url"].as_str()).bind(new_value["username"].as_str()).bind(new_value["password"].as_str()).bind(new_value["is_active"].as_bool()).bind(new_value["file_path"].as_str()).bind(new_value["refresh_interval"].as_i64().ok_or_else(unsupported)? as i32).bind(new_value["priority"].as_i64().ok_or_else(unsupported)? as i32).bind(new_value["status"].as_str()).bind(new_value["last_message"].as_str()).bind(if new_value["custom_properties"].is_null(){None}else{Some(sqlx::types::Json(new_value["custom_properties"].clone()))}).bind(updated).execute(c).await?;
                Ok::<_,StoreError>(())
            })).await??;
            if dummy{dummy_signal(&session,auth.clone(),id,saved_name,owner.clone()).await?;}
            else{
                let interval=saved["refresh_interval"].as_i64().ok_or_else(unsupported)? as i32;
                let active=saved["is_active"].as_bool().ok_or_else(unsupported)?;
                schedule_refresh(&session,id,active,interval,cron.clone(),zone).await?;
            }
            Ok((project(&session,id,Some(cron)).await?,false))
        }.await;
        let close = session.close().await;
        let value = outcome?;
        close?;
        Ok::<_, StoreError>(value)
    });
    operation
        .await
        .map_err(|_| StoreError::Unsupported("EPG update supervisor failed"))?
}
pub(crate) async fn put(
    State(auth): State<AuthService>,
    Path(id): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Value>, StoreError> {
    change(auth, peer, headers, id, body, false)
        .await
        .map(|(reply, _)| reply)
}
pub(crate) async fn patch(
    State(auth): State<AuthService>,
    Path(id): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, StoreError> {
    let (body, detail_allow) = change(auth, peer, headers, id, body, true).await?;
    let mut response = body.into_response();
    if detail_allow {
        response.headers_mut().insert(ALLOW, DETAIL_ALLOW);
    }
    Ok(response)
}

async fn delete_files(paths: Vec<String>, owner: Owner) -> Result<(), StoreError> {
    tokio::task::spawn_blocking(move || {
        let _retained = owner;
        for path in paths {
            // Source uses a textual prefix check, deliberately not canonicalize.
            if path.starts_with("/data/epgs")
                || path.starts_with("/data/uploads")
                || path.starts_with("/data/m3us")
            {
                continue;
            }
            if std::path::Path::new(&path).exists() && std::fs::remove_file(&path).is_err() {
                eprintln!("EPG delete: caught cached file unlink failure");
            }
        }
    })
    .await
    .map_err(|_| unsupported())
}

async fn clear_channel_epg_relations(c: &mut PgConnection, source: i64) -> Result<(), StoreError> {
    // Hold the referenced rows through deletion so concurrent PostgreSQL FK
    // checks cannot introduce a channel link after the collector clears them.
    sqlx::query("SELECT id FROM epg_epgdata WHERE epg_source_id=$1 ORDER BY id FOR UPDATE")
        .bind(source)
        .execute(&mut *c)
        .await?;
    for table in [
        "dispatcharr_channels_channel",
        "dispatcharr_channels_channeloverride",
    ] {
        let referenced: bool = sqlx::query_scalar(&format!(
            "SELECT EXISTS(SELECT 1 FROM {table} WHERE epg_data_id IN \
             (SELECT id FROM epg_epgdata WHERE epg_source_id=$1))"
        ))
        .bind(source)
        .fetch_one(&mut *c)
        .await?;
        if !referenced {
            continue;
        }
        let custom: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE NOT tgisinternal \
             AND tgenabled<>'D' AND tgrelid=$1::text::regclass)",
        )
        .bind(table)
        .fetch_one(&mut *c)
        .await?;
        if custom {
            return Err(unsupported());
        }
        // Django's SET_NULL collector updates just the FK column, including
        // override rows that would otherwise still refer to the removed guide.
        sqlx::query(&format!(
            "UPDATE {table} SET epg_data_id=NULL WHERE epg_data_id IN \
             (SELECT id FROM epg_epgdata WHERE epg_source_id=$1)"
        ))
        .bind(source)
        .execute(&mut *c)
        .await?;
    }
    Ok(())
}

pub(crate) async fn delete(
    State(auth): State<AuthService>,
    Path(raw): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<StatusCode, StoreError> {
    auth.account_user(peer, headers, true).await?;
    let id = key(raw)?;
    let owner: Owner = Arc::new(auth.clone());
    let session = auth.borrow_task_sql_session(owner.clone()).await?;
    let operation = tokio::spawn(async move {
        let outcome=session.run(move|c|Box::pin(async move{
            let mut tx=c.begin().await?;
            let result=async{
                let old:Option<Value>=sqlx::query_scalar("SELECT to_jsonb(e) FROM epg_epgsource e WHERE id=$1 FOR UPDATE").bind(id).fetch_optional(&mut*tx).await?;let old=old.ok_or_else(missing)?;
                if !matches!(old["source_type"].as_str(),Some("xmltv"|"dummy")){return Err(unsupported());}
                let mut paths=Vec::new();
                for path in [old["file_path"].as_str(),old["extracted_file_path"].as_str()].into_iter().flatten().filter(|p|!p.is_empty()){if !paths.iter().any(|v|v==path){paths.push(path.to_owned());}}
                if paths.iter().any(|p|!p.starts_with("/data/")||p.len()>1024){return Err(unsupported());}
                no_custom_triggers(&mut tx).await?;
                clear_channel_epg_relations(&mut tx, id).await?;
                sqlx::query("DELETE FROM epg_programdata WHERE epg_id IN(SELECT id FROM epg_epgdata WHERE epg_source_id=$1)").bind(id).execute(&mut*tx).await?;
                sqlx::query("DELETE FROM epg_epgsourceindex WHERE source_id=$1").bind(id).execute(&mut*tx).await?;
                sqlx::query("DELETE FROM epg_sdprogrammd5 WHERE epg_source_id=$1").bind(id).execute(&mut*tx).await?;
                sqlx::query("DELETE FROM epg_sdschedulemd5 WHERE epg_source_id=$1").bind(id).execute(&mut*tx).await?;
                sqlx::query("DELETE FROM epg_epgdata WHERE epg_source_id=$1").bind(id).execute(&mut*tx).await?;
                sqlx::query("DELETE FROM epg_epgsource WHERE id=$1").bind(id).execute(&mut*tx).await?;
                // Legacy EPG helper is intentionally NOT core.delete_periodic_task:
                // it deletes the named task, cleans only an orphan INTERVAL,
                // and leaves orphan CRON rows (actual Source2 retains cron5).
                let name=format!("epg_source-refresh-{id}");
                // Reject nonlocal refresh-task collectors before deleting the
                // named task; their SET_NULL branches lack Source qualification.
                let refs:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM epg_epgsource WHERE refresh_task_id=(SELECT id FROM django_celery_beat_periodictask WHERE name=$1) UNION ALL SELECT 1 FROM m3u_m3uaccount WHERE refresh_task_id=(SELECT id FROM django_celery_beat_periodictask WHERE name=$1))").bind(&name).fetch_one(&mut*tx).await?;
                if refs{return Err(unsupported());}
                crate::provider_schedule::legacy_delete(&mut tx,&name).await?;
                delete_files(paths,owner).await?;
                Ok::<_,StoreError>(())
            }.await;
            match result{Ok(())=>{tx.commit().await?;Ok(())},Err(e)=>{tx.rollback().await?;Err(e)}}
        })).await;
        let close = session.close().await;
        outcome??;
        close?;
        Ok::<_, StoreError>(StatusCode::NO_CONTENT)
    });
    operation
        .await
        .map_err(|_| StoreError::Unsupported("EPG delete supervisor failed"))?
}

#[cfg(test)]
mod tests {
    use super::*;
    fn old() -> Value {
        json!({"id":3,"name":"Old","source_type":"dummy","url":null,"username":null,"password":null,"is_active":false,"file_path":null,"refresh_interval":0,"priority":0,"status":"idle","last_message":null,"custom_properties":{},"updated_at":null})
    }
    #[test]
    fn schedules_direct_logo_style_accepts_react_options_without_changing_other_fields() {
        let old = json!({"source_type":"schedules_direct", "is_active":false, "refresh_interval":24,
            "custom_properties":{"logo_style":"dark","fetch_posters":false,"poster_style":"sd_recommended",
                "sd_extra_debugging":false,"auto_apply_epg_logos":false,"existing":{"keep":7}}});
        for style in ["dark", "white", "gray", "light"] {
            let mut expected = old["custom_properties"].clone();
            expected["logo_style"] = json!(style);
            let body = json!({"custom_properties": expected});
            let saved =
                sd_logo_style_patch(body.as_object().unwrap().clone(), &old, "", true).unwrap();
            assert_eq!(Value::Object(saved), expected);
        }
    }

    #[test]
    fn schedules_direct_logo_style_keeps_other_updates_outside_scope() {
        let mut old = json!({"source_type":"schedules_direct", "is_active":false, "refresh_interval":24,
            "custom_properties":{"logo_style":"dark","fetch_posters":false}});
        let positive = json!({"custom_properties":{"logo_style":"light","fetch_posters":false}});
        let command = positive.as_object().unwrap();
        assert!(sd_logo_style_patch(command.clone(), &old, "", false).is_err());
        assert!(sd_logo_style_patch(command.clone(), &old, "17 4 29 2 *", true).is_err());
        for input in [
            json!({"custom_properties":{"logo_style":"unknown","fetch_posters":false}}),
            json!({"custom_properties":{"logo_style":"light"}}),
            json!({"custom_properties":{"logo_style":"light","fetch_posters":true}}),
            json!({"custom_properties":{"logo_style":"light","fetch_posters":false},"name":"Other"}),
        ] {
            assert!(
                sd_logo_style_patch(input.as_object().unwrap().clone(), &old, "", true).is_err()
            );
        }
        old["is_active"] = json!(true);
        assert!(sd_logo_style_patch(command.clone(), &old, "", true).is_err());
    }

    #[test]
    fn schedules_direct_active_switch_accepts_only_boolean_transitions_on_the_observed_interval() {
        let mut old =
            json!({"source_type":"schedules_direct", "is_active":true, "refresh_interval":24});
        let command = json!({"is_active":false});
        let command = command.as_object().unwrap();
        assert!(matches!(
            sd_update(command.clone(), &old, "", true),
            Ok(SdUpdate::SetActive(false))
        ));
        assert!(sd_update(command.clone(), &old, "", false).is_err());
        assert!(sd_update(command.clone(), &old, "17 4 29 2 *", true).is_err());
        for outside in [
            json!({"is_active":true}),
            json!({"is_active":"false"}),
            json!({"is_active":false,"status":"idle"}),
        ] {
            assert!(sd_update(outside.as_object().unwrap().clone(), &old, "", true).is_err());
        }
        old["refresh_interval"] = json!(12);
        assert!(sd_update(command.clone(), &old, "", true).is_err());
        old["refresh_interval"] = json!(24);
        old["is_active"] = json!(false);
        assert!(sd_update(command.clone(), &old, "", true).is_err());
        let body = json!({"is_active":true});
        let activate = body.as_object().unwrap();
        assert!(matches!(
            sd_update(activate.clone(), &old, "", true),
            Ok(SdUpdate::SetActive(true))
        ));
        assert!(sd_update(activate.clone(), &old, "", false).is_err());
        assert!(sd_update(activate.clone(), &old, "17 4 29 2 *", true).is_err());
    }

    fn sd_editor_row() -> Value {
        let mut row = old();
        row["source_type"] = json!("schedules_direct");
        row["refresh_interval"] = json!(24);
        row["refresh_task_id"] = json!(14);
        row["username"] = json!("fixture-user");
        row["password"] = json!("fixture-only");
        row["custom_properties"] = json!({"logo_style": "dark"});
        row
    }

    #[test]
    fn schedules_direct_react_editor_updates_local_fields_and_keeps_provider_data() {
        let before = sd_editor_row();
        let request = json!({
            "name": "Evening guide",
            "source_type": "schedules_direct",
            "username": "fixture-user",
            "password": "",
            "is_active": false,
            "refresh_interval": 0,
            "cron_expression": "25 3 * * *",
            "priority": 10,
        });
        let fields = request.as_object().unwrap();
        assert!(sd_update(fields.clone(), &before, "", true).is_err());
        let update = validate_sd_editor(fields.clone(), &before, "", true).unwrap();
        let mut expected = before.clone();
        expected["name"] = json!("Evening guide");
        expected["refresh_interval"] = json!(0);
        expected["priority"] = json!(10);
        assert_eq!(update.saved, expected);
        assert_eq!(update.cron, "25 3 * * *");
        assert!(!update.url_changed);
    }

    #[test]
    fn schedules_direct_editor_toggle_and_interval_follow_saved_cron_semantics() {
        let mut before = sd_editor_row();
        before["refresh_interval"] = json!(0);
        let cron = "25 3 * * *";
        let toggle = json!({"is_active": true});
        let update =
            validate_sd_editor(toggle.as_object().unwrap().clone(), &before, cron, true).unwrap();
        assert_eq!(update.saved["status"], "idle");
        assert_eq!(update.saved["is_active"], true);
        assert_eq!(update.cron, cron);

        let interval = json!({"refresh_interval": 8, "cron_expression": ""});
        let update =
            validate_sd_editor(interval.as_object().unwrap().clone(), &before, cron, true).unwrap();
        assert_eq!(update.saved["refresh_interval"], 8);
        assert_eq!(update.cron, "");
    }

    #[test]
    fn schedules_direct_editor_preserves_other_workflows() {
        let before = sd_editor_row();
        for request in [
            json!({"username": "another-fixture-user"}),
            json!({"password": "replacement-fixture"}),
            json!({"source_type": "xmltv"}),
            json!({"custom_properties": {"logo_style": "white"}}),
        ] {
            assert!(
                validate_sd_editor(request.as_object().unwrap().clone(), &before, "", true)
                    .is_err()
            );
        }
    }

    #[test]
    fn dummy_rename_clears_only_signal_owned_status_fields() {
        let v=validate(json!({"name":"Renamed","source_type":"dummy","status":"error","last_message":"discard"}).as_object().unwrap().clone(),&old(),"",false).unwrap();
        assert_eq!(v.saved["status"], "idle");
        assert_eq!(v.saved["last_message"], Value::Null);
        assert_eq!(v.saved["is_active"], false);
    }
    #[test]
    fn active_xmltv_url_patch_preserves_the_row_and_schedule() {
        let mut old = old();
        old["source_type"] = json!("xmltv");
        old["url"] = json!("https://guide.example.invalid/prior.xml");
        old["username"] = json!("");
        old["password"] = json!("");
        old["is_active"] = json!(true);
        old["status"] = json!("success");
        old["file_path"] = json!("/data/cached_epg/owned-prior.xml");
        old["refresh_task_id"] = json!(14);
        let cron = "17 4 29 2 *";
        let request = json!({
            "name":"Old","source_type":"xmltv","url":"https://guide.example.invalid/replacement.xml",
            "username":"","password":"","is_active":true,"refresh_interval":0,
            "cron_expression":cron,"priority":0
        });
        let input = request.as_object().unwrap();
        let updated = validate(input.clone(), &old, cron, true).unwrap();
        assert!(updated.url_changed);
        assert_eq!(updated.cron, cron);
        let mut expected = old.clone();
        expected["url"] = request["url"].clone();
        assert_eq!(updated.saved, expected);

        assert!(validate(input.clone(), &old, cron, false).is_err());
        for (key, value) in [
            ("url", json!("https://another.example.invalid/guide.xml")),
            ("priority", json!(23)),
            ("is_active", json!(false)),
            ("cron_expression", json!("18 4 29 2 *")),
        ] {
            let mut extra = input.clone();
            extra.insert(key.into(), value);
            assert!(validate(extra, &old, cron, true).is_err(), "{key}");
        }
    }

    #[test]
    fn active_xmltv_form_can_rename_and_update_the_same_origin_url() {
        let mut old = old();
        old["source_type"] = json!("xmltv");
        old["url"] = json!("https://guide.example.invalid/old.xml");
        old["is_active"] = json!(true);
        old["username"] = json!("");
        old["password"] = json!("");
        old["refresh_interval"] = json!(24);
        old["priority"] = json!(0);
        old["refresh_task_id"] = json!(97);
        let input = json!({
            "name":"Owned new guide","source_type":"xmltv",
            "url":"https://guide.example.invalid/new.xml","username":"","password":"",
            "is_active":true,"refresh_interval":24,"cron_expression":"","priority":0
        });
        let validated = validate(input.as_object().unwrap().clone(), &old, "", true).unwrap();
        assert!(validated.url_changed);
        let mut expected = old;
        expected["name"] = input["name"].clone();
        expected["url"] = input["url"].clone();
        assert_eq!(validated.saved, expected);
        assert!(validated.cron.is_empty());
    }

    #[test]
    fn patch_preserves_cron_but_full_put_uses_serializer_default() {
        let mut old = old();
        old["source_type"] = json!("xmltv");
        assert_eq!(
            validate(Map::new(), &old, "11 4 29 2 *", true)
                .unwrap()
                .cron,
            "11 4 29 2 *"
        );
        assert_eq!(
            validate(
                json!({"name":"Old","source_type":"xmltv"})
                    .as_object()
                    .unwrap()
                    .clone(),
                &old,
                "11 4 29 2 *",
                false
            )
            .unwrap()
            .cron,
            ""
        );
    }
    #[test]
    fn status_presave_follows_parsed_boolean_but_partial_view_uses_raw_truth() {
        let mut old = old();
        old["source_type"] = json!("xmltv");
        old["status"] = json!("disabled");
        assert_eq!(
            validate(
                json!({"is_active":true}).as_object().unwrap().clone(),
                &old,
                "",
                true
            )
            .unwrap()
            .saved["status"],
            "idle"
        );
        assert_eq!(
            validate(
                json!({"is_active":"false"}).as_object().unwrap().clone(),
                &old,
                "",
                true
            )
            .unwrap()
            .saved["status"],
            "idle"
        );
    }
}
