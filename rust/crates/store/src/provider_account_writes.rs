//! Source M3U account lifecycle draft. All original Dispatcharr/Django/Celery
//! application and algorithm credit remains with the original authors.
//! Bounded JSON STD and XC writes. Multipart uploads and VOD content collectors are separate.
use crate::{
    admin_writes::{body_object, boolean, character, integer, parse_datetime},
    core_settings_special_budget::Scratch,
    xc_account_tasks::SavedProfile,
    AuthService, StoreError, TaskSqlSession,
};
use axum::{
    extract::{rejection::JsonRejection, ConnectInfo, Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Map, Value};
use sqlx::{Connection, PgConnection, Row};
use std::{collections::HashSet, net::SocketAddr, sync::Arc};
type Body = Result<Json<Value>, JsonRejection>;
type Owner = Arc<dyn Send + Sync>;
fn unsupported() -> StoreError {
    StoreError::Unsupported("Account lifecycle outside Source-qualified bounded STD JSON domain")
}
fn missing() -> StoreError {
    StoreError::StatusBody(
        404,
        json!({"detail":"No M3UAccount matches the given query."}),
    )
}
fn field(name: &str, message: &str) -> StoreError {
    StoreError::InvalidInput(json!({name:[message]}))
}
fn locked() -> StoreError {
    StoreError::StatusBody(
        400,
        json!({"error":"Cannot modify or delete a locked M3U account"}),
    )
}
fn path_key(raw: String) -> Result<i64, StoreError> {
    raw.parse().map_err(|_| StoreError::NotFound)
}
/// Positive HTTP(S) subset of the Source flexible URL validator.
pub(super) fn ordinary_provider_http_url(text: &str) -> bool {
    url::Url::parse(text).is_ok_and(|parsed| {
        matches!(parsed.scheme(), "http" | "https")
            && parsed.host_str().is_some()
            && parsed.username().is_empty()
            && parsed.password().is_none()
            && parsed.fragment().is_none()
    })
}

fn defaults() -> Value {
    json!({"id":null,"name":"","server_url":null,"file_path":null,
        "server_group_id":null,"max_streams":0,"is_active":true,
        "created_at":null,"updated_at":null,"status":"idle","last_message":null,
        "user_agent_id":null,"locked":false,"stream_profile_id":null,
        "account_type":"STD","username":null,"password":null,
        "custom_properties":{},"refresh_interval":0,"refresh_task_id":null,
        "stale_stream_days":7,"priority":0})
}
fn properties(value: &Value) -> Map<String, Value> {
    value
        .as_object()
        .cloned()
        .or_else(|| {
            value
                .as_str()
                .and_then(|v| serde_json::from_str::<Value>(v).ok())
                .and_then(|v| v.as_object().cloned())
        })
        .unwrap_or_default()
}
fn nullable_text(name: &str, value: &Value, max: Option<usize>) -> Result<Value, StoreError> {
    if value.is_null() {
        Ok(Value::Null)
    } else {
        character(name, value, max, true).map(Value::String)
    }
}
fn positive(name: &str, value: &Value) -> Result<Value, StoreError> {
    let minimum = || field(name, "Ensure this value is greater than or equal to 0.");
    let n = integer(name, value).map_err(|error| match error {
        StoreError::InvalidInput(ref v)
            if v[name] == json!(["Ensure this value is greater than or equal to -2147483648."]) =>
        {
            minimum()
        }
        other => other,
    })?;
    if n < 0 {
        Err(minimum())
    } else {
        Ok(json!(n))
    }
}
fn hash_keys(value: &Value) -> Result<Vec<String>, StoreError> {
    if value.is_null() {
        return Ok(Vec::new());
    }
    let text = character("hash_key", value, None, true)?;
    let mut keys = Vec::new();
    let mut invalid = Vec::new();
    for part in text.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        if !["name", "url", "tvg_id", "m3u_id", "group"].contains(&part) {
            if !invalid.iter().any(|v| v == part) {
                invalid.push(part.to_owned());
            }
            continue;
        }
        if !keys.iter().any(|key| key == part) {
            keys.push(part.to_owned());
        }
    }
    if !invalid.is_empty() {
        return Err(field(
            "hash_key",
            &format!(
                "Invalid hash key field(s): {}. Allowed fields: group, m3u_id, name, tvg_id, url.",
                invalid.join(", ")
            ),
        ));
    }
    if !text.is_empty() && keys.is_empty() {
        return Err(field("hash_key","hash_key must include at least one valid field (name, url, tvg_id, m3u_id, group), or be empty to inherit the global default."));
    }
    Ok(keys)
}
fn xc_editor_update(
    input: &Map<String, Value>,
    old: &Value,
    saved: &Value,
    edited: &[&str],
) -> bool {
    if input.get("name").is_some_and(|value| !value.is_string())
        || input
            .get("enable_vod")
            .is_some_and(|value| !value.is_boolean())
        || !input.keys().all(|key| {
            matches!(
                key.as_str(),
                "name"
                    | "server_url"
                    | "server_group"
                    | "max_streams"
                    | "is_active"
                    | "user_agent"
                    | "refresh_interval"
                    | "account_type"
                    | "username"
                    | "stale_stream_days"
                    | "priority"
                    | "enable_vod"
                    | "hash_key"
                    | "cron_expression"
            )
        })
    {
        return false;
    }
    if let Some(url) = input.get("server_url").and_then(Value::as_str) {
        // Source's flexible hostname fallback provides positive authority
        // for the URL already stored in an ordinary form echo.
        static FLEXIBLE_URL: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
            regex::Regex::new(
                r"^(rts?p|https?|udp)://([a-zA-Z0-9_\-.]+:[^\s@]+@)?([a-zA-Z0-9]([a-zA-Z0-9_\-.]{0,61}[a-zA-Z0-9])?|[0-9.]+)?(:[0-9]+)?(/[^\s]*)?$",
            )
            .expect("pinned flexible URL fallback")
        });
        if !url.is_empty() && !FLEXIBLE_URL.is_match(url) {
            return false;
        }
    }
    let mut expected_properties = properties(&old["custom_properties"]);
    if input.get("enable_vod") == Some(&Value::Bool(false)) {
        expected_properties
            .entry("enable_vod")
            .or_insert(Value::Bool(false));
    }
    saved["custom_properties"] == Value::Object(expected_properties)
        && saved.as_object().is_some_and(|account| {
            account.iter().all(|(key, value)| {
                edited.contains(&key.as_str())
                    || key == "custom_properties"
                    || old.get(key) == Some(value)
            })
        })
}
// The basic React Cron Builder emits canonical ASCII fields for daily runs.
fn xc_daily_cron(expression: &str) -> bool {
    let fields: Vec<_> = expression.splitn(6, ' ').collect();
    let [minute, hour, "*", "*", "*"] = fields.as_slice() else {
        return false;
    };
    let valid_number = |field: &str, max: u8| {
        field
            .parse::<u8>()
            .is_ok_and(|number| number <= max && number.to_string() == field)
    };
    valid_number(minute, 59) && valid_number(hour, 23)
}
struct Validated {
    saved: Value,
    cron: String,
    expiration: Option<Option<chrono::DateTime<chrono::Utc>>>,
    hash_changed: bool,
    group_selection: Vec<(i64, bool)>,
}
fn validate(
    mut input: Map<String, Value>,
    old: &Value,
    previous_cron: &str,
    partial: bool,
    create: bool,
) -> Result<Validated, StoreError> {
    // The playlist table toggles an existing XC account with one Boolean.
    let xc_active_toggle = !create
        && partial
        && old["account_type"] == "XC"
        && (old["custom_properties"].is_object() || old["custom_properties"].is_null())
        && input.len() == 1
        && input.get("is_active").is_some_and(Value::is_boolean);
    // Source PATCH view mutates status using raw request truth before serializer.
    if partial {
        if let Some(raw) = input.get("is_active") {
            if !crate::json_compat::python_equal(&old["is_active"], raw) {
                let truth = match raw {
                    Value::Null => false,
                    Value::Bool(v) => *v,
                    Value::String(v) => !v.is_empty(),
                    Value::Number(v) => v.as_f64().is_some_and(|v| v != 0.0),
                    Value::Array(v) => !v.is_empty(),
                    Value::Object(v) => !v.is_empty(),
                };
                input.insert(
                    "status".into(),
                    json!(if truth { "idle" } else { "disabled" }),
                );
            }
        }
    }
    if input.contains_key("locked") {
        return Err(unsupported());
    }
    let create_account_type = input.get("account_type").and_then(Value::as_str);
    // Remote live STD playlists use the same ordinary HTTP(S) URL shape as XC.
    let remote_std_create = create_account_type == Some("STD")
        && input
            .get("file_path")
            .is_none_or(|value| value.is_null() || value.as_str() == Some(""))
        && matches!(input.get("enable_vod"), None | Some(Value::Bool(false)));
    let mut saved = old.clone();
    let mut errors = Map::new();
    for name in [
        "name",
        "server_url",
        "file_path",
        "server_group",
        "max_streams",
        "is_active",
        "updated_at",
        "user_agent",
        "refresh_interval",
        "account_type",
        "username",
        "password",
        "stale_stream_days",
        "priority",
        "status",
        "last_message",
    ] {
        let Some(value) = input.get(name) else {
            if name == "name" && !partial {
                errors.insert(name.into(), json!(["This field is required."]));
            }
            continue;
        };
        let result = match name {
            "name" => character(name, value, Some(255), false).map(Value::String),
            "server_url" => {
                // Preserve existing URLs. New URL changes need the full Original
                // flexible URL validator, not a narrower invented URL policy.
                if value == &old["server_url"] || value.is_null() || value.as_str() == Some("") {
                    Ok(value.clone())
                } else if create && (create_account_type == Some("XC") || remote_std_create) {
                    let text = character(name, value, None, true)?;
                    // Source flexible URL validator accepts these ordinary
                    // HTTP(S) URLs without rewriting them. Keep all other
                    // flexible forms guarded until characterized.
                    if text.len() > 65536 || !text.is_ascii() {
                        return Err(unsupported());
                    }
                    if text.is_empty() {
                        Ok(Value::String(text))
                    } else {
                        if !ordinary_provider_http_url(&text) {
                            return Err(unsupported());
                        }
                        Ok(Value::String(text))
                    }
                } else {
                    Err(unsupported())
                }
            }
            "file_path" | "username" | "password" => nullable_text(name, value, Some(255)),
            "last_message" => nullable_text(name, value, None),
            "is_active" => boolean(name, value).map(Value::Bool),
            "max_streams" | "stale_stream_days" | "priority" => positive(name, value),
            "refresh_interval" => integer(name, value).map(|n| json!(n)),
            "updated_at" => parse_datetime(name, value, true).map(|v| json!(v)),
            "server_group" | "user_agent" => {
                if value.is_null() {
                    Ok(Value::Null)
                } else {
                    crate::channel_memberships::primary_key(value).and_then(|v|v.map(|id|json!(id)).ok_or_else(unsupported)).map_err(|error|match error{StoreError::InvalidInput(Value::Object(fields))=>StoreError::InvalidInput(json!({name:fields.into_iter().next().map(|(_,value)|value).unwrap_or_else(||json!(["A valid integer is required."]))})),other=>other})
                }
            }
            "account_type" | "status" => {
                let choices: &[&str] = if name == "account_type" {
                    &["STD", "XC"]
                } else {
                    &[
                        "idle",
                        "fetching",
                        "parsing",
                        "error",
                        "success",
                        "pending_setup",
                        "disabled",
                    ]
                };
                match value.as_str() {
                    Some(v) if choices.contains(&v) => Ok(value.clone()),
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
            _ => unreachable!(),
        };
        match result {
            Ok(v) => {
                let key = match name {
                    "server_group" => "server_group_id",
                    "user_agent" => "user_agent_id",
                    v => v,
                };
                saved[key] = v;
            }
            Err(StoreError::InvalidInput(Value::Object(v))) => errors.extend(v),
            Err(e) => return Err(e),
        }
    }
    let mut cp = if create {
        Map::new()
    } else {
        properties(&old["custom_properties"])
    };
    if let Some(value) = input.get("custom_properties") {
        let mut incoming = properties(value);
        incoming.remove("hash_key");
        cp.extend(incoming);
    }
    for (name, default) in [
        ("enable_vod", false),
        ("auto_enable_new_groups_live", true),
        ("auto_enable_new_groups_vod", true),
        ("auto_enable_new_groups_series", true),
    ] {
        if let Some(value) = input.get(name) {
            match boolean(name, value) {
                Ok(v) => {
                    cp.insert(name.into(), json!(v));
                }
                Err(StoreError::InvalidInput(Value::Object(v))) => errors.extend(v),
                Err(e) => return Err(e),
            }
        } else if create {
            cp.insert(name.into(), json!(default));
        }
    }
    let previous = crate::core_settings_special::hash_fields_text(
        old["custom_properties"]
            .get("hash_key")
            .and_then(Value::as_str)
            .unwrap_or(""),
    );
    let mut changed = false;
    if let Some(value) = input.get("hash_key") {
        match hash_keys(value) {
            Ok(keys) => {
                changed = !create && crate::core_settings_special_broker::changed(&previous, &keys);
                if keys.is_empty() {
                    cp.remove("hash_key");
                } else {
                    cp.insert("hash_key".into(), json!(keys.join(",")));
                }
            }
            Err(StoreError::InvalidInput(Value::Object(v))) => errors.extend(v),
            Err(e) => return Err(e),
        }
    }
    saved["custom_properties"] = Value::Object(cp);
    let cron = match input.get("cron_expression") {
        Some(v) => character("cron_expression", v, None, true)?,
        None if partial => previous_cron.to_owned(),
        None => String::new(),
    };
    let expiration = input
        .get("exp_date")
        .map(|v| parse_datetime("exp_date", v, true))
        .transpose()?;
    if !errors.is_empty() {
        return Err(StoreError::InvalidInput(Value::Object(errors)));
    }
    // The playlist group filter saves these preferences independently of
    // group memberships, activation and the VOD refresh setting.
    let xc_preferences = !create
        && partial
        && old["account_type"] == "XC"
        && saved["account_type"] == "XC"
        && (old["custom_properties"].is_object() || old["custom_properties"].is_null())
        && !input.is_empty()
        && input.iter().all(|(key, value)| {
            value.is_boolean()
                && matches!(
                    key.as_str(),
                    "auto_enable_new_groups_live"
                        | "auto_enable_new_groups_vod"
                        | "auto_enable_new_groups_series"
                )
        });
    // The Original serializer updates live memberships even on inactive
    // accounts and with VOD enabled. This shape leaves activation, settings
    // and the existing provider state alone.
    let xc_group_selection = !create
        && partial
        && old["account_type"] == "XC"
        && saved["account_type"] == "XC"
        && (old["custom_properties"].is_object() || old["custom_properties"].is_null())
        && input.len() == 1
        && input.contains_key("channel_groups");
    let mut group_selection = Vec::new();
    if xc_group_selection {
        let groups = input["channel_groups"]
            .as_array()
            .filter(|groups| groups.len() <= 256)
            .ok_or_else(unsupported)?;
        for group in groups {
            let group = group
                .as_object()
                .filter(|group| group.len() == 2)
                .ok_or_else(unsupported)?;
            let id = group
                .get("channel_group")
                .and_then(Value::as_i64)
                .filter(|id| (1..=i64::from(i32::MAX)).contains(id))
                .ok_or_else(unsupported)?;
            let enabled = group
                .get("enabled")
                .and_then(Value::as_bool)
                .ok_or_else(unsupported)?;
            group_selection.push((id, enabled));
        }
    }
    // The live group filter echoes the existing properties when changing
    // orphan cleanup. Keep any other custom property changes separately guarded.
    let xc_orphan_cleanup = !create
        && partial
        && old["account_type"] == "XC"
        && saved["account_type"] == "XC"
        && (old["custom_properties"].is_object() || old["custom_properties"].is_null())
        && input.len() == 1
        && input
            .get("custom_properties")
            .and_then(Value::as_object)
            .is_some_and(|properties| {
                matches!(
                    properties
                        .get("orphan_channel_cleanup")
                        .and_then(Value::as_str),
                    Some("always" | "preserve_customized" | "never")
                ) && properties.iter().all(|(key, value)| {
                    key == "orphan_channel_cleanup"
                        || key == "hash_key"
                        || old["custom_properties"].get(key) == Some(value)
                })
            });
    // The playlist editor sends all its current values even for a single edit.
    let xc_editor = partial
        && old["account_type"] == "XC"
        && (old["custom_properties"].is_object() || old["custom_properties"].is_null())
        && !changed;
    let xc_label = !create
        && xc_editor
        && cron == previous_cron
        && input.get("name").is_some_and(Value::is_string)
        && xc_editor_update(&input, old, &saved, &["name"]);
    // The stock XC editor can save capacity and VOD priority together. The
    // existing account receiver propagates capacity to the default profiles.
    let limits = ["max_streams", "priority"];
    let xc_limits = !create
        && xc_editor
        && cron == previous_cron
        && limits.iter().any(|key| input.contains_key(*key))
        && limits
            .iter()
            .all(|key| input.get(*key).is_none_or(Value::is_i64))
        && xc_editor_update(&input, old, &saved, &limits);
    let interval_hours = input
        .get("refresh_interval")
        .and_then(Value::as_i64)
        .filter(|hours| (0..=i64::from(i32::MAX)).contains(hours));
    let xc_interval = !create
        && xc_editor
        && cron.is_empty()
        && previous_cron.is_empty()
        && interval_hours.is_some()
        && xc_editor_update(&input, old, &saved, &["refresh_interval"]);
    // The editor clears the interval when switching to a daily cron.
    // Switching back explicitly clears the cron and supplies the new interval.
    let xc_daily_schedule_transition = !create
        && xc_editor
        && input.get("cron_expression").is_some_and(Value::is_string)
        && xc_editor_update(&input, old, &saved, &["refresh_interval"])
        && match (previous_cron.is_empty(), cron.is_empty()) {
            (true, false) => interval_hours == Some(0) && xc_daily_cron(&cron),
            (false, true) => interval_hours.is_some() && xc_daily_cron(previous_cron),
            _ => false,
        };
    let xc_create = create && saved["account_type"] == "XC";
    if xc_create {
        for key in [
            "enable_vod",
            "auto_enable_new_groups_live",
            "auto_enable_new_groups_vod",
            "auto_enable_new_groups_series",
        ] {
            if input.get(key).is_some_and(|value| !value.is_boolean()) {
                return Err(unsupported());
            }
        }
        if input
            .get("custom_properties")
            .is_some_and(|value| !value.is_null() && !value.is_object())
        {
            return Err(unsupported());
        }
        if saved["is_active"] == false && input.get("enable_vod") == Some(&Value::Bool(true)) {
            return Err(unsupported());
        }
    }
    if !xc_preferences
        && !xc_label
        && !xc_limits
        && !xc_interval
        && !xc_daily_schedule_transition
        && !xc_active_toggle
        && !xc_create
        && !xc_group_selection
        && !xc_orphan_cleanup
        && (saved["account_type"] != "STD" || (!create && old["account_type"] != "STD"))
    {
        return Err(unsupported());
    }
    if !xc_group_selection
        && input
            .get("channel_groups")
            .is_some_and(|v| v.as_array().is_none_or(|v| !v.is_empty()))
    {
        return Err(unsupported());
    }
    if !cron.trim().is_empty() && cron.split_whitespace().count() != 5 {
        return Err(unsupported());
    }
    if !create && saved["is_active"] != old["is_active"] {
        saved["status"] = json!(if saved["is_active"] == true {
            "idle"
        } else {
            "disabled"
        });
    }
    Ok(Validated {
        saved,
        cron,
        expiration,
        hash_changed: changed,
        group_selection,
    })
}
async fn no_custom_triggers(c: &mut PgConnection) -> Result<(), StoreError> {
    let found:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE NOT tgisinternal AND tgenabled<>'D' AND tgrelid IN('m3u_m3uaccount'::regclass,'m3u_m3uaccountprofile'::regclass,'django_celery_beat_periodictask'::regclass,'django_celery_beat_periodictasks'::regclass,'django_celery_beat_intervalschedule'::regclass,'django_celery_beat_crontabschedule'::regclass,'dispatcharr_channels_channel'::regclass,'dispatcharr_channels_channelstream'::regclass,'dispatcharr_channels_stream'::regclass,'dispatcharr_channels_channelgroupm3uaccount'::regclass,'m3u_m3ufilter'::regclass,'core_systemnotification'::regclass,'core_notificationdismissal'::regclass,'accounts_user'::regclass,'dispatcharr_channels_recording'::regclass,'dispatcharr_channels_recurringrecordingrule'::regclass,'dispatcharr_channels_channelprofilemembership'::regclass,'dispatcharr_channels_channeloverride'::regclass))").fetch_one(c).await?;
    if found {
        Err(unsupported())
    } else {
        Ok(())
    }
}
async fn preflight(c: &mut PgConnection, id: Option<i64>, saved: &Value) -> Result<(), StoreError> {
    no_custom_triggers(c).await?;
    if id.is_none() && saved["account_type"] == "XC" {
        let integrations:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM dispatcharr_connect_eventsubscription s JOIN dispatcharr_connect_integration i ON i.id=s.integration_id WHERE s.enabled AND i.enabled AND s.event IN('m3u_refresh','m3u_error')) OR EXISTS(SELECT 1 FROM plugins_pluginconfig WHERE enabled)").fetch_one(&mut*c).await?;
        if integrations {
            return Err(unsupported());
        }
    }
    let collision:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM m3u_m3uaccount WHERE name=$1 AND ($2::bigint IS NULL OR id<>$2))").bind(saved["name"].as_str()).bind(id).fetch_one(&mut*c).await?;
    if collision {
        return Err(field("name", "m3u account with this name already exists."));
    }
    for (name, table) in [
        ("server_group", "m3u_servergroup"),
        ("user_agent", "core_useragent"),
    ] {
        if let Some(value) = saved[format!("{name}_id")].as_i64() {
            let query = format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE id=$1)");
            let exists: bool = sqlx::query_scalar(&query)
                .bind(value)
                .fetch_one(&mut *c)
                .await?;
            if !exists {
                return Err(field(
                    name,
                    &format!("Invalid pk \"{value}\" - object does not exist."),
                ));
            }
        }
    }
    Ok(())
}
fn validate_group_references(group_ids: &[i64], existing: &[i64]) -> Result<(), StoreError> {
    let existing: HashSet<_> = existing.iter().collect();
    // DRF 3.18 keeps only failing nested rows, keyed by their input index.
    let errors: Map<String, Value> = group_ids
        .iter()
        .enumerate()
        .filter(|(_, group)| !existing.contains(group))
        .map(|(index, group)| {
            (
                index.to_string(),
                json!({"channel_group":[format!("Invalid pk \"{group}\" - object does not exist.")]}),
            )
        })
        .collect();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(StoreError::InvalidInput(json!({"channel_groups": errors})))
    }
}

fn keep_first_group_selections(selection: &mut Vec<(i64, bool)>) {
    // Source bulk_update emits one ordered CASE per batch. The first WHEN for
    // a membership wins. Retain all submitted indices until validation ends.
    let mut seen = HashSet::new();
    selection.retain(|(id, _)| seen.insert(*id));
}

async fn write(c: &mut PgConnection, id: Option<i64>, saved: Value) -> Result<i64, StoreError> {
    let updated = parse_datetime("updated_at", &saved["updated_at"], true)?;
    // Whitelisted explicit columns preserve model defaults, readonly creation
    // time and the existing update timestamp. No jsonb record default loss.
    let query = if id.is_some() {
        "UPDATE m3u_m3uaccount SET name=$2,server_url=$3,file_path=$4,server_group_id=$5,max_streams=$6,is_active=$7,updated_at=$8,status=$9,last_message=$10,user_agent_id=$11,account_type=$12,username=$13,password=$14,custom_properties=$15,refresh_interval=$16,stale_stream_days=$17,priority=$18 WHERE id=$1 RETURNING id::bigint"
    } else {
        "INSERT INTO m3u_m3uaccount(id,name,server_url,file_path,server_group_id,max_streams,is_active,updated_at,status,last_message,user_agent_id,account_type,username,password,custom_properties,refresh_interval,stale_stream_days,priority,created_at,locked,stream_profile_id,refresh_task_id) VALUES(COALESCE($1,nextval('m3u_m3uaccount_id_seq')),$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,false,NULL,NULL) RETURNING id::bigint"
    };
    let mut q = sqlx::query_scalar(query)
        .bind(id)
        .bind(saved["name"].as_str())
        .bind(saved["server_url"].as_str())
        .bind(saved["file_path"].as_str())
        .bind(saved["server_group_id"].as_i64())
        .bind(saved["max_streams"].as_i64().ok_or_else(unsupported)? as i32)
        .bind(saved["is_active"].as_bool())
        .bind(updated)
        .bind(saved["status"].as_str())
        .bind(saved["last_message"].as_str())
        .bind(saved["user_agent_id"].as_i64())
        .bind(saved["account_type"].as_str())
        .bind(saved["username"].as_str())
        .bind(saved["password"].as_str())
        .bind(sqlx::types::Json(saved["custom_properties"].clone()))
        .bind(saved["refresh_interval"].as_i64().ok_or_else(unsupported)? as i32)
        .bind(
            saved["stale_stream_days"]
                .as_i64()
                .ok_or_else(unsupported)? as i32,
        )
        .bind(saved["priority"].as_i64().ok_or_else(unsupported)? as i32);
    if id.is_none() {
        q = q.bind(chrono::Utc::now());
    }
    q.fetch_one(c).await.map_err(StoreError::from)
}
struct TargetLock {
    account: i64,
    token: String,
}
impl TargetLock {
    async fn acquire(auth: &AuthService, account: i64) -> Result<Self, StoreError> {
        let cache = auth.notification_cache.as_ref().ok_or_else(unsupported)?;
        let token = uuid::Uuid::new_v4().simple().to_string();
        let mut cmd = redis::cmd("SET");
        cmd.arg(format!("task_lock_refresh_single_m3u_account_{account}"))
            .arg(&token)
            .arg("EX")
            .arg(7200)
            .arg("NX");
        match cache.command_settled::<Option<String>>(cmd).await? {
            Some(v) if v == "OK" => Ok(Self { account, token }),
            None => Err(StoreError::StatusBody(
                409,
                json!({"detail":"Cannot change the hash key override while this account is refreshing. Wait for the refresh to finish, then try again.","hash_key":["Account is currently refreshing. Try again later."]}),
            )),
            _ => Err(unsupported()),
        }
    }
    async fn release(&self, auth: &AuthService) -> Result<(), StoreError> {
        let cache = auth.notification_cache.as_ref().ok_or_else(unsupported)?;
        let mut cmd = redis::cmd("EVAL");
        cmd.arg("if redis.call('get',KEYS[1])==ARGV[1] then return redis.call('del',KEYS[1]) end return 0").arg(1).arg(format!("task_lock_refresh_single_m3u_account_{}",self.account)).arg(&self.token);
        let _: i64 = cache.command_settled(cmd).await?;
        Ok(())
    }
}
async fn update_default_capacity(c: &mut PgConnection, account: i64, max: i32) -> Result<(), StoreError> {
    sqlx::query(
        "UPDATE m3u_m3uaccountprofile SET max_streams=$2 WHERE m3u_account_id=$1 AND is_default AND max_streams<>$2",
    )
    .bind(account)
    .bind(max)
    .execute(c)
    .await?;
    Ok(())
}

async fn default_profile(
    session: &TaskSqlSession,
    auth: &AuthService,
    id: i64,
    saved: &Value,
    create: bool,
    expiration: Option<Option<chrono::DateTime<chrono::Utc>>>,
    owner: Owner,
) -> Result<(), StoreError> {
    let signal = auth.profile_save_signal.clone().ok_or_else(unsupported)?;
    let name = saved["name"].as_str().ok_or_else(unsupported)?.to_owned();
    let max = saved["max_streams"].as_i64().ok_or_else(unsupported)? as i32;
    if create {
        let profile_name = format!("{name} Default");
        let profile:i64=session.run(move|c|Box::pin(async move{sqlx::query_scalar("INSERT INTO m3u_m3uaccountprofile(m3u_account_id,name,max_streams,is_active,is_default,current_viewers,search_pattern,replace_pattern,custom_properties,exp_date) VALUES($1,$2,$3,true,true,0,'^(.*)$','$1','{}',NULL) RETURNING id::bigint").bind(id).bind(profile_name).bind(max).fetch_one(c).await})).await??;
        signal
            .guard(session.clone(), profile, owner.clone())
            .await?;
        signal
            .after_save(
                session.clone(),
                SavedProfile {
                    id: profile,
                    account_id: id,
                    name: format!("{name} Default"),
                    account_name: name.clone(),
                    expiration: None,
                },
                owner.clone(),
            )
            .await?;
    } else {
        // QuerySet.update: no profile save signal for max_streams propagation.
        session
            .run(move |c| Box::pin(update_default_capacity(c, id, max)))
            .await??;
    }
    if let Some(expiration) = expiration.filter(|v| !create || v.is_some()) {
        let profile:Option<(i64,String)>=session.run(move|c|Box::pin(async move{sqlx::query_as("SELECT id::bigint,name FROM m3u_m3uaccountprofile WHERE m3u_account_id=$1 AND is_default ORDER BY id LIMIT 1").bind(id).fetch_optional(c).await})).await??;
        if let Some((profile, profile_name)) = profile {
            signal
                .guard(session.clone(), profile, owner.clone())
                .await?;
            session
                .run(move |c| {
                    Box::pin(async move {
                        sqlx::query("UPDATE m3u_m3uaccountprofile SET exp_date=$2 WHERE id=$1")
                            .bind(profile)
                            .bind(expiration)
                            .execute(c)
                            .await
                            .map(|_| ())
                    })
                })
                .await??;
            signal
                .after_save(
                    session.clone(),
                    SavedProfile {
                        id: profile,
                        account_id: id,
                        name: profile_name,
                        account_name: name,
                        expiration,
                    },
                    owner,
                )
                .await?;
        }
    }
    Ok(())
}
// Original model post_save receiver sequence followed by serializer.create's
// explicit expiration write. This is shared preparation, not XC admission.
struct SavedAccountEffects<'a> {
    account: i64,
    saved: &'a Value,
    cron: &'a str,
    timezone: &'a str,
    create: bool,
    expiration: Option<Option<chrono::DateTime<chrono::Utc>>>,
    group_selection: &'a [(i64, bool)],
}
async fn save_receivers(
    session: &TaskSqlSession,
    auth: &AuthService,
    effect: SavedAccountEffects<'_>,
    owner: Owner,
) -> Result<Scratch, StoreError> {
    let SavedAccountEffects {
        account,
        saved,
        cron,
        timezone,
        create,
        expiration,
        group_selection,
    } = effect;
    default_profile(session, auth, account, saved, create, None, owner.clone()).await?;
    let scratch = Scratch::new();
    // apps/m3u/signals.py:19: XC runs Groups directly in the create view,
    // after super().create has frozen response.data and playlist_created sent.
    // Never publish a synthetic Groups root for XC from this model receiver.
    if create && saved["account_type"] != "XC" {
        crate::core_settings_special_broker::publish(
            auth,
            &scratch,
            "apps.m3u.tasks.refresh_m3u_groups",
            json!([account]),
            json!({}),
        )
        .await?;
    }
    let task_name = format!("m3u_account-refresh-{account}");
    let kwargs = format!("{{\"account_id\": {account}}}");
    let schedule_cron = cron.to_owned();
    let zone = timezone.to_owned();
    let interval = saved["refresh_interval"].as_i64().ok_or_else(unsupported)? as i32;
    let active = saved["is_active"].as_bool().ok_or_else(unsupported)?;
    let periodic = session
        .run(move |c| {
            Box::pin(async move {
                crate::provider_schedule::upsert(
                    c,
                    crate::provider_schedule::Spec {
                        name: &task_name,
                        task: "apps.m3u.tasks.refresh_single_m3u_account",
                        kwargs: &kwargs,
                        interval_hours: interval,
                        cron: &schedule_cron,
                        active,
                        timezone: &zone,
                    },
                )
                .await
            })
        })
        .await??;
    let periodic = i32::try_from(periodic).map_err(|_| unsupported())?;
    session.run(move|c|Box::pin(async move{sqlx::query("UPDATE m3u_m3uaccount SET refresh_task_id=$2 WHERE id=$1 AND refresh_task_id IS DISTINCT FROM $2").bind(account).bind(periodic).execute(c).await.map(|_|())})).await??;
    if !group_selection.is_empty() {
        let selected = serde_json::to_value(group_selection).map_err(|_| unsupported())?;
        // Serializer.update bulk_update changes only enabled; missing account
        // memberships are ignored, no membership rows or signals are created.
        session.run(move|c|Box::pin(async move {
            sqlx::query("UPDATE dispatcharr_channels_channelgroupm3uaccount m SET enabled=(v.pair->>1)::boolean FROM jsonb_array_elements($2::jsonb) AS v(pair) WHERE m.m3u_account_id=$1 AND m.channel_group_id=(v.pair->>0)::bigint").bind(account).bind(sqlx::types::Json(selected)).execute(c).await.map(|_|())
        })).await??;
    }
    if expiration.is_some_and(|v| !create || v.is_some()) {
        default_profile(session, auth, account, saved, false, expiration, owner).await?;
    }
    Ok(scratch)
}

async fn project(
    session: &TaskSqlSession,
    id: i64,
    cron: String,
) -> Result<Json<Value>, StoreError> {
    let value = session
        .run(move |c| {
            Box::pin(async move { crate::provider_reads::physical_account_detail(c, id).await })
        })
        .await??;
    let mut value = value;
    value["cron_expression"] = json!(cron);
    Ok(Json(value))
}
fn ordinary_name_only(input: &Map<String, Value>) -> bool {
    input.len() == 1 && input.get("name").is_some_and(Value::is_string)
}

fn existing_std_interval_account(old: &Value, previous_cron: &str) -> bool {
    let eligible_activity = old["is_active"] == false
        || (old["is_active"] == true
            && old["status"] == "idle"
            && old["refresh_interval"].as_i64().is_some_and(|hours| hours > 0));
    old["account_type"] == "STD"
        && eligible_activity
        && old["locked"] == false
        && old["refresh_task_id"].as_i64().is_some_and(|id| id > 0)
        && previous_cron.is_empty()
}

async fn rename_without_broker(
    session: &TaskSqlSession,
    id: i64,
    old: &Value,
    validated: Validated,
) -> Result<Json<Value>, StoreError> {
    let mut expected = old.clone();
    expected["name"] = validated.saved["name"].clone();
    if validated.saved != expected
        || validated.hash_changed
        || validated.expiration.is_some()
        || !validated.group_selection.is_empty()
        || !validated.cron.is_empty()
    {
        return Err(unsupported());
    }
    let name = validated.saved["name"]
        .as_str()
        .ok_or_else(unsupported)?
        .to_owned();
    let task = old["refresh_task_id"].as_i64().ok_or_else(unsupported)?;
    let active = old["is_active"].as_bool().ok_or_else(unsupported)?;
    let interval = old["refresh_interval"]
        .as_i64()
        .and_then(|value| i32::try_from(value).ok())
        .ok_or_else(unsupported)?;
    let max = old["max_streams"]
        .as_i64()
        .and_then(|value| i32::try_from(value).ok())
        .ok_or_else(unsupported)?;
    session
        .run(move |c| {
            Box::pin(async move {
                // Lock the same account/schedule while preserving all unrelated
                // columns and the ordinary periodic-row save effects.
                let mut tx = c.begin().await?;
                let account: Option<i64> = sqlx::query_scalar(
                    "UPDATE m3u_m3uaccount SET name=$2 WHERE id=$1 AND account_type='STD' \
                     AND NOT locked AND is_active=$6 AND (NOT $6 OR status='idle') \
                     AND refresh_interval=$3 AND refresh_task_id=$4 AND max_streams=$5 RETURNING id::bigint",
                )
                .bind(id)
                .bind(name)
                .bind(interval)
                .bind(task)
                .bind(max)
                .bind(active)
                .fetch_optional(&mut *tx)
                .await?;
                let account = account.ok_or_else(unsupported)?;
                let task_name = format!("m3u_account-refresh-{id}");
                let present: Option<i64> = sqlx::query_scalar(
                    "SELECT t.id::bigint FROM django_celery_beat_periodictask t \
                     JOIN django_celery_beat_intervalschedule i ON i.id=t.interval_id \
                     WHERE t.id=$1 AND t.name=$2 AND t.task=$3 AND t.enabled=$5 \
                     AND i.every=$4 AND i.period='hours' FOR UPDATE OF t",
                )
                .bind(task)
                .bind(&task_name)
                .bind("apps.m3u.tasks.refresh_single_m3u_account")
                .bind(interval.max(1))
                .bind(active)
                .fetch_optional(&mut *tx)
                .await?;
                if present.is_none() {
                    return Err(unsupported());
                }
                update_default_capacity(&mut tx, account, max).await?;
                let kwargs = format!("{{\"account_id\": {id}}}");
                let scheduled = crate::provider_schedule::upsert(
                    &mut tx,
                    crate::provider_schedule::Spec {
                        name: &task_name,
                        task: "apps.m3u.tasks.refresh_single_m3u_account",
                        kwargs: &kwargs,
                        interval_hours: interval,
                        cron: "",
                        active,
                        timezone: "UTC",
                    },
                )
                .await?;
                if scheduled != task {
                    return Err(unsupported());
                }
                let mut value = crate::provider_reads::physical_account_detail(&mut tx, id).await?;
                value["cron_expression"] = json!("");
                tx.commit().await?;
                Ok(Json(value))
            })
        })
        .await?
}

async fn change(
    auth: AuthService,
    peer: SocketAddr,
    headers: HeaderMap,
    raw: Option<String>,
    body: Body,
    partial: bool,
) -> Result<(StatusCode, Json<Value>), StoreError> {
    auth.account_user(peer, headers, true).await?;
    let id = raw.map(path_key).transpose()?;
    let input = body_object(body)?;
    let broker_missing = auth.core_task_broker.get().is_none();
    let queue_free_name = broker_missing && partial && id.is_some() && ordinary_name_only(&input);
    let direct_xc = id.is_none() && input.get("account_type").and_then(Value::as_str) == Some("XC");
    let direct_vod = input.get("enable_vod") == Some(&Value::Bool(true));
    let inline_groups = if direct_xc {
        let _bounded = crate::provider_reads::provider_reply(&input)?;
        Some(auth.inline_xc_groups_service()?)
    } else {
        None
    };
    // Bind the SAME admitted writer to cache/broker retained physical owners,
    // not only the SQL actor. Lost replies must not release its budget early.
    let writer = auth.task_sql_writer()?;
    let auth = auth.with_request_writer_permit(writer)?;
    // Resolve Source cache-authoritative global hash before owning a physical
    // SQL session. No second pool checkout behind the held actor query gate.
    let global = if id.is_some()
        && input
            .get("hash_key")
            .is_some_and(|v| v.is_null() || v.as_str().is_some_and(|v| v.trim().is_empty()))
    {
        crate::core_settings_special_effects::load_group(
            &auth,
            "stream_settings",
            json!({"m3u_hash_key":"name,url,tvg_id"}),
        )
        .await?
    } else {
        Value::Null
    };
    if auth.profile_save_signal.is_none()
        || auth.event_sink.is_none()
        || (broker_missing && !queue_free_name)
    {
        return Err(unsupported());
    }
    let owner: Owner = Arc::new(auth.clone());
    let session = auth.borrow_task_sql_session(owner.clone()).await?;
    let operation = tokio::spawn(async move {
        let mut lock = None;
        let mut handed_off = false;
        let outcome=async{
            let (old,old_cron)=if let Some(id)=id{
                let row=session.run(move|c|Box::pin(async move{sqlx::query("SELECT CASE WHEN octet_length(to_jsonb(a)::text)<=1048576 THEN to_jsonb(a) END AS raw,COALESCE(concat_ws(' ',ct.minute,ct.hour,ct.day_of_month,ct.month_of_year,ct.day_of_week),'') AS cron FROM m3u_m3uaccount a LEFT JOIN django_celery_beat_periodictask t ON t.id=a.refresh_task_id LEFT JOIN django_celery_beat_crontabschedule ct ON ct.id=t.crontab_id WHERE a.id=$1").bind(id).fetch_optional(c).await})).await??.ok_or_else(|| if queue_free_name { unsupported() } else { missing() })?;
                let raw:Option<Value>=row.try_get("raw")?;let old=raw.ok_or_else(unsupported)?;
                let cron:String=row.try_get("cron")?;
                if queue_free_name && !existing_std_interval_account(&old, &cron) { return Err(unsupported()); }
                if old["locked"]==true{return Err(locked())}
                (old,cron)
            }else{(defaults(),String::new())};
            let mut validated=validate(input,&old,&old_cron,partial,id.is_none())?;
            let check=validated.saved.clone();let group_ids:Vec<i64>=validated.group_selection.iter().map(|(id,_)|*id).collect();
            session.run(move|c|Box::pin(async move{
                preflight(c,id,&check).await?;
                if !group_ids.is_empty() {
                    let found:Vec<i64>=sqlx::query_scalar("SELECT id::bigint FROM dispatcharr_channels_channelgroup WHERE id::bigint=ANY($1)").bind(&group_ids).fetch_all(c).await?;
                    validate_group_references(&group_ids,&found)?;
                }
                Ok::<_,StoreError>(())
            })).await??;
            if queue_free_name {
                let account = id.ok_or_else(unsupported)?;
                let response = rename_without_broker(&session, account, &old, validated).await?;
                return Ok((account, None, (StatusCode::OK, response)));
            }
            let saved=validated.saved;let cron=validated.cron;
            keep_first_group_selections(&mut validated.group_selection);
            let inline_admission=inline_groups.as_ref().map(|groups|groups.admit(&auth)).transpose()?;
            if validated.hash_changed{lock=Some(TargetLock::acquire(&auth,id.ok_or_else(unsupported)?).await?);}
            let zone=if !cron.trim().is_empty(){let settings=session.system_settings(auth.clone()).await?;settings["time_zone"].as_str().filter(|v|!v.is_empty()).unwrap_or("UTC").to_owned()}else{"UTC".into()};
            if id.is_some()&&old["is_active"]==true&&saved["is_active"]==false{
                auth.profile_save_signal.as_ref().ok_or_else(unsupported)?.deactivate_account(session.clone(),id.ok_or_else(unsupported)?,owner.clone()).await?;
            }
            let write_saved=saved.clone();let account=session.run(move|c|Box::pin(async move{write(c,id,write_saved).await})).await??;
            let scratch=save_receivers(&session,&auth,SavedAccountEffects{account,saved:&saved,cron:&cron,timezone:&zone,create:id.is_none(),expiration:validated.expiration,group_selection:&validated.group_selection},owner.clone()).await?;
            if let Some(owned)=&lock{
                let override_keys=crate::core_settings_special::hash_fields_text(saved["custom_properties"].get("hash_key").and_then(Value::as_str).unwrap_or(""));
                let keys=if override_keys.is_empty(){crate::core_settings_special::hash_fields_text(global["m3u_hash_key"].as_str().unwrap_or("name,url,tvg_id"))}else{override_keys};
                crate::core_settings_special_broker::publish(&auth,&scratch,"core.tasks.rehash_streams",json!([keys]),json!({"account_id":account,"locks_preacquired":true,"preacquired_lock_ids":[account],"preacquired_lock_token":owned.token})).await?;handed_off=true;
            }
            let response=if direct_xc {
                let mut response=project(&session,account,cron.clone()).await?;
                // Serializer201 still owns input properties; PostgreSQL jsonb
                // order belongs only to later queries, never this response.
                response.0["custom_properties"]=saved["custom_properties"].clone();
                Some(response)
            }else{None};
            if id.is_none(){crate::core_task_offsets::retained_event(auth.event_sink.clone().ok_or_else(unsupported)?,json!({"type":"update","data":{"type":"playlist_created","playlist_id":account}}),owner.clone()).await?;}
            Ok((account,inline_admission,(if id.is_none(){StatusCode::CREATED}else{StatusCode::OK},match response {Some(response)=>response,None=>project(&session,account,cron).await?})))
        }.await;
        let released = if !handed_off {
            if let Some(owned) = lock {
                owned.release(&auth).await
            } else {
                Ok(())
            }
        } else {
            Ok(())
        };
        let closed = session.close().await;
        let value = outcome?;
        released?;
        closed?;
        let (account, inline_admission, response) = value;
        if let Some(inline_admission) = inline_admission {
            inline_admission
                .run(auth.clone(), account, direct_vod, owner.clone())
                .await?;
        }
        Ok::<_, StoreError>(response)
    });
    operation.await.map_err(|_| {
        StoreError::StatusBody(
            503,
            json!({"detail":"Account lifecycle supervisor failed."}),
        )
    })?
}
pub(crate) async fn create(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    match change(auth, peer, headers, None, body, false).await {
        Ok((status, value)) if value.0["account_type"] == "XC" => {
            match crate::provider_reads::xc_account_reply(&value.0, true) {
                Ok(response) => (status, response).into_response(),
                Err(error) => error.into_response(),
            }
        }
        Ok(reply) => reply.into_response(),
        Err(error) => crate::provider_reads::provider_validation_error(error),
    }
}
pub(crate) async fn put(
    State(auth): State<AuthService>,
    Path(id): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Result<(StatusCode, Json<Value>), StoreError> {
    change(auth, peer, headers, Some(id), body, false).await
}
pub(crate) async fn patch(
    State(auth): State<AuthService>,
    Path(id): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    match change(auth, peer, headers, Some(id), body, true).await {
        Ok((status, value)) if value.0["account_type"] == "XC" => {
            match crate::provider_reads::xc_account_reply(&value.0, false) {
                Ok(response) => (status, response).into_response(),
                Err(error) => error.into_response(),
            }
        }
        Ok(reply) => reply.into_response(),
        Err(error) => crate::provider_reads::provider_validation_error(error),
    }
}

/// Source account Collector executes inside the already-open channel atomic.
/// No second transaction, pool, file deletion, or API replay is introduced.
pub(crate) async fn delete_in_collector(
    c: &mut PgConnection,
    auth: AuthService,
    id: i64,
    owner: Owner,
) -> Result<(), StoreError> {
    if c.protocol_transaction_is_idle().await? {
        return Err(unsupported());
    }
    let delete_vod_categories = delete_preflight(c, id).await?;
    // The Source Collector bulk-deletes profiles, then sends their post_delete
    // receivers in reverse primary-key order. Preserve that boundary rather
    // than invoking standalone delete (which opens an inner transaction).
    let profiles:Vec<i64>=sqlx::query_scalar("SELECT id::bigint FROM m3u_m3uaccountprofile WHERE m3u_account_id=$1 ORDER BY id DESC LIMIT 65").bind(id).fetch_all(&mut*c).await?;
    if profiles.len() > 64 {
        return Err(unsupported());
    }
    sqlx::query("CREATE TEMP TABLE native_account_stream_channels ON COMMIT DROP AS SELECT DISTINCT cs.channel_id FROM dispatcharr_channels_channelstream cs JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id WHERE s.m3u_account_id=$1").bind(id).execute(&mut*c).await?;
    sqlx::query("DELETE FROM dispatcharr_channels_channelstream WHERE stream_id IN(SELECT id FROM dispatcharr_channels_stream WHERE m3u_account_id=$1)").bind(id).execute(&mut*c).await?;
    sqlx::query("DELETE FROM dispatcharr_channels_stream WHERE m3u_account_id=$1")
        .bind(id)
        .execute(&mut *c)
        .await?;
    // ChannelStream.post_delete rollup touches surviving manual channels too.
    sqlx::query("UPDATE dispatcharr_channels_channel c SET is_catchup=EXISTS(SELECT 1 FROM dispatcharr_channels_channelstream cs JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id JOIN m3u_m3uaccount a ON a.id=s.m3u_account_id WHERE cs.channel_id=c.id AND s.is_catchup AND a.is_active),catchup_days=COALESCE((SELECT max(s.catchup_days) FROM dispatcharr_channels_channelstream cs JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id JOIN m3u_m3uaccount a ON a.id=s.m3u_account_id WHERE cs.channel_id=c.id AND s.is_catchup AND a.is_active),0) WHERE c.id IN(SELECT channel_id FROM native_account_stream_channels)").execute(&mut*c).await?;
    sqlx::query("UPDATE dispatcharr_channels_channel SET auto_created_by_id=NULL WHERE auto_created_by_id=$1").bind(id).execute(&mut*c).await?;
    sqlx::query("DELETE FROM dispatcharr_channels_channelgroupm3uaccount WHERE m3u_account_id=$1")
        .bind(id)
        .execute(&mut *c)
        .await?;
    sqlx::query("DELETE FROM m3u_m3ufilter WHERE m3u_account_id=$1")
        .bind(id)
        .execute(&mut *c)
        .await?;
    sqlx::query("DELETE FROM m3u_m3uaccountprofile WHERE m3u_account_id=$1")
        .bind(id)
        .execute(&mut *c)
        .await?;
    let signal = auth.profile_save_signal.as_ref().ok_or_else(unsupported)?;
    for profile in profiles {
        signal
            .after_profile_delete_in_collector(c, id, profile, owner.clone())
            .await?;
    }
    // Ordinary category-only XC Collector: preserve global/shared/orphan
    // categories and every other account's memberships exactly as Source does.
    if delete_vod_categories {
        sqlx::query("DELETE FROM vod_m3uvodcategoryrelation WHERE m3u_account_id=$1")
            .bind(id)
            .execute(&mut *c)
            .await?;
    }
    sqlx::query("DELETE FROM m3u_m3uaccount WHERE id=$1")
        .bind(id)
        .execute(&mut *c)
        .await?;
    crate::provider_schedule::legacy_delete(c, &format!("m3u_account-refresh-{id}")).await?;
    Ok(())
}
async fn delete_preflight(c: &mut PgConnection, id: i64) -> Result<bool, StoreError> {
    no_custom_triggers(c).await?;
    let account: Option<(bool, String)> =
        sqlx::query_as("SELECT locked,account_type FROM m3u_m3uaccount WHERE id=$1")
            .bind(id)
            .fetch_optional(&mut *c)
            .await?;
    let Some((is_locked, kind)) = account else {
        return Err(missing());
    };
    if is_locked {
        return Err(locked());
    }
    if !matches!(kind.as_str(), "STD" | "XC") {
        return Err(unsupported());
    }
    // VOD orphan cleanup is a different Source Collector/signal domain; do not
    // silently delete its relation rows or leave orphan movies/series behind.
    // Actual SourceXC11 delete removes only this account's category relations;
    // even newly orphaned global VODCategory rows survive. Content collectors
    // remain guarded separately; do not infer their cleanup from categories.
    let xc = kind == "XC";
    if xc {
        let custom_triggers: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE NOT tgisinternal AND tgenabled<>'D' AND tgrelid='vod_m3uvodcategoryrelation'::regclass)",
        )
        .fetch_one(&mut *c)
        .await?;
        if custom_triggers {
            return Err(unsupported());
        }
    }
    let vod:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM vod_m3umovierelation WHERE m3u_account_id=$1 UNION ALL SELECT 1 FROM vod_m3useriesrelation WHERE m3u_account_id=$1 UNION ALL SELECT 1 FROM vod_m3uepisoderelation WHERE m3u_account_id=$1) OR (NOT $2 AND EXISTS(SELECT 1 FROM vod_m3uvodcategoryrelation WHERE m3u_account_id=$1))").bind(id).bind(xc).fetch_one(&mut*c).await?;
    if vod {
        return Err(unsupported());
    }
    let profiles: i64 =
        sqlx::query_scalar("SELECT count(*) FROM m3u_m3uaccountprofile WHERE m3u_account_id=$1")
            .bind(id)
            .fetch_one(&mut *c)
            .await?;
    if profiles > 64 {
        return Err(unsupported());
    }
    // Preflight receiver capacity before any channel stop or Collector write.
    // Callback guards repeat at their actual receiver boundary for drift.
    let(users,maximum):(i64,i32)=sqlx::query_as("SELECT count(*),COALESCE(max(octet_length(custom_properties::text)),0) FROM accounts_user WHERE custom_properties ? 'allowed_m3u_profile_ids'").fetch_one(&mut*c).await?;
    if users > 64 || maximum > 65536 {
        return Err(unsupported());
    }
    let notices:i64=sqlx::query_scalar("SELECT count(*) FROM core_systemnotification n WHERE EXISTS(SELECT 1 FROM m3u_m3uaccountprofile p WHERE p.m3u_account_id=$1 AND n.notification_key IN('xc-exp-warning-'||p.id,'xc-exp-expired-'||p.id))").bind(id).fetch_one(&mut*c).await?;
    let dismissals:i64=sqlx::query_scalar("SELECT count(*) FROM core_notificationdismissal d JOIN core_systemnotification n ON n.id=d.notification_id WHERE EXISTS(SELECT 1 FROM m3u_m3uaccountprofile p WHERE p.m3u_account_id=$1 AND n.notification_key IN('xc-exp-warning-'||p.id,'xc-exp-expired-'||p.id))").bind(id).fetch_one(&mut*c).await?;
    if notices > 128 || dismissals > 1024 {
        return Err(unsupported());
    }
    Ok(xc)
}
pub(crate) async fn delete(
    State(auth): State<AuthService>,
    Path(raw): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Json<Value>, StoreError> {
    auth.account_user(peer, headers, true).await?;
    let id = path_key(raw)?;
    let writer = auth.task_sql_writer()?;
    let auth = auth.with_request_writer_permit(writer)?;
    if auth.profile_save_signal.is_none() {
        return Err(unsupported());
    }
    let jobs = auth.recording_collector_jobs()?.ok_or_else(unsupported)?;
    let owner: Owner = Arc::new(auth.clone());
    let session = auth.borrow_task_sql_session(owner.clone()).await?;
    let operation = tokio::spawn(async move {
        let outcome=async{
            session.run(move|c|Box::pin(async move{delete_preflight(c,id).await})).await??;
            let mut selection=auth.prepare_task_channel_deletion(session.clone(),jobs,owner).await?;
            // Bounded keyset pages populate ONE SQL-backed selection; stop all
            // captured UUIDs outside the atomic, then perform one Collector.
            let mut after=0_i64;
            loop{
                let ids:Vec<i64>=session.run(move|c|Box::pin(async move{sqlx::query_scalar("SELECT id::bigint FROM dispatcharr_channels_channel WHERE auto_created AND auto_created_by_id=$1 AND id>$2 ORDER BY id LIMIT 500").bind(id).bind(after).fetch_all(c).await})).await??;
                if ids.is_empty(){break}after = *ids.last().ok_or_else(unsupported)?;selection.append(ids).await?;
            }
            let report=selection.execute_with_account(auth,id).await?;
            if report.effect_outcome.is_some(){return Err(StoreError::StatusBody(503,json!({"detail":"Account Collector side effects did not settle successfully."})))}
            Ok(Json(json!({"deleted_channels":report.deleted})))
        }.await;
        let closed = session.close().await;
        let value = outcome?;
        closed?;
        Ok::<_, StoreError>(value)
    });
    operation.await.map_err(|_| {
        StoreError::StatusBody(503, json!({"detail":"Account delete supervisor failed."}))
    })?
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn account_hash_clear_merges_properties_and_typed_preferences() {
        let mut old = defaults();
        old["name"] = json!("Existing");
        old["custom_properties"] =
            json!({"hash_key":"name,url","keep":1,"auto_enable_new_groups_live":false});
        let v=validate(json!({"hash_key":null,"custom_properties":{"revision":2,"hash_key":"tvg_id"},"auto_enable_new_groups_live":true}).as_object().unwrap().clone(),&old,"17 3 * * *",true,false).unwrap();
        assert!(v.hash_changed);
        assert_eq!(v.cron, "17 3 * * *");
        assert_eq!(
            v.saved["custom_properties"],
            json!({"keep":1,"revision":2,"auto_enable_new_groups_live":true})
        );
        let put = validate(
            json!({"name":"Existing"}).as_object().unwrap().clone(),
            &old,
            "17 3 * * *",
            false,
            false,
        )
        .unwrap();
        assert_eq!(put.cron, "");
    }
    #[test]
    fn flags_off_name_edit_preserves_active_and_inactive_std_account_state() {
        let mut old = defaults();
        old["name"] = json!("Owned rehash provider target");
        old["status"] = json!("idle");
        old["refresh_task_id"] = json!(7);
        old["refresh_interval"] = json!(4);
        old["custom_properties"] = json!({"hash_key":"name,url","owned_fixture_marker":"preserve"});
        let input = Map::from_iter([("name".into(), json!("Owned renamed standard provider"))]);
        assert!(ordinary_name_only(&input));
        for active in [false, true] {
            old["is_active"] = json!(active);
            assert!(existing_std_interval_account(&old, ""));
            let validated = validate(input.clone(), &old, "", true, false).unwrap();
            let mut expected = old.clone();
            expected["name"] = json!("Owned renamed standard provider");
            assert_eq!(validated.saved, expected);
            assert!(!validated.hash_changed && validated.expiration.is_none());
            assert!(validated.cron.is_empty() && validated.group_selection.is_empty());
        }
        let extra = Map::from_iter([("name".into(), json!("Owned name")), ("hash_key".into(), json!("name"))]);
        assert!(!ordinary_name_only(&extra));
        assert!(!existing_std_interval_account(&old, "0 7 * * *"));
        old["refresh_interval"] = json!(0);
        assert!(!existing_std_interval_account(&old, ""));
        old["refresh_interval"] = json!(4);
        old["status"] = json!("fetching");
        assert!(!existing_std_interval_account(&old, ""));
    }

    #[test]
    fn account_hash_order_only_has_no_rehash_lock_or_job() {
        let mut old = defaults();
        old["custom_properties"] = json!({"hash_key":"name,url"});
        let v = validate(
            json!({"hash_key":"url,name,url"})
                .as_object()
                .unwrap()
                .clone(),
            &old,
            "",
            true,
            false,
        )
        .unwrap();
        assert!(!v.hash_changed);
        assert_eq!(v.saved["custom_properties"]["hash_key"], "url,name");
    }
    #[test]
    fn positive_source_create_is_inactive_but_keeps_initial_idle_status() {
        let v=validate(json!({"name":"Owned","is_active":false,"max_streams":2,"hash_key":"name,url","auto_enable_new_groups_live":false}).as_object().unwrap().clone(),&defaults(),"",false,true).unwrap();
        assert_eq!(v.saved["status"], "idle");
        assert_eq!(v.saved["custom_properties"]["enable_vod"], false);
        assert_eq!(
            v.saved["custom_properties"]["auto_enable_new_groups_vod"],
            true
        );
    }
    #[test]
    fn remote_std_create_admits_active_and_inactive_live_provider_urls() {
        let url = "http://127.0.0.1:30000/owned.m3u";
        for active in [false, true] {
            let input = json!({
                "name": "Owned",
                "server_url": url,
                "account_type": "STD",
                "is_active": active,
                "refresh_interval": 0
            });
            let outcome = validate(
                input.as_object().unwrap().clone(),
                &defaults(),
                "",
                false,
                true,
            )
            .unwrap();
            assert_eq!(outcome.saved["server_url"], url);
            assert_eq!(outcome.saved["account_type"], "STD");
            assert_eq!(outcome.saved["is_active"], active);
            assert_eq!(outcome.saved["status"], "idle");
            assert_eq!(outcome.saved["refresh_interval"], 0);
            assert_eq!(outcome.saved["custom_properties"]["enable_vod"], false);
        }
    }

    #[test]
    fn remote_std_create_accepts_typed_editor_defaults() {
        let url = "https://example.invalid/owned.m3u";
        let input = json!({
            "name": "Owned",
            "server_url": url,
            "account_type": "STD",
            "is_active": true,
            "refresh_interval": 24,
            "cron_expression": "",
            "exp_date": null,
            "server_group": null,
            "user_agent": null,
            "username": "",
            "password": "",
            "hash_key": null,
            "stale_stream_days": 0,
            "max_streams": 0,
            "enable_vod": false
        });
        let outcome = validate(
            input.as_object().unwrap().clone(),
            &defaults(),
            "",
            false,
            true,
        )
        .unwrap();
        assert_eq!(outcome.saved["server_url"], url);
        assert_eq!(outcome.saved["refresh_interval"], 24);
        assert_eq!(outcome.expiration, Some(None));
        assert!(outcome.cron.is_empty());
    }

    #[test]
    fn remote_std_create_preserves_other_provider_url_admission() {
        let url = "https://example.invalid/owned.m3u";
        for (input, create) in [
            (json!({"name": "Owned", "server_url": url}), true),
            (
                json!({"name": "Owned", "server_url": url, "account_type": "STD",
                       "file_path": "owned.m3u"}),
                true,
            ),
            (
                json!({"name": "Owned", "server_url": url, "account_type": "STD",
                       "enable_vod": true}),
                true,
            ),
            (json!({"server_url": url, "account_type": "STD"}), false),
        ] {
            assert!(matches!(
                validate(
                    input.as_object().unwrap().clone(),
                    &defaults(),
                    "",
                    !create,
                    create
                ),
                Err(StoreError::Unsupported(_)),
            ));
        }
        let xc = json!({"name": "Owned", "server_url": url, "account_type": "XC"});
        assert_eq!(
            validate(
                xc.as_object().unwrap().clone(),
                &defaults(),
                "",
                false,
                true
            )
            .unwrap()
            .saved["server_url"],
            url
        );
    }

    #[test]
    fn xc_group_selection_preserves_activity_and_vod_preferences() {
        let groups = json!({"channel_groups":[
            {"channel_group":11,"enabled":false},
            {"channel_group":29,"enabled":true}
        ]});
        let groups = groups.as_object().unwrap();
        for active in [true, false] {
            for properties in [
                json!({"enable_vod":true,"marker":"keep"}),
                json!({"enable_vod":false}),
                json!({}),
                Value::Null,
            ] {
                let mut old = defaults();
                old["account_type"] = json!("XC");
                old["is_active"] = json!(active);
                old["custom_properties"] = properties;
                let actual = validate(groups.clone(), &old, "", true, false).unwrap();
                let mut expected = old;
                if expected["custom_properties"].is_null() {
                    expected["custom_properties"] = json!({});
                }
                assert_eq!(actual.saved, expected);
                assert_eq!(actual.group_selection, [(11, false), (29, true)]);
                assert!(!actual.hash_changed);
                assert_eq!(actual.expiration, None);
            }
        }
    }
    #[test]
    fn xc_group_selection_keeps_other_mutations_outside_its_scope() {
        let mut old = defaults();
        old["account_type"] = json!("XC");
        old["is_active"] = json!(false);
        old["custom_properties"] = json!({"enable_vod":true});
        for input in [
            json!({"channel_groups":[],"enable_vod":false}),
            json!({"channel_groups":[{"channel_group":11,"enabled":false,"auto_channel_sync":true}]}),
        ] {
            assert!(matches!(
                validate(input.as_object().unwrap().clone(), &old, "", true, false),
                Err(StoreError::Unsupported(_))
            ));
        }
        old["custom_properties"] = json!("non-mapping preference state");
        assert!(matches!(
            validate(
                Map::from_iter([("channel_groups".into(), json!([]))]),
                &old,
                "",
                true,
                false
            ),
            Err(StoreError::Unsupported(_))
        ));
    }
    #[test]
    fn duplicate_group_selections_validate_all_indices_and_keep_first_value() {
        let mut old = defaults();
        old["account_type"] = json!("XC");
        let input = json!({"channel_groups":[
            {"channel_group":11,"enabled":false},
            {"channel_group":29,"enabled":true},
            {"channel_group":11,"enabled":true},
            {"channel_group":29,"enabled":false}
        ]});
        let mut selection = validate(input.as_object().unwrap().clone(), &old, "", true, false)
            .unwrap()
            .group_selection;
        let ids: Vec<_> = selection.iter().map(|(id, _)| *id).collect();
        assert_eq!(ids, [11, 29, 11, 29]);
        assert!(matches!(
            validate_group_references(&ids, &[11]),
            Err(StoreError::InvalidInput(body)) if body == json!({"channel_groups":{
                "1":{"channel_group":["Invalid pk \"29\" - object does not exist."]},
                "3":{"channel_group":["Invalid pk \"29\" - object does not exist."]}
            }})
        ));
        assert!(validate_group_references(&ids, &[29, 11]).is_ok());
        keep_first_group_selections(&mut selection);
        assert_eq!(selection, [(11, false), (29, true)]);
    }
    #[test]
    fn xc_live_group_filter_saves_orphan_cleanup_mode_only() {
        let mut old = defaults();
        old["name"] = json!("Owned XC");
        old["account_type"] = json!("XC");
        for active in [true, false] {
            old["is_active"] = json!(active);
            old["status"] = json!(if active { "success" } else { "disabled" });
            for properties in [
                json!({"enable_vod":true,"hash_key":"name,url","marker":{"old":1}}),
                json!({"enable_vod":false,"orphan_channel_cleanup":"always"}),
                Value::Null,
            ] {
                old["custom_properties"] = properties.clone();
                for mode in ["always", "preserve_customized", "never"] {
                    let mut incoming = properties.as_object().cloned().unwrap_or_default();
                    incoming.insert("orphan_channel_cleanup".into(), json!(mode));
                    // Incoming raw hash_key is ignored by the Original serializer.
                    incoming.insert("hash_key".into(), json!("changed"));
                    let mut expected = old.clone();
                    let mut saved_props = properties.as_object().cloned().unwrap_or_default();
                    saved_props.insert("orphan_channel_cleanup".into(), json!(mode));
                    expected["custom_properties"] = Value::Object(saved_props);

                    let input =
                        Map::from_iter([("custom_properties".into(), Value::Object(incoming))]);
                    let actual = validate(input, &old, "45 4 * * *", true, false).unwrap();
                    assert_eq!(actual.saved, expected);
                    assert_eq!(actual.cron, "45 4 * * *");
                    assert!(!actual.hash_changed);
                    assert_eq!(actual.expiration, None);
                    assert!(actual.group_selection.is_empty());
                }
            }
        }
    }

    #[test]
    fn xc_orphan_cleanup_does_not_admit_other_account_changes() {
        let mut old = defaults();
        old["name"] = json!("Owned XC");
        old["account_type"] = json!("XC");
        old["custom_properties"] = json!({"enable_vod":true,"marker":"keep"});
        for input in [
            json!({"custom_properties":{"orphan_channel_cleanup":"invalid"}}),
            json!({"custom_properties":{"orphan_channel_cleanup":"never","enable_vod":false}}),
            json!({"custom_properties":{"orphan_channel_cleanup":"never","marker":"new"}}),
            json!({"custom_properties":{"orphan_channel_cleanup":"never","other_new_key":true}}),
            json!({"custom_properties":{"orphan_channel_cleanup":"never"},"is_active":false}),
            json!({"custom_properties":{"orphan_channel_cleanup":"never"},"hash_key":"name"}),
        ] {
            assert!(matches!(
                validate(input.as_object().unwrap().clone(), &old, "", true, false),
                Err(StoreError::Unsupported(_))
            ));
        }
    }
    #[test]
    fn missing_group_references_preserve_sparse_request_indices() {
        let invalid = validate_group_references(&[9, 20, 4, 45], &[4, 9]);
        assert!(matches!(
            invalid,
            Err(StoreError::InvalidInput(body)) if body == json!({"channel_groups":{
                "1":{"channel_group":["Invalid pk \"20\" - object does not exist."]},
                "3":{"channel_group":["Invalid pk \"45\" - object does not exist."]}
            }})
        ));
        assert!(validate_group_references(&[9, 4], &[4, 9]).is_ok());
        assert!(validate_group_references(&[], &[]).is_ok());
    }
    #[test]
    fn captured_existing_xc_preferences_do_not_admit_general_xc_mutations() {
        let mut old = defaults();
        old["name"] = json!("Owned XC");
        old["account_type"] = json!("XC");
        old["is_active"] = json!(true);
        old["custom_properties"] = json!({"enable_vod":true,"inert":"preserved"});
        let request =
            json!({"auto_enable_new_groups_vod":false,"auto_enable_new_groups_series":false});
        let actual = validate(request.as_object().unwrap().clone(), &old, "", true, false).unwrap();
        assert_eq!(
            actual.saved["custom_properties"],
            json!({"enable_vod":true,"inert":"preserved","auto_enable_new_groups_vod":false,"auto_enable_new_groups_series":false})
        );
        assert!(!actual.hash_changed);
        assert_eq!(actual.expiration, None);
        let mut wider = request.clone();
        wider["enable_vod"] = json!(false);
        assert!(validate(wider.as_object().unwrap().clone(), &old, "", true, false).is_err());
        assert!(validate(request.as_object().unwrap().clone(), &old, "", false, false).is_err());
        assert!(validate(Map::new(), &old, "", true, false).is_err());
        assert!(validate(
            json!({"auto_enable_new_groups_live":"true"})
                .as_object()
                .unwrap()
                .clone(),
            &old,
            "",
            true,
            false
        )
        .is_err());
        old["custom_properties"] = json!("non-mapping preference state");
        assert!(validate(request.as_object().unwrap().clone(), &old, "", true, false).is_err());
    }

    #[test]
    fn xc_playlist_editor_switches_between_interval_and_daily_cron() {
        for active in [true, false] {
            let mut old = defaults();
            old["name"] = json!("Playlist");
            old["account_type"] = json!("XC");
            old["server_url"] = json!("http://owned-provider:8080/api");
            old["username"] = json!("fixture-user");
            old["is_active"] = json!(active);
            old["status"] = json!(if active { "success" } else { "disabled" });
            old["refresh_interval"] = json!(137);
            old["custom_properties"] =
                json!({"enable_vod":true,"hash_key":"name,url","marker":"keep"});
            let request = json!({
                "name": "Playlist", "account_type":"XC", "server_url":"http://owned-provider:8080/api",
                "username":"fixture-user", "user_agent":null, "server_group":null,
                "is_active":active, "max_streams":0, "stale_stream_days":7, "priority":0,
                "refresh_interval":0, "cron_expression":"37 4 * * *", "enable_vod":true, "hash_key":"name,url"
            });
            let enter =
                validate(request.as_object().unwrap().clone(), &old, "", true, false).unwrap();
            old["refresh_interval"] = json!(0);
            assert_eq!(enter.saved, old);
            assert_eq!(enter.cron, "37 4 * * *");
            assert!(!enter.hash_changed && enter.expiration.is_none());

            let mut request = request.as_object().unwrap().clone();
            request.insert("refresh_interval".into(), json!(6));
            request.insert("cron_expression".into(), json!(""));
            let leave = validate(request, &old, &enter.cron, true, false).unwrap();
            old["refresh_interval"] = json!(6);
            assert_eq!(leave.saved, old);
            assert!(leave.cron.is_empty() && !leave.hash_changed && leave.expiration.is_none());
        }
    }

    #[test]
    fn xc_daily_schedule_transition_rejects_broader_schedule_and_account_changes() {
        let mut old = defaults();
        old["name"] = json!("Playlist");
        old["account_type"] = json!("XC");
        old["custom_properties"] = json!({"enable_vod":false});
        for (request, previous_cron) in [
            (
                json!({"refresh_interval":0, "cron_expression":"60 4 * * *"}),
                "",
            ),
            (
                json!({"refresh_interval":0, "cron_expression":"37 24 * * *"}),
                "",
            ),
            (
                json!({"refresh_interval":0, "cron_expression":"37 4 * * 0"}),
                "",
            ),
            (
                json!({"refresh_interval":0, "cron_expression":"37  4 * * *"}),
                "",
            ),
            (
                json!({"refresh_interval":1, "cron_expression":"37 4 * * *"}),
                "",
            ),
            (
                json!({"refresh_interval":0, "cron_expression":"37 4 * * *", "name":"Other"}),
                "",
            ),
            (
                json!({"refresh_interval":0, "cron_expression":"37 4 * * *", "is_active":false}),
                "",
            ),
            (
                json!({"refresh_interval":0, "cron_expression":"37 4 * * *"}),
                "31 4 * * *",
            ),
            (json!({"cron_expression":""}), "37 4 * * *"),
        ] {
            assert!(matches!(
                validate(
                    request.as_object().unwrap().clone(),
                    &old,
                    previous_cron,
                    true,
                    false
                ),
                Err(StoreError::Unsupported(_))
            ));
        }
    }

    #[test]
    fn xc_playlist_interval_edit_preserves_the_account() {
        for active in [true, false] {
            let mut old = defaults();
            old["name"] = json!("Playlist");
            old["account_type"] = json!("XC");
            old["server_url"] = json!("http://owned-provider:8080/api");
            old["username"] = json!("fixture-user");
            old["is_active"] = json!(active);
            old["status"] = json!(if active { "success" } else { "disabled" });
            old["refresh_interval"] = json!(6);
            old["custom_properties"] =
                json!({"enable_vod":true,"hash_key":"name,url","marker":"keep"});
            for hours in [0, 17] {
                let request = json!({
                    "name": "Playlist", "account_type":"XC", "server_url":"http://owned-provider:8080/api",
                    "username":"fixture-user", "user_agent": null, "server_group":null,
                    "is_active":active, "max_streams":0, "stale_stream_days":7, "priority":0,
                    "refresh_interval":hours, "cron_expression":"", "enable_vod":true, "hash_key":"name,url"
                });
                let edit =
                    validate(request.as_object().unwrap().clone(), &old, "", true, false).unwrap();
                let mut expected = old.clone();
                expected["refresh_interval"] = json!(hours);
                assert_eq!(edit.saved, expected);
                assert!(edit.cron.is_empty() && !edit.hash_changed && edit.expiration.is_none());
            }
        }
    }

    #[test]
    fn xc_playlist_interval_edit_rejects_wider_changes() {
        let mut old = defaults();
        old["name"] = json!("Playlist");
        old["account_type"] = json!("XC");
        old["custom_properties"] = json!({"enable_vod":false});
        for (request, old_cron) in [
            (json!({"refresh_interval":6, "name":"Other"}), ""),
            (json!({"refresh_interval":6, "is_active":false}), ""),
            (json!({"refresh_interval":6, "enable_vod":true}), ""),
            (json!({"refresh_interval":6, "hash_key":"name"}), ""),
            (
                json!({"refresh_interval":6, "cron_expression":"30 5 * * *"}),
                "",
            ),
            (
                json!({"refresh_interval":6, "cron_expression":""}),
                "*/15 5 * * *",
            ),
            (json!({"refresh_interval":6}), "30 5 * * *"),
            (json!({"refresh_interval":-1}), ""),
        ] {
            assert!(matches!(
                validate(
                    request.as_object().unwrap().clone(),
                    &old,
                    old_cron,
                    true,
                    false
                ),
                Err(StoreError::Unsupported(_))
            ));
        }
    }

    #[test]
    fn xc_playlist_editor_rename_preserves_the_echoed_values() {
        let mut old = defaults();
        old["name"] = json!("Old name");
        old["account_type"] = json!("XC");
        old["server_url"] = json!("http://owned-provider:8080/api");
        old["username"] = json!("fixture-user");
        old["user_agent_id"] = json!(7);
        old["server_group_id"] = json!(9);
        old["custom_properties"] = json!({"enable_vod":true,"hash_key":"name,url","marker":"keep"});
        let mut input = json!({
            "name": "Updated name",
            "account_type": "XC",
            "server_url": "http://owned-provider:8080/api",
            "username": "fixture-user",
            "user_agent": "7",
            "server_group": "9",
            "is_active": true,
            "max_streams": 0,
            "refresh_interval": 0,
            "cron_expression": "30 5 * * *",
            "stale_stream_days": 7,
            "priority": 0,
            "enable_vod": true,
            "hash_key": "name,url"
        });

        let renamed = validate(
            input.as_object().unwrap().clone(),
            &old,
            "30 5 * * *",
            true,
            false,
        )
        .unwrap();
        let mut expected = old.clone();
        expected["name"] = input["name"].clone();
        assert_eq!(renamed.saved, expected);
        assert_eq!(renamed.cron, "30 5 * * *");
        assert!(renamed.expiration.is_none() && !renamed.hash_changed);

        old["custom_properties"] = Value::Null;
        input["hash_key"] = Value::Null;
        input["enable_vod"] = Value::Bool(false);
        input["cron_expression"] = json!("");
        let renamed = validate(input.as_object().unwrap().clone(), &old, "", true, false).unwrap();
        assert_eq!(renamed.saved["name"], input["name"]);
        assert_eq!(
            renamed.saved["custom_properties"],
            json!({"enable_vod":false})
        );
        assert!(!renamed.hash_changed);
        let named = validate(
            Map::from_iter([("name".into(), json!("Label only"))]),
            &old,
            "",
            true,
            false,
        )
        .unwrap();
        assert_eq!(named.saved["name"], "Label only");
    }

    #[test]
    fn xc_playlist_editor_rename_keeps_other_mutations_guarded() {
        let mut old = defaults();
        old["name"] = json!("Old name");
        old["account_type"] = json!("XC");
        old["server_url"] = json!("invalid stored URL");
        old["custom_properties"] = json!({"enable_vod":false});
        for request in [
            json!({"name":"Updated","is_active":false}),
            json!({"name":"Updated","enable_vod":true}),
            json!({"name":"Updated","username":"other"}),
            json!({"name":"Updated","password":"fixture-only"}),
            json!({"name":"Updated","server_url":"invalid stored URL"}),
            json!({"name":"Updated","hash_key":"name"}),
            json!({"name":"Updated","cron_expression":"31 5 * * *"}),
            json!({"name":"Updated","max_streams":2}),
        ] {
            assert!(matches!(
                validate(request.as_object().unwrap().clone(), &old, "", true, false),
                Err(StoreError::Unsupported(_))
            ));
        }
    }

    #[test]
    fn xc_playlist_editor_saves_capacity_and_priority_with_the_existing_cron() {
        let mut old = defaults();
        old["name"] = json!("Owned playlist");
        old["account_type"] = json!("XC");
        old["server_url"] = json!("http://owned-provider:8080/api");
        old["username"] = json!("fixture-user");
        old["priority"] = json!(4);
        old["max_streams"] = json!(2);
        old["custom_properties"] = json!({"enable_vod":true,"hash_key":"name,url","marker":"keep"});
        let cron = "30 5 * * *";
        for (active, limit, priority) in [(true, 6, 4), (false, 2, 3), (true, 0, 9)] {
            old["is_active"] = json!(active);
            old["status"] = json!(if active { "idle" } else { "disabled" });
            let request = json!({
                "name": old["name"], "account_type": "XC", "is_active": active,
                "server_url": old["server_url"], "username": old["username"],
                "user_agent": null, "server_group": null, "enable_vod": true,
                "refresh_interval": 0, "cron_expression": cron, "stale_stream_days": 7,
                "hash_key": "name,url", "max_streams": limit, "priority": priority,
            });
            let prepared = validate(
                request.as_object().unwrap().clone(),
                &old,
                cron,
                true,
                false,
            )
            .unwrap();
            let mut expected = old.clone();
            expected["max_streams"] = json!(limit);
            expected["priority"] = json!(priority);
            assert_eq!(prepared.saved, expected);
            assert_eq!(prepared.cron, cron);
            assert!(
                prepared.expiration.is_none()
                    && !prepared.hash_changed
                    && prepared.group_selection.is_empty()
            );
        }
    }

    #[test]
    fn xc_playlist_editor_capacity_and_priority_keep_other_changes_guarded() {
        let mut old = defaults();
        old["name"] = json!("Owned playlist");
        old["account_type"] = json!("XC");
        for request in [
            json!({"name":"another name","max_streams":2}),
            json!({"is_active":false,"priority":7}),
            json!({"refresh_interval":2,"max_streams":3,"priority":4}),
            json!({"cron_expression":"30 5 * * *","priority":7}),
            json!({"password":"fixture-only","max_streams":2}),
            json!({"priority":"9","max_streams":2}),
        ] {
            assert!(matches!(
                validate(request.as_object().unwrap().clone(), &old, "", true, false),
                Err(StoreError::Unsupported(_))
            ));
        }
        for key in ["max_streams", "priority"] {
            let request = Map::from_iter([(key.into(), json!(-1))]);
            assert!(matches!(
                validate(request, &old, "", true, false),
                Err(StoreError::InvalidInput(ref error)) if error[key] == json!(["Ensure this value is greater than or equal to 0."])
            ));
        }
    }

    #[test]
    fn xc_playlist_table_toggle_preserves_account_settings() {
        let mut old = defaults();
        old["name"] = json!("Owned XC");
        old["account_type"] = json!("XC");
        old["refresh_interval"] = json!(12);
        let properties = json!({"enable_vod":true,"marker":"keep","hash_key":"name,url"});

        for properties in [properties, Value::Null] {
            old["custom_properties"] = properties.clone();
            for (previous, status, active, expected_status) in [
                (true, "success", false, "disabled"),
                (false, "disabled", true, "idle"),
                (true, "success", true, "success"),
                (false, "error", false, "error"),
            ] {
                old["is_active"] = json!(previous);
                old["status"] = json!(status);
                let actual = validate(
                    Map::from_iter([("is_active".into(), json!(active))]),
                    &old,
                    "30 5 * * *",
                    true,
                    false,
                )
                .unwrap();
                let mut expected = old.clone();
                expected["is_active"] = json!(active);
                expected["status"] = json!(expected_status);
                expected["custom_properties"] = Value::Object(super::properties(&properties));
                assert_eq!(actual.saved, expected);
                assert_eq!(actual.cron, "30 5 * * *");
                assert_eq!(actual.expiration, None);
                assert!(!actual.hash_changed);
                assert!(actual.group_selection.is_empty());
            }
        }
    }

    #[test]
    fn xc_activity_toggle_does_not_admit_wider_account_edits() {
        let mut old = defaults();
        old["name"] = json!("Owned XC");
        old["account_type"] = json!("XC");

        for input in [
            json!({"is_active":false,"status":"success"}),
            json!({"is_active":false,"enable_vod":true}),
            json!({"is_active":false,"name":"Other"}),
            json!({"is_active":false,"hash_key":"name"}),
            json!({"is_active":"false"}),
        ] {
            assert!(matches!(
                validate(input.as_object().unwrap().clone(), &old, "", true, false),
                Err(StoreError::Unsupported(_))
            ));
        }
        assert!(validate(
            Map::from_iter([("is_active".into(), json!(false))]),
            &old,
            "",
            false,
            false
        )
        .is_err());
    }

    #[test]
    fn xc_group_filter_preferences_preserve_account_state() {
        let mut old = defaults();
        old["name"] = json!("Owned XC");
        old["account_type"] = json!("XC");
        let states = [
            json!({"enable_vod":true,"marker":"keep","auto_enable_new_groups_live":true}),
            json!({"enable_vod":false}),
            json!({}),
            Value::Null,
        ];
        let inputs = [
            json!({
                "auto_enable_new_groups_live":false,
                "auto_enable_new_groups_vod":false,
                "auto_enable_new_groups_series":true
            }),
            json!({"auto_enable_new_groups_series":false}),
        ];

        for active in [true, false] {
            old["is_active"] = json!(active);
            old["status"] = json!(if active { "success" } else { "disabled" });
            for properties in &states {
                old["custom_properties"] = properties.clone();
                for request in &inputs {
                    let request = request.as_object().unwrap();
                    let mut expected = old.clone();
                    let mut expected_properties =
                        properties.as_object().cloned().unwrap_or_default();
                    expected_properties.extend(request.clone());
                    expected["custom_properties"] = Value::Object(expected_properties);
                    let actual =
                        validate(request.clone(), &old, "30 5 * * *", true, false).unwrap();
                    assert_eq!(actual.saved, expected);
                    assert_eq!(actual.cron, "30 5 * * *");
                    assert!(!actual.hash_changed);
                    assert_eq!(actual.expiration, None);
                    assert!(actual.group_selection.is_empty());
                }
            }
        }
    }
}
