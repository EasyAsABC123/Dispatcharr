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
use sqlx::{PgConnection, Row};
use std::{net::SocketAddr, sync::Arc};
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
    cron: &str,
    partial: bool,
    create: bool,
) -> Result<Validated, StoreError> {
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
                } else if create && input.get("account_type").and_then(Value::as_str) == Some("XC")
                {
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
                        let parsed = url::Url::parse(&text).map_err(|_| unsupported())?;
                        if !matches!(parsed.scheme(), "http" | "https")
                            || parsed.host_str().is_none()
                            || !parsed.username().is_empty()
                            || parsed.password().is_some()
                            || parsed.fragment().is_some()
                        {
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
        None if partial => cron.to_owned(),
        None => String::new(),
    };
    let expiration = input
        .get("exp_date")
        .map(|v| parse_datetime("exp_date", v, true))
        .transpose()?;
    if !errors.is_empty() {
        return Err(StoreError::InvalidInput(Value::Object(errors)));
    }
    // Source3 qualified an ordinary PATCH of these two preferences on an
    // existing activeXC account. Do not admit XC create/credentials/activation,
    // enable_vod transitions or a general unqualified XC full serializer.
    let xc_preferences = !create
        && partial
        && old["account_type"] == "XC"
        && saved["account_type"] == "XC"
        && old["is_active"] == true
        && old["custom_properties"].is_object()
        && old["custom_properties"]["enable_vod"] == true
        && input.len() == 2
        && input.get("auto_enable_new_groups_vod") == Some(&Value::Bool(false))
        && input.get("auto_enable_new_groups_series") == Some(&Value::Bool(false));
    let xc_group_selection = !create
        && partial
        && old["account_type"] == "XC"
        && saved["account_type"] == "XC"
        && old["is_active"] == true
        && old["custom_properties"]["enable_vod"] == false
        && input.len() == 1
        && input.contains_key("channel_groups");
    let mut group_selection = Vec::new();
    if xc_group_selection {
        let groups = input["channel_groups"]
            .as_array()
            .filter(|groups| groups.len() <= 256)
            .ok_or_else(unsupported)?;
        let mut seen = std::collections::HashSet::new();
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
            // Duplicate-input CASE ordering and wider nested fields remain
            // outside this measured ordinary selection prerequisite.
            if !seen.insert(id) {
                return Err(unsupported());
            }
            group_selection.push((id, enabled));
        }
    }
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
        && !xc_create
        && !xc_group_selection
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
        session.run(move|c|Box::pin(async move{sqlx::query("UPDATE m3u_m3uaccountprofile SET max_streams=$2 WHERE m3u_account_id=$1 AND is_default AND max_streams<>$2").bind(id).bind(max).execute(c).await.map(|_|())})).await??;
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
        || auth.core_task_broker.get().is_none()
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
                let row=session.run(move|c|Box::pin(async move{sqlx::query("SELECT CASE WHEN octet_length(to_jsonb(a)::text)<=1048576 THEN to_jsonb(a) END AS raw,COALESCE(concat_ws(' ',ct.minute,ct.hour,ct.day_of_month,ct.month_of_year,ct.day_of_week),'') AS cron FROM m3u_m3uaccount a LEFT JOIN django_celery_beat_periodictask t ON t.id=a.refresh_task_id LEFT JOIN django_celery_beat_crontabschedule ct ON ct.id=t.crontab_id WHERE a.id=$1").bind(id).fetch_optional(c).await})).await??.ok_or_else(missing)?;
                let raw:Option<Value>=row.try_get("raw")?;let old=raw.ok_or_else(unsupported)?;if old["locked"]==true{return Err(locked())}let cron:String=row.try_get("cron")?;(old,cron)
            }else{(defaults(),String::new())};
            let validated=validate(input,&old,&old_cron,partial,id.is_none())?;let saved=validated.saved;let cron=validated.cron;
            let check=saved.clone();let group_ids:Vec<i64>=validated.group_selection.iter().map(|(id,_)|*id).collect();
            session.run(move|c|Box::pin(async move{
                preflight(c,id,&check).await?;
                if !group_ids.is_empty() {
                    let found:i64=sqlx::query_scalar("SELECT count(*) FROM dispatcharr_channels_channelgroup WHERE id::bigint=ANY($1)").bind(&group_ids).fetch_one(c).await?;
                    if found!=group_ids.len() as i64{return Err(unsupported());}
                }
                Ok::<_,StoreError>(())
            })).await??;
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
        old["is_active"] = json!(false);
        assert!(validate(request.as_object().unwrap().clone(), &old, "", true, false).is_err());
    }
}
