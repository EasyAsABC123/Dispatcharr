//! Original Dispatcharr notification reads and personal dismissal contracts.
//! Original application authors retain full credit; see ATTRIBUTION.md.
use crate::{channels::truthy, django_pickle, AuthService, StoreError, User};
use axum::{
    body::Bytes,
    extract::{ConnectInfo, Path, RawQuery, State},
    http::HeaderMap,
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, SecondsFormat, Utc};
use redis::{
    aio::{ConnectionManager, ConnectionManagerConfig},
    FromRedisValue,
};
use serde_json::{json, Map, Value};
use sqlx::{postgres::PgRow, Row};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::timeout,
};

#[derive(Clone)]
pub struct NotificationCache {
    connection: ConnectionManager,
    permits: Arc<Semaphore>,
    queued: Arc<Semaphore>,
    retained_scope: Option<Arc<dyn Send + Sync>>,
}
enum CacheOperationError {
    Admission,
    Timeout,
    Task,
    Redis(redis::RedisError),
}
fn unavailable() -> StoreError {
    StoreError::StatusBody(503, json!({"detail":"Notification cache unavailable."}))
}
fn failed() -> StoreError {
    StoreError::StatusBody(500, json!({"detail":"Notification processing failed."}))
}
impl NotificationCache {
    pub async fn connect(url: &str) -> Result<Self, StoreError> {
        let client = redis::Client::open(url).map_err(|_| unavailable())?;
        let connection = timeout(
            Duration::from_secs(5),
            client.get_connection_manager_with_config(
                ConnectionManagerConfig::new()
                    .set_number_of_retries(0)
                    .set_connection_timeout(Duration::from_secs(5)),
            ),
        )
        .await
        .map_err(|_| unavailable())?
        .map_err(|_| unavailable())?;
        Ok(Self {
            connection,
            permits: Arc::new(Semaphore::new(16)),
            queued: Arc::new(Semaphore::new(256)),
            retained_scope: None,
        })
    }
    /// Clone scope only; connection/admission budgets stay globally shared.
    /// Nested scopes compose instead of replacing an earlier owner's lease.
    pub(crate) fn with_retained_scope(mut self, scope: Arc<dyn Send + Sync>) -> Self {
        self.retained_scope = Some(match self.retained_scope.take() {
            Some(previous) => Arc::new((previous, scope)),
            None => scope,
        });
        self
    }
    async fn admit(&self) -> Result<Arc<OwnedSemaphorePermit>, CacheOperationError> {
        let permit = match self.permits.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                // Bound futures waiting for physical admission as well as the
                // operations themselves. Exhaustion keeps the existing cache
                // unavailable classification rather than an unbounded queue.
                let _queued = self
                    .queued
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| CacheOperationError::Admission)?;
                timeout(Duration::from_secs(2), self.permits.clone().acquire_owned())
                    .await
                    .map_err(|_| CacheOperationError::Admission)?
                    .map_err(|_| CacheOperationError::Admission)?
            }
        };
        Ok(Arc::new(permit))
    }
    async fn query_admitted<T: FromRedisValue + Send + 'static>(
        &self,
        cmd: redis::Cmd,
        permit: Arc<OwnedSemaphorePermit>,
    ) -> Result<T, CacheOperationError> {
        let mut connection = self.connection.clone();
        let scope = self.retained_scope.clone();
        // The owned physical future is detached on logical timeout/caller
        // cancellation. Its permit/scope survive until the actual reply (or
        // connection error), not just the observer's two-second deadline.
        let task = tokio::spawn(async move {
            let result = cmd.query_async::<T>(&mut connection).await;
            (result, permit, scope)
        });
        let (result, _permit, _scope) = timeout(Duration::from_secs(2), task)
            .await
            .map_err(|_| CacheOperationError::Timeout)?
            .map_err(|_| CacheOperationError::Task)?;
        result.map_err(CacheOperationError::Redis)
    }
    pub(crate) async fn command<T: FromRedisValue + Send + 'static>(
        &self,
        cmd: redis::Cmd,
    ) -> Result<T, StoreError> {
        let permit = self.admit().await.map_err(|_| unavailable())?;
        self.query_admitted(cmd, permit)
            .await
            .map_err(|_| unavailable())
    }
    #[cfg(test)]
    pub(crate) fn physical_operations(&self) -> usize {
        16 - self.permits.available_permits()
    }
    pub(crate) async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        let mut cmd = redis::cmd("EVAL");
        cmd.arg("if redis.call('STRLEN',KEYS[1])>1048576 then return redis.error_reply('cache value exceeds limit') end return redis.call('GET',KEYS[1])").arg(1).arg(key);
        self.command(cmd).await
    }
    pub(crate) async fn invalidate_user_agent(&self, version: &str) -> Result<(), StoreError> {
        // CoreSettings signals ignore connectivity/timeout errors, but do not
        // swallow authentication, permission or server command failures.
        let Ok(permit) = self.admit().await else {
            return Ok(());
        };
        let mut delete = redis::cmd("DEL");
        delete.arg(":1:coresettings:default_user_agent");
        let mut bump = redis::cmd("SET");
        bump.arg(":1:coresettings:default_user_agent:ver")
            .arg(version);
        for command in [delete, bump] {
            // Both original logical attempts share one operation slot. Each
            // physical command retains a clone, so a late first response does
            // not release admission while the second command is still queued.
            match self
                .query_admitted::<redis::Value>(command, permit.clone())
                .await
            {
                Ok(_) => {}
                Err(CacheOperationError::Timeout | CacheOperationError::Admission) => {
                    tracing::warn!("Default User-Agent cache invalidation timed out")
                }
                Err(CacheOperationError::Redis(error))
                    if error.is_io_error() || error.is_timeout() =>
                {
                    tracing::warn!("Default User-Agent cache invalidation backend unavailable")
                }
                Err(_) => {
                    return Err(StoreError::StatusBody(
                        500,
                        json!({"detail":"Default User-Agent cache invalidation failed."}),
                    ))
                }
            }
        }
        Ok(())
    }
    async fn set(&self, key: &str, value: &Value) -> Result<(), StoreError> {
        self.set_for(key, value, 300).await
    }
    pub(crate) async fn set_for(
        &self,
        key: &str,
        value: &Value,
        seconds: u64,
    ) -> Result<(), StoreError> {
        let encoded = django_pickle::encode(value).ok_or_else(failed)?;
        let mut cmd = redis::cmd("SET");
        cmd.arg(key).arg(encoded).arg("EX").arg(seconds);
        self.command(cmd).await
    }
    async fn network_settings<F, Fut>(&self, load: F) -> Result<Value, StoreError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<Value, StoreError>>,
    {
        self.settings_group("network_access", load).await
    }
    pub(crate) async fn settings_group<F, Fut>(
        &self,
        key: &str,
        load: F,
    ) -> Result<Value, StoreError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<Value, StoreError>>,
    {
        let group_key = format!(":1:coresettings:group:{key}");
        let version_key = format!(":1:coresettings:groupver:{key}");
        // _get_group falls through to DB without filling on a backend outage.
        let cached = match self.get(&group_key).await {
            Ok(value) => value,
            Err(_) => return load().await,
        };
        if let Some(raw) = cached {
            let value = decode_cache(&raw).ok_or_else(failed)?;
            if value.is_object() {
                return Ok(value);
            }
        }
        let before = match self.get(&version_key).await {
            Ok(raw) => raw
                .map(|raw| decode_cache(&raw).ok_or_else(failed))
                .transpose()?,
            Err(_) => return load().await,
        };
        let value = load().await?;
        let after = match self.get(&version_key).await {
            Ok(raw) => raw
                .map(|raw| decode_cache(&raw).ok_or_else(failed))
                .transpose()?,
            Err(_) => return Ok(value),
        };
        if crate::json_compat::python_equal(
            before.as_ref().unwrap_or(&Value::Null),
            after.as_ref().unwrap_or(&Value::Null),
        ) {
            let _ = self.set(&group_key, &value).await;
        }
        Ok(value)
    }
    async fn visible(
        &self,
        auth: &AuthService,
        user: &User,
        id: i64,
        conditions: &Value,
    ) -> Result<bool, StoreError> {
        let key = format!(":1:dev_notif_condition_{id}_{}", user.id);
        if let Some(raw) = self.get(&key).await? {
            let value = decode_cache(&raw).ok_or_else(failed)?;
            if !value.is_null() {
                return Ok(truthy(Some(&value)));
            }
        }
        let conditions = condition_items(conditions)?;
        let mut show = true;
        for condition in conditions {
            if condition.is_array() || condition.is_object() {
                return Err(failed());
            }
            if condition.as_str() == Some("m3u_epg_network_insecure") {
                // Original check catches every exception and returns False.
                let settings = self
                    .network_settings(|| load_network(auth))
                    .await
                    .unwrap_or(Value::Null);
                show = network_insecure(&settings);
                if !show {
                    break;
                }
            }
        }
        self.set(&key, &Value::Bool(show)).await?;
        Ok(show)
    }
}
pub(crate) fn decode_cache(raw: &[u8]) -> Option<Value> {
    // django_redis stores integer values directly and pickles all bools/JSON.
    if let Ok(text) = std::str::from_utf8(raw) {
        if !text.is_empty()
            && text.len() <= 4301
            && text.trim() == text
            && text
                .strip_prefix('-')
                .unwrap_or(text)
                .bytes()
                .all(|v| v.is_ascii_digit())
        {
            return serde_json::from_str(text).ok();
        }
    }
    django_pickle::decode(raw)
}
async fn load_network(auth: &AuthService) -> Result<Value, StoreError> {
    let settings = auth.store.setting("network_access").await?;
    Ok(if settings.is_object() {
        settings
    } else {
        json!({})
    })
}
fn network_insecure(settings: &Value) -> bool {
    settings
        .get("M3U_EPG")
        .and_then(Value::as_str)
        .is_some_and(|value| {
            value
                .split(',')
                .map(str::trim)
                .any(|network| matches!(network, "0.0.0.0/0" | "::/0"))
        })
}
fn condition_items(conditions: &Value) -> Result<Vec<Value>, StoreError> {
    match conditions {
        Value::String(_) => Ok(vec![conditions.clone()]),
        Value::Array(items) => Ok(items.clone()),
        Value::Object(items) => Ok(items.keys().cloned().map(Value::String).collect()),
        _ => Err(failed()),
    }
}
fn date(value: DateTime<Utc>) -> Value {
    value
        .to_rfc3339_opts(
            if value.timestamp_subsec_micros() == 0 {
                SecondsFormat::Secs
            } else {
                SecondsFormat::Micros
            },
            true,
        )
        .into()
}
fn serialize(row: PgRow) -> Result<Value, StoreError> {
    let mut value = Map::new();
    value.insert("id".into(), row.try_get::<i64, _>("object_id")?.into());
    for key in [
        "notification_key",
        "notification_type",
        "priority",
        "title",
        "message",
        "source",
    ] {
        value.insert(key.into(), row.try_get::<String, _>(key)?.into());
    }
    for key in ["is_active", "admin_only"] {
        value.insert(key.into(), row.try_get::<bool, _>(key)?.into());
    }
    value.insert("action_data".into(), row.try_get("action_data")?);
    value.insert("created_at".into(), date(row.try_get("created_at")?));
    value.insert(
        "expires_at".into(),
        row.try_get::<Option<DateTime<Utc>>, _>("expires_at")?
            .map_or(Value::Null, date),
    );
    value.insert(
        "is_dismissed".into(),
        row.try_get::<bool, _>("dismissed")?.into(),
    );
    Ok(Value::Object(value))
}
async fn visible(
    auth: &AuthService,
    user: &User,
    id: Option<i64>,
) -> Result<Vec<Value>, StoreError> {
    let rows=sqlx::query("SELECT n.*,n.id::bigint AS object_id,EXISTS(SELECT 1 FROM core_notificationdismissal d WHERE d.notification_id=n.id AND d.user_id=$1) AS dismissed FROM core_systemnotification n WHERE n.is_active AND (n.expires_at IS NULL OR n.expires_at>now()) AND ($2 OR NOT n.admin_only) ORDER BY n.priority DESC,n.created_at DESC")
        .bind(user.id).bind(user.user_level>=10).fetch_all(auth.store.pool()).await?;
    let mut out = Vec::new();
    for row in rows {
        let value = serialize(row)?;
        if value["source"] == "developer" {
            let action = &value["action_data"];
            if truthy(Some(action)) {
                let action = action.as_object().ok_or_else(failed)?;
                if let Some(conditions) =
                    action.get("condition").filter(|value| truthy(Some(value)))
                {
                    let cache=auth.notification_cache.as_ref().ok_or(StoreError::Unsupported("Developer notification conditions require the original shared Redis cache."))?;
                    if !cache
                        .visible(auth, user, value["id"].as_i64().expect("id"), conditions)
                        .await?
                    {
                        continue;
                    }
                }
            }
        }
        if id.is_none_or(|id| value["id"].as_i64() == Some(id)) {
            out.push(value);
        }
    }
    Ok(out)
}
fn key(raw: String) -> Result<i64, StoreError> {
    crate::channel_memberships::primary_key(&Value::String(raw))
        .map_err(|_| StoreError::NotFound)?
        .ok_or_else(missing)
}
fn missing() -> StoreError {
    StoreError::StatusBody(
        404,
        json!({"detail":"No SystemNotification matches the given query."}),
    )
}
async fn list(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Result<Json<Value>, StoreError> {
    let user = auth.account_user(peer, headers, false).await?;
    let include = url::form_urlencoded::parse(raw.as_deref().unwrap_or("").as_bytes())
        .filter(|(key, _)| key == "include_dismissed")
        .last()
        .is_some_and(|(_, value)| value.to_lowercase() == "true");
    let values: Vec<_> = visible(&auth, &user, None)
        .await?
        .into_iter()
        .filter(|value| include || value["is_dismissed"] != true)
        .collect();
    Ok(Json(
        json!({"count":values.len(),"unread_count":values.len(),"notifications":values}),
    ))
}
async fn detail(
    State(auth): State<AuthService>,
    Path(raw): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Json<Value>, StoreError> {
    let user = auth.account_user(peer, headers, false).await?;
    let id = key(raw)?;
    Ok(Json(
        visible(&auth, &user, Some(id))
            .await?
            .pop()
            .ok_or_else(missing)?,
    ))
}
async fn count(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Json<Value>, StoreError> {
    let user = auth.account_user(peer, headers, false).await?;
    let count = visible(&auth, &user, None)
        .await?
        .iter()
        .filter(|value| value["is_dismissed"] != true)
        .count();
    Ok(Json(json!({"unread_count":count})))
}
// Source request.data is not serializer-validated for this action.
fn action_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(value) => Some(value.clone()),
        Value::Bool(value) => Some(if *value { "True" } else { "False" }.into()),
        Value::Number(value) => Some(if value.is_f64() {
            crate::json_compat::float_string(value.as_f64()?)
        } else {
            value.to_string()
        }),
        Value::Array(_) | Value::Object(_) => crate::python_action::collection_text(value, 50),
    }
}
async fn dismiss(
    State(auth): State<AuthService>,
    Path(raw): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, StoreError> {
    let user = auth.account_user(peer, headers.clone(), false).await?;
    let id = key(raw)?;
    let value = visible(&auth, &user, Some(id))
        .await?
        .pop()
        .ok_or_else(missing)?;
    let body = if body.is_empty() || !headers.contains_key("content-type") {
        json!({})
    } else if headers
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("application/x-www-form-urlencoded"))
    {
        Value::Object(
            url::form_urlencoded::parse(&body)
                .into_owned()
                .map(|(key, value)| (key, Value::String(value)))
                .collect(),
        )
    } else {
        serde_json::from_slice(&body)
            .map_err(|_| StoreError::InvalidInput(json!({"detail":"JSON parse error"})))?
    };
    let body = body.as_object().ok_or_else(failed)?;
    let action = body.get("action_taken").unwrap_or(&Value::Null);
    let text = action_string(action);
    if !action.is_null() && text.is_none() {
        return Err(failed());
    }
    if text.as_ref().is_some_and(|value| value.contains('\0')) {
        return Err(failed());
    }
    if text
        .as_ref()
        .is_some_and(|value| value.chars().count() > 50)
    {
        return Err(failed());
    }
    // Source get_or_create first reads the pair. A normal sequential repeat
    // must not attempt INSERT: PostgreSQL nextval survives ON CONFLICT.
    let mut dismissal: Option<i64> = sqlx::query_scalar(
        "SELECT id::bigint FROM core_notificationdismissal WHERE user_id=$1 AND notification_id=$2",
    )
    .bind(user.id)
    .bind(id)
    .fetch_optional(auth.store.pool())
    .await?;
    let mut created = false;
    if dismissal.is_none() {
        dismissal = sqlx::query_scalar("INSERT INTO core_notificationdismissal(user_id,notification_id,dismissed_at,action_taken) VALUES($1,$2,now(),$3) ON CONFLICT(user_id,notification_id) DO NOTHING RETURNING id::bigint")
            .bind(user.id).bind(id).bind(&text).fetch_optional(auth.store.pool()).await?;
        created = dismissal.is_some();
        if !created {
            // A concurrent winner is read in a fresh statement/snapshot.
            // Source may allocate a losing sequence value in this race too.
            dismissal = sqlx::query_scalar("SELECT id::bigint FROM core_notificationdismissal WHERE user_id=$1 AND notification_id=$2")
                .bind(user.id).bind(id).fetch_optional(auth.store.pool()).await?;
        }
    }
    let dismissal = dismissal.ok_or_else(failed)?;
    if !created && truthy(Some(action)) {
        sqlx::query("UPDATE core_notificationdismissal SET action_taken=$1 WHERE id=$2 AND user_id=$3 AND notification_id=$4")
            .bind(&text).bind(dismissal).bind(user.id).bind(id).execute(auth.store.pool()).await?;
    }
    Ok(Json(
        json!({"success":true,"message":"Notification dismissed","notification_key":value["notification_key"]}),
    ))
}
async fn dismiss_all(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Json<Value>, StoreError> {
    let user = auth.account_user(peer, headers, false).await?;
    let ids: Vec<i64> = visible(&auth, &user, None)
        .await?
        .into_iter()
        .filter(|value| value["is_dismissed"] != true)
        .map(|value| value["id"].as_i64().expect("id"))
        .collect();
    if !ids.is_empty() {
        sqlx::query("INSERT INTO core_notificationdismissal(user_id,notification_id,dismissed_at,action_taken) SELECT $1,id,now(),NULL FROM unnest($2::bigint[]) AS id ON CONFLICT(user_id,notification_id) DO NOTHING").bind(user.id).bind(&ids).execute(auth.store.pool()).await?;
    }
    Ok(Json(json!({"success":true,"dismissed_count":ids.len()})))
}
fn create_field_error(field: &str, message: &str) -> StoreError {
    StoreError::InvalidInput(json!({field:[message]}))
}
fn create_field(
    fields: &mut Map<String, Value>,
    errors: &mut Map<String, Value>,
    name: &str,
    result: Result<Value, StoreError>,
) -> Result<(), StoreError> {
    match result {
        Ok(value) => {
            fields.insert(name.into(), value);
        }
        Err(StoreError::InvalidInput(Value::Object(messages))) => errors.extend(messages),
        Err(error) => return Err(error),
    }
    Ok(())
}
fn create_choice(name: &str, value: &Value, allowed: &[&str]) -> Result<Value, StoreError> {
    if value.is_null() {
        return Err(create_field_error(name, "This field may not be null."));
    }
    if value.as_str().is_some_and(|v| allowed.contains(&v)) {
        return Ok(value.clone());
    }
    let text = match value {
        Value::String(text) => text.clone(),
        Value::Null => unreachable!(),
        Value::Bool(value) => if *value { "True" } else { "False" }.into(),
        Value::Number(value) => {
            if value.is_f64() {
                crate::json_compat::float_string(value.as_f64().ok_or_else(failed)?)
            } else {
                value.to_string()
            }
        }
        Value::Array(_) | Value::Object(_) => {
            crate::python_action::collection_text(value, 2 * 1024 * 1024).ok_or_else(failed)?
        }
    };
    Err(create_field_error(
        name,
        &format!("\"{text}\" is not a valid choice."),
    ))
}
/// Pinned SystemNotificationSerializer JSON write fields; readonly/unknown
/// inputs never become SQL columns. Return field errors for uniqueness to join.
type CreateValidation = (Map<String, Value>, Map<String, Value>);
fn create_fields(input: &Map<String, Value>) -> Result<CreateValidation, StoreError> {
    let mut fields = Map::new();
    let mut errors = Map::new();
    for (name, max) in [
        ("notification_key", Some(255)),
        ("title", Some(255)),
        ("message", None),
    ] {
        if let Some(value) = input.get(name) {
            create_field(
                &mut fields,
                &mut errors,
                name,
                crate::admin_writes::character(name, value, max, false).map(Value::String),
            )?;
        } else {
            errors.insert(name.into(), json!(["This field is required."]));
        }
    }
    for (name, default, choices) in [
        (
            "notification_type",
            "info",
            &[
                "version_update",
                "setting_recommendation",
                "announcement",
                "warning",
                "info",
            ][..],
        ),
        (
            "priority",
            "normal",
            &["low", "normal", "high", "critical"][..],
        ),
        ("source", "system", &["system", "developer"][..]),
    ] {
        create_field(
            &mut fields,
            &mut errors,
            name,
            input
                .get(name)
                .map_or_else(|| Ok(json!(default)), |v| create_choice(name, v, choices)),
        )?;
    }
    for (name, default) in [("is_active", true), ("admin_only", false)] {
        create_field(
            &mut fields,
            &mut errors,
            name,
            input.get(name).map_or_else(
                || Ok(json!(default)),
                |v| crate::admin_writes::boolean(name, v).map(Value::Bool),
            ),
        )?;
    }
    create_field(
        &mut fields,
        &mut errors,
        "action_data",
        match input.get("action_data") {
            Some(Value::Null) => Err(create_field_error(
                "action_data",
                "This field may not be null.",
            )),
            Some(value) => Ok(value.clone()),
            None => Ok(json!({})),
        },
    )?;
    create_field(
        &mut fields,
        &mut errors,
        "expires_at",
        match input.get("expires_at") {
            Some(value) => crate::admin_writes::parse_datetime("expires_at", value, true)
                .map(|value| value.map_or(Value::Null, date)),
            None => Ok(Value::Null),
        },
    )?;
    Ok((fields, errors))
}
async fn create(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Result<Json<Value>, axum::extract::rejection::JsonRejection>,
) -> Result<(axum::http::StatusCode, Json<Value>), StoreError> {
    // Source dispatch checks IsAdmin (including UI policy) before request.data.
    auth.account_user(peer, headers, true).await?;
    let input = crate::admin_writes::body_object(body)?;
    let (fields, mut errors) = create_fields(&input)?;
    if let Some(key) = fields.get("notification_key").and_then(Value::as_str) {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM core_systemnotification WHERE notification_key=$1)",
        )
        .bind(key)
        .fetch_one(auth.store.pool())
        .await?;
        if exists {
            errors.insert(
                "notification_key".into(),
                json!(["system notification with this notification key already exists."]),
            );
        }
    }
    if !errors.is_empty() {
        return Err(StoreError::InvalidInput(Value::Object(errors)));
    }
    let expires = fields["expires_at"]
        .as_str()
        .map(DateTime::parse_from_rfc3339)
        .transpose()
        .map_err(|_| failed())?
        .map(|v| v.with_timezone(&Utc));
    // Django supplies model defaults and auto_now[_add], not SQL DEFAULTs.
    // These timestamps belong to this request, never an older transaction.
    let created_at = Utc::now();
    let updated_at = Utc::now();
    let row = sqlx::query("INSERT INTO core_systemnotification(notification_key,notification_type,priority,source,title,message,action_data,is_active,admin_only,expires_at,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING *,id::bigint AS object_id,false AS dismissed")
        .bind(fields["notification_key"].as_str().expect("validated key"))
        .bind(fields["notification_type"].as_str().expect("validated type"))
        .bind(fields["priority"].as_str().expect("validated priority"))
        .bind(fields["source"].as_str().expect("validated source"))
        .bind(fields["title"].as_str().expect("validated title"))
        .bind(fields["message"].as_str().expect("validated message"))
        .bind(&fields["action_data"])
        .bind(fields["is_active"].as_bool().expect("validated active"))
        .bind(fields["admin_only"].as_bool().expect("validated admin_only"))
        .bind(expires).bind(created_at).bind(updated_at)
        .fetch_one(auth.store.pool()).await.map_err(|error| {
            // A real race after UniqueValidator's preflight can still fail in
            // Source ModelSerializer.create. Do not overwrite or retry it.
            if matches!(&error, sqlx::Error::Database(e) if e.code().as_deref()==Some("23505") && e.constraint()==Some("core_systemnotification_notification_key_key")) {
                failed()
            } else { error.into() }
        })?;
    // Source create serializes directly, even inactive/expired/developer rows.
    Ok((axum::http::StatusCode::CREATED, Json(serialize(row)?)))
}
async fn guarded_detail_write(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(raw): Path<String>,
    headers: HeaderMap,
    method: axum::http::Method,
    body: Result<Json<Value>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<Value>, StoreError> {
    let user = auth.account_user(peer, headers, true).await?;
    let id = key(raw)?;
    if visible(&auth, &user, Some(id)).await?.is_empty() {
        return Err(missing());
    }
    if method != axum::http::Method::DELETE {
        crate::admin_writes::body_object(body)?;
    }
    Err(StoreError::Unsupported(
        "Administrative notification production and scheduling have not been ported.",
    ))
}
pub(crate) fn router(auth: AuthService) -> Router {
    Router::new()
        .route("/api/core/notifications/", get(list).post(create))
        .route("/api/core/notifications/count/", get(count))
        .route("/api/core/notifications/dismiss-all/", post(dismiss_all))
        .route(
            "/api/core/notifications/{id}/",
            get(detail)
                .put(guarded_detail_write)
                .patch(guarded_detail_write)
                .delete(guarded_detail_write),
        )
        .route("/api/core/notifications/{id}/dismiss/", post(dismiss))
        .with_state(auth)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn create_uses_model_defaults_and_ignores_readonly_input() {
        let (fields, errors) = create_fields(json!({"notification_key":" key ","title":" Hello ","message":" World ","id":999,"created_at":"invalid","is_dismissed":true,"updated_at":"invalid","unknown":"ignored"}).as_object().unwrap()).unwrap();
        assert!(errors.is_empty());
        assert_eq!(
            Value::Object(fields),
            json!({"notification_key":"key","title":"Hello","message":"World","notification_type":"info","priority":"normal","source":"system","action_data":{},"is_active":true,"admin_only":false,"expires_at":null})
        );
    }
    #[test]
    fn create_collects_required_and_independent_field_errors() {
        let (_, errors) = create_fields(json!({"title":" ","message":null,"priority":"NORMAL","is_active":null,"action_data":null}).as_object().unwrap()).unwrap();
        assert_eq!(
            Value::Object(errors),
            json!({"notification_key":["This field is required."],"title":["This field may not be blank."],"message":["This field may not be null."],"priority":["\"NORMAL\" is not a valid choice."],"is_active":["This field may not be null."],"action_data":["This field may not be null."]})
        );
    }
    #[test]
    fn create_preserves_json_choices_boolean_and_nullable_expiry_fields() {
        for action in [
            json!([]),
            json!([1, true]),
            json!(false),
            json!("scalar"),
            json!(17),
        ] {
            let (fields, errors) = create_fields(json!({"notification_key":12,"title":34,"message":56,"source":"developer","priority":"high","notification_type":"warning","action_data":action,"is_active":"off","admin_only":"1","expires_at":"2026-11-12T14:00:00+01:00"}).as_object().unwrap()).unwrap();
            assert!(errors.is_empty());
            assert_eq!(fields["notification_key"], "12");
            assert_eq!(fields["title"], "34");
            assert_eq!(fields["message"], "56");
            assert_eq!(fields["action_data"], action);
            assert_eq!(fields["source"], "developer");
            assert_eq!(fields["is_active"], false);
            assert_eq!(fields["admin_only"], true);
            assert_eq!(fields["expires_at"], "2026-11-12T13:00:00Z");
        }
    }
    #[test]
    fn create_character_limits_count_unicode_and_keep_message_unbounded() {
        let (fields, errors) = create_fields(json!({"notification_key":"東".repeat(255),"title":"京".repeat(255),"message":"x".repeat(4096)}).as_object().unwrap()).unwrap();
        assert!(errors.is_empty());
        assert_eq!(fields["message"].as_str().unwrap().len(), 4096);
        let (_, errors) = create_fields(json!({"notification_key":"東".repeat(256),"title":true,"message":"a\u{0000}b","expires_at":"bad"}).as_object().unwrap()).unwrap();
        assert_eq!(errors.len(), 4);
        assert!(errors.contains_key("notification_key"));
        assert!(errors.contains_key("title"));
        assert!(errors.contains_key("message"));
        assert!(errors.contains_key("expires_at"));
    }
    #[test]
    fn known_condition_uses_literal_network_membership() {
        assert!(network_insecure(&json!({"M3U_EPG":"127.0.0.1, ::/0 "})));
        assert!(!network_insecure(&json!({"M3U_EPG":["::/0"]})));
        assert!(!network_insecure(&json!({"M3U_EPG":"0.0.0.0/1"})));
    }
    #[test]
    fn conditions_preserve_source_iterable_and_hashability_rules() {
        assert_eq!(
            condition_items(&json!({"one":false})).unwrap(),
            vec![json!("one")]
        );
        assert!(condition_items(&json!(true)).is_err());
        assert_eq!(decode_cache(b"0"), Some(json!(0)));
        assert_eq!(decode_cache(&[0x80, 4, 0x89, b'.']), Some(json!(false)));
    }
    #[tokio::test]
    #[ignore = "requires isolated REDIS_TEST_URL and original Python cache fixtures"]
    async fn redis_cache_interoperates_with_original_python_and_bounds_values() {
        let url =
            std::env::var("REDIS_TEST_URL").expect("REDIS_TEST_URL must be supplied explicitly");
        let prefix =
            std::env::var("NOTIFICATION_CACHE_TEST_PREFIX").expect("owned fixture prefix required");
        assert!(prefix.starts_with("rust-probe-notification-cache-"));
        let cache = NotificationCache::connect(&url).await.unwrap();
        let key = |suffix: &str| format!(":1:{prefix}:{suffix}");
        let expected = serde_json::json!({"M3U_EPG":"::/0","nested":[true,null,1.5]});
        for (suffix, value) in [
            ("python-false", serde_json::json!(false)),
            ("python-dict", expected.clone()),
        ] {
            let raw = cache
                .get(&key(suffix))
                .await
                .unwrap()
                .expect("Python fixture must exist");
            assert_eq!(decode_cache(&raw), Some(value));
        }
        for (suffix, value) in [
            ("rust-true", serde_json::json!(true)),
            ("rust-false", serde_json::json!(false)),
            ("rust-dict", expected),
        ] {
            cache.set(&key(suffix), &value).await.unwrap();
            let raw = cache.get(&key(suffix)).await.unwrap().unwrap();
            assert_eq!(decode_cache(&raw), Some(value));
            let mut cmd = redis::cmd("TTL");
            cmd.arg(key(suffix));
            let ttl: i64 = cache.command(cmd).await.unwrap();
            assert!((1..=300).contains(&ttl));
        }
        // A real Redis version bump while the loader is held must prevent
        // stale fill, then a stable version permits the original cache shape.
        const GROUP: &str = ":1:coresettings:group:network_access";
        const VERSION: &str = ":1:coresettings:groupver:network_access";
        let mut cmd = redis::cmd("DEL");
        cmd.arg(GROUP).arg(VERSION);
        let _: i64 = cache.command(cmd).await.unwrap();
        let mut cmd = redis::cmd("SET");
        cmd.arg(VERSION).arg(1);
        let _: () = cache.command(cmd).await.unwrap();
        let started = tokio::sync::Notify::new();
        let changed = tokio::sync::Notify::new();
        let (settings, ()) = tokio::join!(
            cache.network_settings(|| async {
                started.notify_one();
                changed.notified().await;
                Ok(serde_json::json!({"M3U_EPG":"::/0"}))
            }),
            async {
                started.notified().await;
                let mut cmd = redis::cmd("SET");
                cmd.arg(VERSION).arg(2);
                let _: () = cache.command(cmd).await.unwrap();
                changed.notify_one();
            }
        );
        assert_eq!(settings.unwrap(), serde_json::json!({"M3U_EPG":"::/0"}));
        assert!(cache.get(GROUP).await.unwrap().is_none());
        cache
            .network_settings(|| async { Ok(serde_json::json!({"M3U_EPG":"::/0"})) })
            .await
            .unwrap();
        assert_eq!(
            cache
                .network_settings(|| async { panic!("cached group must avoid loader") })
                .await
                .unwrap(),
            serde_json::json!({"M3U_EPG":"::/0"})
        );
        let mut cmd = redis::cmd("SET");
        cmd.arg(key("oversized")).arg(vec![0u8; 1024 * 1024 + 1]);
        let _: () = cache.command(cmd).await.unwrap();
        assert!(cache.get(&key("oversized")).await.is_err());
    }

    // Prepared during GRID quiet; do not execute until the coordinator's
    // explicit release. These require actual paused disposable Redis replies.
    async fn scoped_fixture() -> (NotificationCache, redis::aio::MultiplexedConnection, String) {
        assert_eq!(
            std::env::var("NOTIFICATION_SCOPE_FIXTURE").as_deref(),
            Ok("playback-fixture-only")
        );
        let url = std::env::var("REDIS_TEST_URL").expect("owned Redis required");
        assert!(url.contains("playback-fixture-only"));
        let cache = NotificationCache::connect(&url).await.unwrap();
        let control = redis::Client::open(url)
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let key = format!(
            "rust-probe-notification-scope-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        (cache, control, key)
    }
    async fn pause_scope(control: &mut redis::aio::MultiplexedConnection, millis: u64) {
        let _: () = redis::cmd("CLIENT")
            .arg("PAUSE")
            .arg(millis)
            .arg("ALL")
            .query_async(control)
            .await
            .unwrap();
    }
    async fn wait_scope(cache: &NotificationCache, operations: usize) {
        tokio::time::timeout(Duration::from_secs(6), async {
            while cache.physical_operations() != operations {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    #[ignore = "actual owned Redis pause/caller cancellation and scoped admission"]
    async fn cancelled_cache_command_retains_physical_slot_and_scope() {
        let (cache, mut control, key) = scoped_fixture().await;
        let _: () = redis::cmd("SET")
            .arg(&key)
            .arg("owned")
            .arg("EX")
            .arg(60)
            .query_async(&mut control)
            .await
            .unwrap();
        let owner = Arc::new(Semaphore::new(1));
        let scoped = cache
            .clone()
            .with_retained_scope(Arc::new(owner.clone().acquire_owned().await.unwrap()));
        pause_scope(&mut control, 400).await;
        let task = tokio::spawn(async move { scoped.get(&key).await });
        wait_scope(&cache, 1).await;
        task.abort();
        let _ = task.await;
        assert_eq!(cache.physical_operations(), 1);
        assert_eq!(owner.available_permits(), 0);
        wait_scope(&cache, 0).await;
        assert_eq!(owner.available_permits(), 1);
    }
    #[tokio::test]
    #[ignore = "actual owned Redis delayed reply beyond logical cache deadline"]
    async fn cache_logical_timeout_retains_pending_reply_and_composed_scopes() {
        let (cache, mut control, key) = scoped_fixture().await;
        let first = Arc::new(Semaphore::new(1));
        let second = Arc::new(Semaphore::new(1));
        let scoped = cache
            .clone()
            .with_retained_scope(Arc::new(first.clone().acquire_owned().await.unwrap()))
            .with_retained_scope(Arc::new(second.clone().acquire_owned().await.unwrap()));
        pause_scope(&mut control, 4000).await;
        assert!(scoped.get(&key).await.is_err());
        drop(scoped);
        assert_eq!(cache.physical_operations(), 1);
        assert_eq!(first.available_permits(), 0);
        assert_eq!(second.available_permits(), 0);
        wait_scope(&cache, 0).await;
        assert_eq!(first.available_permits(), 1);
        assert_eq!(second.available_permits(), 1);
    }
    #[tokio::test]
    #[ignore = "actual owned Redis cancellation storm with bounded physical/queued admission"]
    async fn cache_cancelled_burst_cannot_reuse_sixteen_pending_slots() {
        let (cache, mut control, key) = scoped_fixture().await;
        pause_scope(&mut control, 1200).await;
        let mut tasks = Vec::new();
        for _ in 0..16 {
            let cache = cache.clone();
            let key = key.clone();
            tasks.push(tokio::spawn(async move { cache.get(&key).await }));
        }
        wait_scope(&cache, 16).await;
        for task in tasks {
            task.abort();
            let _ = task.await;
        }
        assert_eq!(cache.physical_operations(), 16);
        let queued = cache.queued.available_permits();
        assert_eq!(queued, 256);
        wait_scope(&cache, 0).await;
        assert_eq!(cache.queued.available_permits(), 256);
        // Admission error/type/error wire regression is a separate original
        // API/cache/UA case matrix; a resource witness does not prove it.
    }

    #[tokio::test]
    #[ignore = "actual owned Redis physical16/queue256 hard admission and cancelled waiters"]
    async fn cache_waiter_bound_and_cancelled_queue_restore_admission() {
        let (cache, mut control, key) = scoped_fixture().await;
        pause_scope(&mut control, 4000).await;
        let mut active = Vec::new();
        for _ in 0..16 {
            let cache = cache.clone();
            let key = key.clone();
            active.push(tokio::spawn(async move { cache.get(&key).await }));
        }
        wait_scope(&cache, 16).await;
        let mut queued = Vec::new();
        for _ in 0..256 {
            let cache = cache.clone();
            let key = key.clone();
            queued.push(tokio::spawn(async move { cache.get(&key).await }));
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while cache.queued.available_permits() != 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        let began = tokio::time::Instant::now();
        assert!(cache.get(&key).await.is_err());
        assert!(began.elapsed() < Duration::from_millis(500));
        assert_eq!(cache.physical_operations(), 16);
        for task in queued {
            task.abort();
            let _ = task.await;
        }
        assert_eq!(cache.queued.available_permits(), 256);
        for task in active {
            task.abort();
            let _ = task.await;
        }
        assert_eq!(cache.physical_operations(), 16);
        wait_scope(&cache, 0).await;
    }
    #[tokio::test]
    #[ignore = "actual owned Redis wrongtype/ACL distinguishes generic503 and UAserver500"]
    async fn cache_and_user_agent_server_error_classifications_stay_distinct() {
        let (cache, mut control, key) = scoped_fixture().await;
        let _: () = redis::cmd("RPUSH")
            .arg(&key)
            .arg("owned")
            .query_async(&mut control)
            .await
            .unwrap();
        assert!(matches!(
            cache.get(&key).await,
            Err(StoreError::StatusBody(503, _))
        ));
        let name = format!(
            "rust-probe-cache-acl-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let _: () = redis::cmd("ACL")
            .arg("SETUSER")
            .arg(&name)
            .arg("on")
            .arg(">playback-fixture-only")
            .arg("~*")
            .arg("+@all")
            .arg("-set")
            .query_async(&mut control)
            .await
            .unwrap();
        let mut url = url::Url::parse(&std::env::var("REDIS_TEST_URL").unwrap()).unwrap();
        url.set_username(&name).unwrap();
        let restricted = NotificationCache::connect(url.as_str()).await.unwrap();
        let mut command = redis::cmd("SET");
        command.arg(&key).arg("denied");
        assert!(matches!(
            restricted.command::<()>(command).await,
            Err(StoreError::StatusBody(503, _))
        ));
        assert!(matches!(
            restricted.invalidate_user_agent("owned-version").await,
            Err(StoreError::StatusBody(500, _))
        ));
        assert_eq!(restricted.physical_operations(), 0);
        drop(restricted);
        let _: () = redis::cmd("ACL")
            .arg("DELUSER")
            .arg(name)
            .query_async(&mut control)
            .await
            .unwrap();
        let _: () = redis::cmd("DEL")
            .arg(key)
            .query_async(&mut control)
            .await
            .unwrap();
    }
}

#[cfg(test)]
#[path = "notification_restart_peer.rs"]
mod notification_restart_peer;
