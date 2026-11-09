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
use redis::{aio::MultiplexedConnection, FromRedisValue};
use serde_json::{json, Map, Value};
use sqlx::{postgres::PgRow, Row};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{sync::Semaphore, time::timeout};

#[derive(Clone)]
pub struct NotificationCache {
    connection: MultiplexedConnection,
    permits: Arc<Semaphore>,
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
            client.get_multiplexed_async_connection(),
        )
        .await
        .map_err(|_| unavailable())?
        .map_err(|_| unavailable())?;
        Ok(Self {
            connection,
            permits: Arc::new(Semaphore::new(16)),
        })
    }
    pub(crate) async fn command<T: FromRedisValue>(
        &self,
        cmd: redis::Cmd,
    ) -> Result<T, StoreError> {
        let _permit = timeout(Duration::from_secs(2), self.permits.acquire())
            .await
            .map_err(|_| unavailable())?
            .map_err(|_| unavailable())?;
        let mut connection = self.connection.clone();
        timeout(Duration::from_secs(2), cmd.query_async(&mut connection))
            .await
            .map_err(|_| unavailable())?
            .map_err(|_| unavailable())
    }
    pub(crate) async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        let mut cmd = redis::cmd("EVAL");
        cmd.arg("if redis.call('STRLEN',KEYS[1])>1048576 then return redis.error_reply('cache value exceeds limit') end return redis.call('GET',KEYS[1])").arg(1).arg(key);
        self.command(cmd).await
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
    let result=sqlx::query("INSERT INTO core_notificationdismissal(user_id,notification_id,dismissed_at,action_taken) VALUES($1,$2,now(),$3) ON CONFLICT(user_id,notification_id) DO UPDATE SET action_taken=CASE WHEN $4 THEN EXCLUDED.action_taken ELSE core_notificationdismissal.action_taken END").bind(user.id).bind(id).bind(text).bind(truthy(Some(action))).execute(auth.store.pool()).await;
    result?;
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
async fn guarded_write(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Json<Value>, StoreError> {
    auth.account_user(peer, headers, true).await?;
    Err(StoreError::Unsupported(
        "Administrative notification production and scheduling have not been ported.",
    ))
}
pub(crate) fn router(auth: AuthService) -> Router {
    Router::new()
        .route("/api/core/notifications/", get(list).post(guarded_write))
        .route("/api/core/notifications/count/", get(count))
        .route("/api/core/notifications/dismiss-all/", post(dismiss_all))
        .route(
            "/api/core/notifications/{id}/",
            get(detail)
                .put(guarded_write)
                .patch(guarded_write)
                .delete(guarded_write),
        )
        .route("/api/core/notifications/{id}/dismiss/", post(dismiss))
        .with_state(auth)
}
#[cfg(test)]
mod tests {
    use super::*;
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
}
