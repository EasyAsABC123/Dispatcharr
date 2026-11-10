//! Original OutputProfile management writes and ordered reference cleanup.
//! Original Dispatcharr authors retain all application credit; see ATTRIBUTION.md.
//! Actual original SQL/cache/failure probes are documented in OUTPUT-PROFILE-CONTRACTS.md.
use crate::{
    admin_writes::{boolean, character},
    AuthService, StoreError,
};
use axum::{
    extract::{ConnectInfo, Path, Request, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use redis::{aio::MultiplexedConnection, FromRedisValue};
use serde_json::{json, Map, Value};
use sqlx::{Acquire, Postgres, Transaction};
use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::timeout,
};

const SELECT:&str="SELECT json_build_object('id',id,'name',name,'command',command,'parameters',parameters,'is_active',is_active,'locked',locked) FROM core_outputprofile WHERE id=$1";
const COMMAND_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_WRITERS: usize = 4;
const MAX_COMMANDS: usize = 16;
type WriterLease = Arc<OwnedSemaphorePermit>;
#[derive(Clone)]
pub struct OutputProfileWrites {
    auth: AuthService,
    workers: Arc<Semaphore>,
    cache: Option<ProfileCache>,
}
#[derive(Clone)]
struct ProfileCache {
    connection: MultiplexedConnection,
    physical_operations: Arc<Semaphore>,
    driver: Arc<CacheDriver>,
}
struct CacheDriver {
    handle: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}
impl CacheDriver {
    async fn stop(&self) {
        let handle = { self.handle.lock().unwrap_or_else(|e| e.into_inner()).take() };
        if let Some(handle) = handle {
            handle.abort();
            let _ = handle.await;
        }
    }
}
impl Drop for CacheDriver {
    fn drop(&mut self) {
        if let Some(handle) = self
            .handle
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            handle.abort();
        }
    }
}
fn unavailable() -> StoreError {
    StoreError::StatusBody(503, json!({"detail":"Output profile service unavailable."}))
}
fn cache_error() -> StoreError {
    StoreError::StatusBody(
        500,
        json!({"detail":"Output profile cache processing failed."}),
    )
}
fn missing() -> StoreError {
    StoreError::StatusBody(
        404,
        json!({"detail":"No OutputProfile matches the given query."}),
    )
}
fn skipped() -> bool {
    std::env::var("DISPATCHARR_SKIP_REDIS_CACHE")
        .is_ok_and(|v| ["1", "true", "yes"].contains(&v.to_lowercase().as_str()))
}
fn version() -> Result<String, StoreError> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| cache_error())?
        .as_nanos()
        .to_string())
}
impl ProfileCache {
    // A timed-out observer must not release physical Redis admission while its
    // multiplexed reply is still pending. No wait queue or unbounded task spawn.
    async fn command<T: FromRedisValue + Send + 'static>(
        &self,
        cmd: redis::Cmd,
        writer: WriterLease,
    ) -> Result<Option<T>, StoreError> {
        if skipped() {
            return Ok(None);
        }
        let Ok(permit) = self.physical_operations.clone().try_acquire_owned() else {
            return Ok(None);
        };
        let mut connection = self.connection.clone();
        let task = tokio::spawn(async move {
            let _permit = permit;
            let _writer = writer;
            cmd.query_async::<T>(&mut connection).await
        });
        match timeout(COMMAND_TIMEOUT, task).await {
            Err(_) => Ok(None),
            Ok(Err(_)) => Err(cache_error()),
            Ok(Ok(Err(error))) if error.is_io_error() || error.is_timeout() => Ok(None),
            Ok(Ok(Err(_))) => Err(cache_error()),
            Ok(Ok(Ok(value))) => Ok(Some(value)),
        }
    }
    async fn get(
        &self,
        key: &str,
        writer: &WriterLease,
    ) -> Result<Option<Option<Value>>, StoreError> {
        let mut cmd = redis::cmd("EVAL");
        cmd.arg("if redis.call('STRLEN',KEYS[1])>1048576 then return redis.error_reply('cache value exceeds limit') end return redis.call('GET',KEYS[1])").arg(1).arg(key);
        let Some(raw) = self.command::<Option<Vec<u8>>>(cmd, writer.clone()).await? else {
            return Ok(None);
        };
        raw.map(|raw| crate::decode_source_cache(&raw).ok_or_else(cache_error))
            .transpose()
            .map(Some)
    }
    async fn set_group(
        &self,
        key: &str,
        value: &Value,
        writer: &WriterLease,
    ) -> Result<(), StoreError> {
        let encoded = crate::encode_source_cache(value).ok_or_else(cache_error)?;
        let mut cmd = redis::cmd("SET");
        cmd.arg(key).arg(encoded).arg("EX").arg(300);
        self.command::<()>(cmd, writer.clone()).await?;
        Ok(())
    }
    async fn invalidate_pair(
        &self,
        key: &str,
        version_key: &str,
        writer: &WriterLease,
    ) -> Result<(), StoreError> {
        let mut delete = redis::cmd("DEL");
        delete.arg(key);
        self.command::<i64>(delete, writer.clone()).await?;
        // Source continues to SET even if DEL had a connectivity failure.
        let mut bump = redis::cmd("SET");
        bump.arg(version_key).arg(version()?);
        self.command::<()>(bump, writer.clone()).await?;
        Ok(())
    }
    async fn invalidate_group(&self, key: &str, writer: &WriterLease) -> Result<(), StoreError> {
        self.invalidate_pair(
            &format!(":1:coresettings:group:{key}"),
            &format!(":1:coresettings:groupver:{key}"),
            writer,
        )
        .await?;
        if key == "stream_settings" {
            self.invalidate_pair(
                ":1:coresettings:default_user_agent",
                ":1:coresettings:default_user_agent:ver",
                writer,
            )
            .await?
        }
        Ok(())
    }
}
impl OutputProfileWrites {
    /// Independent state/router, to be merged only after original/native gates.
    /// No AuthService constructor, schema or manifest changes are needed.
    pub async fn new(auth: AuthService, redis_url: Option<&str>) -> Result<Self, StoreError> {
        let cache = if let Some(url) = redis_url {
            let client = redis::Client::open(url).map_err(|_| cache_error())?;
            let (connection, driver) = timeout(
                Duration::from_secs(5),
                client.create_multiplexed_tokio_connection(),
            )
            .await
            .map_err(|_| unavailable())?
            .map_err(|_| unavailable())?;
            Some(ProfileCache {
                connection,
                physical_operations: Arc::new(Semaphore::new(MAX_COMMANDS)),
                driver: Arc::new(CacheDriver {
                    handle: std::sync::Mutex::new(Some(tokio::spawn(driver))),
                }),
            })
        } else {
            None
        };
        Ok(Self {
            auth,
            workers: Arc::new(Semaphore::new(MAX_WRITERS)),
            cache,
        })
    }
    pub fn router(self) -> Router {
        Router::new()
            .route("/api/core/outputprofiles/", post(create))
            .route(
                "/api/core/outputprofiles/{id}/",
                axum::routing::put(put).patch(patch).delete(delete),
            )
            .with_state(self)
    }
    /// Close admission, await logical and physical ownership, then close the
    /// actual Redis driver. A deadline forces the socket closed before waiting
    /// for retained command/writer leases; false reports remaining SQL work.
    pub async fn shutdown(&self, grace: Duration) -> bool {
        self.workers.close();
        let drained = || {
            self.workers.available_permits() == MAX_WRITERS
                && self.cache.as_ref().is_none_or(|cache| {
                    cache.physical_operations.available_permits() == MAX_COMMANDS
                })
        };
        let deadline = tokio::time::Instant::now() + grace;
        while !drained() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        if let Some(cache) = &self.cache {
            cache.physical_operations.close();
            cache.driver.stop().await;
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while !drained() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        drained()
    }
    fn reserve(&self) -> Result<OwnedSemaphorePermit, StoreError> {
        self.workers
            .clone()
            .try_acquire_owned()
            .map_err(|_| unavailable())
    }
    async fn existing(&self, raw: &str) -> Result<Value, StoreError> {
        let parsed =
            dispatcharr_ingest_core::source_integer_literal(raw).ok_or(StoreError::NotFound)?;
        let id = parsed.parse::<i64>().map_err(|_| missing())?;
        sqlx::query_scalar(SELECT)
            .bind(id)
            .fetch_optional(self.auth.store.pool())
            .await?
            .ok_or_else(missing)
    }
    async fn group(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        key: &str,
        writer: &WriterLease,
    ) -> Result<Value, StoreError> {
        let Some(cache) = self.cache.as_ref() else {
            return load_group(tx, key).await;
        };
        let group_key = format!(":1:coresettings:group:{key}");
        let version_key = format!(":1:coresettings:groupver:{key}");
        let Some(cached) = cache.get(&group_key, writer).await? else {
            return load_group(tx, key).await;
        };
        if let Some(value) = cached.filter(Value::is_object) {
            return Ok(value);
        }
        let Some(before) = cache.get(&version_key, writer).await? else {
            return load_group(tx, key).await;
        };
        let value = load_group(tx, key).await?;
        let Some(after) = cache.get(&version_key, writer).await? else {
            return Ok(value);
        };
        if crate::json_compat::python_equal(
            before.as_ref().unwrap_or(&Value::Null),
            after.as_ref().unwrap_or(&Value::Null),
        ) {
            cache.set_group(&group_key, &value, writer).await?
        }
        Ok(value)
    }
    async fn invalidate(&self, key: &str, writer: &WriterLease) -> Result<(), StoreError> {
        if let Some(cache) = self.cache.as_ref() {
            cache.invalidate_group(key, writer).await?
        }
        Ok(())
    }
    async fn update_group(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        key: &str,
        name: &str,
        field: &str,
        writer: &WriterLease,
    ) -> Result<(), StoreError> {
        // Source get_or_create emits a first post_save for a missing group;
        // _update_group then emits a second save after merging its update.
        let existing: Option<Value> =
            sqlx::query_scalar("SELECT value FROM core_coresettings WHERE key=$1")
                .bind(key)
                .fetch_optional(&mut **tx)
                .await?;
        let old = if let Some(old) = existing {
            old
        } else {
            // Django get_or_create wraps creation (including post_save) in a
            // nested atomic block. A failed INSERT rolls back its savepoint,
            // leaving earlier outer delete/user mutations eligible to commit.
            let mut savepoint = tx.begin().await?;
            let inserted=sqlx::query("INSERT INTO core_coresettings(key,name,value) VALUES($1,$2,'{}'::jsonb) ON CONFLICT(key) DO NOTHING").bind(key).bind(name).execute(&mut *savepoint).await;
            let created = match inserted {
                Ok(result) => result.rows_affected() == 1,
                Err(error) => {
                    savepoint.rollback().await?;
                    return Err(error.into());
                }
            };
            if created {
                if let Err(error) = self.invalidate(key, writer).await {
                    savepoint.rollback().await?;
                    return Err(error);
                }
            }
            savepoint.commit().await?;
            sqlx::query_scalar::<_, Value>("SELECT value FROM core_coresettings WHERE key=$1")
                .bind(key)
                .fetch_one(&mut **tx)
                .await?
        };
        let mut value = old.as_object().cloned().unwrap_or_default();
        value.insert(field.into(), Value::Null);
        sqlx::query("UPDATE core_coresettings SET value=$2 WHERE key=$1")
            .bind(key)
            .bind(Value::Object(value))
            .execute(&mut **tx)
            .await?;
        self.invalidate(key, writer).await
    }
    async fn scrub(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        id: i64,
        writer: &WriterLease,
    ) -> Result<(), StoreError> {
        // Bound retained rows even for large user tables. JSONB comparisons
        // preserve PostgreSQL integer/string/numeric equality, excluding bools.
        let mut after = None::<i64>;
        loop {
            let rows:Vec<i64>=sqlx::query_scalar("SELECT id::bigint FROM accounts_user WHERE ($1::bigint IS NULL OR id>$1) AND (custom_properties->'output_profile'=to_jsonb($2::bigint) OR custom_properties->'output_profile'=to_jsonb($3::text)) ORDER BY id LIMIT 128")
                .bind(after).bind(id).bind(id.to_string()).fetch_all(&mut **tx).await?;
            if rows.is_empty() {
                break;
            }
            for user_id in rows {
                after = Some(user_id);
                // Keep unrelated potentially large JSON inside PostgreSQL.
                // Concurrent changes differ from Django's loaded-object save;
                // that family remains an explicit unwired compatibility gate.
                sqlx::query("UPDATE accounts_user SET custom_properties=custom_properties-'output_profile' WHERE id=$1 AND jsonb_typeof(custom_properties)='object'").bind(user_id).execute(&mut **tx).await?;
            }
        }
        let stream = self.group(tx, "stream_settings", writer).await?;
        if id_matches(stream.get("hdhr_output_profile_id"), id) {
            self.update_group(
                tx,
                "stream_settings",
                "Stream Settings",
                "hdhr_output_profile_id",
                writer,
            )
            .await?
        }
        let dvr = self.group(tx, "dvr_settings", writer).await?;
        if id_matches(dvr.get("output_profile_id"), id) {
            self.update_group(
                tx,
                "dvr_settings",
                "DVR Settings",
                "output_profile_id",
                writer,
            )
            .await?
        }
        Ok(())
    }
}
fn id_matches(raw: Option<&Value>, id: i64) -> bool {
    match raw {
        Some(Value::Bool(v)) => i64::from(*v) == id,
        Some(Value::Number(v)) if v.is_f64() => v.as_f64().is_some_and(|v| {
            v.is_finite() && crate::json_compat::python_equal(&json!(v.trunc()), &json!(id))
        }),
        Some(Value::Number(v)) => v.to_string() == id.to_string(),
        Some(Value::String(v)) => {
            dispatcharr_ingest_core::source_integer_literal(v).is_some_and(|v| v == id.to_string())
        }
        _ => false,
    }
}
async fn load_group(tx: &mut Transaction<'_, Postgres>, key: &str) -> Result<Value, StoreError> {
    let value: Option<Value> =
        sqlx::query_scalar("SELECT value FROM core_coresettings WHERE key=$1")
            .bind(key)
            .fetch_optional(&mut **tx)
            .await?;
    if let Some(value) = value.filter(|v| v.as_object().is_some_and(|m| !m.is_empty())) {
        return Ok(value);
    }
    Ok(if key == "stream_settings" {
        json!({"default_user_agent":null,"default_stream_profile":null,"m3u_hash_key":"","default_output_format":"mpegts","hdhr_output_profile_id":null})
    } else {
        json!({"tv_template":"TV_Shows/{show}/S{season:02d}E{episode:02d}.mkv","movie_template":"Movies/{title} ({year}).mkv","tv_fallback_dir":"TV_Shows","tv_fallback_template":"TV_Shows/{show}/{start}.mkv","movie_fallback_template":"Movies/{start}.mkv","comskip_enabled":false,"comskip_custom_path":"","comskip_mode":"cut","comskip_hw_accel":"none","pre_offset_minutes":0,"post_offset_minutes":0,"series_rules":[],"output_profile_id":null})
    })
}
async fn validate(
    service: &OutputProfileWrites,
    mut body: Map<String, Value>,
    existing: Option<i64>,
    partial: bool,
    html: bool,
) -> Result<Map<String, Value>, StoreError> {
    for field in ["is_active", "locked"] {
        if html && !partial && !body.contains_key(field) {
            body.insert(field.into(), json!(false));
        }
    }
    let mut out = Map::new();
    let mut errors = Map::new();
    for field in ["name", "command", "parameters", "is_active", "locked"] {
        let Some(value) = body.get(field) else {
            if !partial && ["name", "command", "parameters"].contains(&field) {
                errors.insert(field.into(), json!(["This field is required."]));
            }
            continue;
        };
        if html && ["is_active", "locked"].contains(&field) && value.as_str() == Some("") {
            continue;
        }
        let result = if ["is_active", "locked"].contains(&field) {
            boolean(field, value).map(Value::Bool)
        } else {
            character(
                field,
                value,
                if field == "parameters" {
                    None
                } else {
                    Some(255)
                },
                false,
            )
            .map(Value::String)
        };
        match result {
            Ok(value) => {
                if field == "name" {
                    let duplicate:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM core_outputprofile WHERE name=$1 AND($2::bigint IS NULL OR id<>$2))").bind(value.as_str().unwrap()).bind(existing).fetch_one(service.auth.store.pool()).await?;
                    if duplicate {
                        errors.insert(
                            field.into(),
                            json!(["output profile with this name already exists."]),
                        );
                        continue;
                    }
                }
                out.insert(field.into(), value);
            }
            Err(StoreError::InvalidInput(Value::Object(fields))) => errors.extend(fields),
            Err(error) => return Err(error),
        }
    }
    if errors.is_empty() {
        Ok(out)
    } else {
        Err(StoreError::InvalidInput(Value::Object(errors)))
    }
}
fn merged(old: Option<&Value>, changes: Map<String, Value>) -> Result<Value, StoreError> {
    let mut value = old.cloned().unwrap_or_else(
        || json!({"name":"","command":"","parameters":"","is_active":true,"locked":false}),
    );
    for (key, change) in changes {
        value[key] = change
    }
    if let Some(old) = old.filter(|v| v["locked"] == true) {
        for field in ["name", "command", "parameters", "locked", "is_active"] {
            if old[field] != value[field] {
                return Err(StoreError::InvalidInput(json!([format!(
                    "Cannot modify {field} on a protected profile."
                )])));
            }
        }
    }
    Ok(value)
}
async fn save(
    service: OutputProfileWrites,
    old: Option<Value>,
    body: Map<String, Value>,
    partial: bool,
    html: bool,
    _permit: OwnedSemaphorePermit,
) -> Result<Response, StoreError> {
    let id = old.as_ref().and_then(|v| v["id"].as_i64());
    let changes = validate(&service, body, id, partial, html).await?;
    let value = merged(old.as_ref(), changes)?;
    let id: i64 = if let Some(id) = id {
        sqlx::query_scalar("UPDATE core_outputprofile SET name=$2,command=$3,parameters=$4,locked=$5,is_active=$6 WHERE id=$1 RETURNING id::bigint")
            .bind(id).bind(value["name"].as_str().unwrap()).bind(value["command"].as_str().unwrap()).bind(value["parameters"].as_str().unwrap()).bind(value["locked"].as_bool().unwrap()).bind(value["is_active"].as_bool().unwrap()).fetch_one(service.auth.store.pool()).await?
    } else {
        sqlx::query_scalar("INSERT INTO core_outputprofile(name,command,parameters,locked,is_active) VALUES($1,$2,$3,$4,$5) RETURNING id::bigint")
            .bind(value["name"].as_str().unwrap()).bind(value["command"].as_str().unwrap()).bind(value["parameters"].as_str().unwrap()).bind(value["locked"].as_bool().unwrap()).bind(value["is_active"].as_bool().unwrap()).fetch_one(service.auth.store.pool()).await?
    };
    let saved: Value = sqlx::query_scalar(SELECT)
        .bind(id)
        .fetch_one(service.auth.store.pool())
        .await?;
    Ok((
        if old.is_some() {
            StatusCode::OK
        } else {
            StatusCode::CREATED
        },
        Json(saved),
    )
        .into_response())
}
async fn serializer_fields(request: Request) -> Result<(Map<String, Value>, bool), StoreError> {
    crate::logo_writes::mutation_fields(request).await.map_err(|error|match error {
        StoreError::InvalidInput(value)if value==json!({"non_field_errors":["Invalid data. Expected a dictionary, but got NoneType."]})=>StoreError::InvalidInput(json!({"non_field_errors":["No data provided"]})),
        error=>error,
    })
}
async fn create(
    State(service): State<OutputProfileWrites>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Result<Response, StoreError> {
    service
        .auth
        .account_user(peer, request.headers().clone(), true)
        .await?;
    let permit = service.reserve()?;
    let (body, html) = serializer_fields(request).await?;
    tokio::spawn(save(service, None, body, false, html, permit))
        .await
        .map_err(|_| unavailable())?
}
async fn change(
    service: OutputProfileWrites,
    peer: SocketAddr,
    raw: String,
    request: Request,
    partial: bool,
) -> Result<Response, StoreError> {
    service
        .auth
        .account_user(peer, request.headers().clone(), true)
        .await?;
    let permit = service.reserve()?;
    let old = service.existing(&raw).await?;
    let (body, html) = serializer_fields(request).await?;
    tokio::spawn(save(service, Some(old), body, partial, html, permit))
        .await
        .map_err(|_| unavailable())?
}
async fn patch(
    State(service): State<OutputProfileWrites>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(raw): Path<String>,
    request: Request,
) -> Result<Response, StoreError> {
    change(service, peer, raw, request, true).await
}
async fn put(
    State(service): State<OutputProfileWrites>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(raw): Path<String>,
    request: Request,
) -> Result<Response, StoreError> {
    change(service, peer, raw, request, false).await
}
async fn run_delete(
    service: OutputProfileWrites,
    id: i64,
    permit: OwnedSemaphorePermit,
) -> Result<Response, StoreError> {
    let writer = Arc::new(permit);
    let mut tx = service.auth.store.pool().begin().await?;
    sqlx::query("DELETE FROM core_outputprofile WHERE id=$1")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    if service.scrub(&mut tx, id, &writer).await.is_err() {
        // Original post_delete logs and swallows callback errors. A PostgreSQL
        // error can still poison the surrounding collector transaction; this
        // boundary requires actual original failure probes before acceptance.
        tracing::warn!("Output profile reference scrub failed");
    }
    tx.commit().await?;
    Ok((
        StatusCode::NO_CONTENT,
        [(axum::http::header::CONTENT_LENGTH, "0")],
        axum::body::Body::from_stream(futures_util::stream::empty::<
            Result<bytes::Bytes, std::convert::Infallible>,
        >()),
    )
        .into_response())
}
async fn delete(
    State(service): State<OutputProfileWrites>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(raw): Path<String>,
    headers: HeaderMap,
) -> Result<Response, StoreError> {
    service.auth.account_user(peer, headers, true).await?;
    let permit = service.reserve()?;
    let old = service.existing(&raw).await?;
    if old["locked"] == true {
        return Err(StoreError::InvalidInput(json!([
            "This profile is locked and cannot be deleted."
        ])));
    }
    tokio::spawn(run_delete(service, old["id"].as_i64().unwrap(), permit))
        .await
        .map_err(|_| unavailable())?
}
#[cfg(test)]
#[path = "output_profile_writes_tests.rs"]
mod tests;
