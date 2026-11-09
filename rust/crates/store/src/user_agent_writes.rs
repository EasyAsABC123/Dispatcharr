//! Original Dispatcharr UserAgent serializer writes and shared-cache signals.
//! Original application authors/contributors retain full credit; see ATTRIBUTION.md.
use crate::{
    admin_writes::{boolean, character},
    AuthService, StoreError,
};
use axum::{
    extract::{ConnectInfo, Path, Request, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Map, Value};
use std::{
    net::SocketAddr,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::OwnedSemaphorePermit;
fn missing() -> StoreError {
    StoreError::StatusBody(
        404,
        json!({"detail":"No UserAgent matches the given query."}),
    )
}
fn unavailable() -> StoreError {
    StoreError::StatusBody(503, json!({"detail":"User-Agent service unavailable."}))
}
async fn existing(auth: &AuthService, raw: &str) -> Result<Value, StoreError> {
    let normalized =
        dispatcharr_ingest_core::source_integer_literal(raw).ok_or(StoreError::NotFound)?;
    let id = normalized.parse::<i64>().map_err(|_| missing())?;
    crate::core_reads::user_agent_value(auth, id).await
}
fn reserve(auth: &AuthService) -> Result<OwnedSemaphorePermit, StoreError> {
    auth.user_agent_workers
        .clone()
        .try_acquire_owned()
        .map_err(|_| unavailable())
}
async fn validate(
    auth: &AuthService,
    mut body: Map<String, Value>,
    existing: Option<i64>,
    partial: bool,
    html: bool,
) -> Result<Map<String, Value>, StoreError> {
    if html && !partial && !body.contains_key("is_active") {
        body.insert("is_active".into(), json!(false));
    }
    let mut values = Map::new();
    let mut errors = Map::new();
    for key in ["name", "user_agent", "description", "is_active"] {
        let Some(value) = body.get(key) else {
            if !partial && ["name", "user_agent"].contains(&key) {
                errors.insert(key.into(), json!(["This field is required."]));
            }
            continue;
        };
        if html && key == "is_active" && value.as_str() == Some("") {
            continue;
        }
        let parsed = if key == "is_active" {
            boolean(key, value).map(Value::from)
        } else {
            character(
                key,
                value,
                Some(if key == "description" { 255 } else { 512 }),
                key == "description",
            )
            .map(Value::from)
        };
        let value = match parsed {
            Ok(v) => v,
            Err(StoreError::InvalidInput(Value::Object(fields))) => {
                errors.extend(fields);
                continue;
            }
            Err(error) => return Err(error),
        };
        if ["name", "user_agent"].contains(&key) {
            let query=format!("SELECT EXISTS(SELECT 1 FROM core_useragent WHERE {key}=$1 AND ($2::bigint IS NULL OR id<>$2))");
            let duplicate: bool = sqlx::query_scalar(&query)
                .bind(value.as_str().unwrap())
                .bind(existing)
                .fetch_one(auth.store.pool())
                .await?;
            if duplicate {
                errors.insert(
                    key.into(),
                    json!([format!(
                        "user agent with this {} already exists.",
                        key.replace('_', " ")
                    )]),
                );
                continue;
            }
        }
        values.insert(key.into(), value);
    }
    if errors.is_empty() {
        Ok(values)
    } else {
        Err(StoreError::InvalidInput(Value::Object(errors)))
    }
}
async fn invalidate(auth: &AuthService) -> Result<(), StoreError> {
    if std::env::var("DISPATCHARR_SKIP_REDIS_CACHE")
        .is_ok_and(|v| ["1", "true", "yes"].contains(&v.to_lowercase().as_str()))
    {
        return Ok(());
    }
    let Some(cache) = auth.notification_cache.as_ref() else {
        return Ok(());
    };
    let version = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| unavailable())?
        .as_nanos()
        .to_string();
    cache.invalidate_user_agent(&version).await
}
async fn run_create(
    auth: AuthService,
    body: Map<String, Value>,
    html: bool,
    _permit: OwnedSemaphorePermit,
) -> Result<Response, StoreError> {
    let values = validate(&auth, body, None, false, html).await?;
    let id:i64=sqlx::query_scalar("INSERT INTO core_useragent(name,user_agent,description,is_active,created_at,updated_at) VALUES($1,$2,$3,$4,NOW(),NOW()) RETURNING id").bind(values["name"].as_str().unwrap()).bind(values["user_agent"].as_str().unwrap()).bind(values.get("description").and_then(Value::as_str).unwrap_or("")).bind(values.get("is_active").and_then(Value::as_bool).unwrap_or(true)).fetch_one(auth.store.pool()).await?;
    invalidate(&auth).await?;
    Ok((
        StatusCode::CREATED,
        Json(crate::core_reads::user_agent_value(&auth, id).await?),
    )
        .into_response())
}
pub(crate) async fn create(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Result<Response, StoreError> {
    auth.account_user(peer, request.headers().clone(), true)
        .await?;
    let permit = reserve(&auth)?;
    let (body, html) = crate::logo_writes::mutation_fields(request).await?;
    // Model mutation + post-save invalidation outlive an abandoned HTTP caller,
    // behind four owned permits. No unbounded detached mutation tasks are spawned.
    tokio::spawn(run_create(auth, body, html, permit))
        .await
        .map_err(|_| unavailable())?
}
async fn run_update(
    auth: AuthService,
    old: Value,
    body: Map<String, Value>,
    partial: bool,
    html: bool,
    _permit: OwnedSemaphorePermit,
) -> Result<Response, StoreError> {
    let id = old["id"].as_i64().unwrap();
    let values = validate(&auth, body, Some(id), partial, html).await?;
    sqlx::query("UPDATE core_useragent SET name=$2,user_agent=$3,description=$4,is_active=$5,updated_at=NOW() WHERE id=$1").bind(id).bind(values.get("name").unwrap_or(&old["name"]).as_str().unwrap()).bind(values.get("user_agent").unwrap_or(&old["user_agent"]).as_str().unwrap()).bind(values.get("description").unwrap_or(&old["description"]).as_str().unwrap()).bind(values.get("is_active").unwrap_or(&old["is_active"]).as_bool().unwrap()).execute(auth.store.pool()).await?;
    invalidate(&auth).await?;
    Ok(Json(crate::core_reads::user_agent_value(&auth, id).await?).into_response())
}
async fn update(
    auth: AuthService,
    peer: SocketAddr,
    raw: String,
    request: Request,
    partial: bool,
) -> Result<Response, StoreError> {
    auth.account_user(peer, request.headers().clone(), true)
        .await?;
    let permit = reserve(&auth)?;
    let old = existing(&auth, &raw).await?;
    let (body, html) = crate::logo_writes::mutation_fields(request).await?;
    tokio::spawn(run_update(auth, old, body, partial, html, permit))
        .await
        .map_err(|_| unavailable())?
}
pub(crate) async fn patch(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(id): Path<String>,
    request: Request,
) -> Result<Response, StoreError> {
    update(auth, peer, id, request, true).await
}
pub(crate) async fn put(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(id): Path<String>,
    request: Request,
) -> Result<Response, StoreError> {
    update(auth, peer, id, request, false).await
}
async fn run_delete(
    auth: AuthService,
    id: i64,
    _permit: OwnedSemaphorePermit,
) -> Result<Response, StoreError> {
    let mut tx = auth.store.pool().begin().await?;
    sqlx::query("UPDATE core_streamprofile SET user_agent_id=NULL WHERE user_agent_id=$1")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE m3u_m3uaccount SET user_agent_id=NULL WHERE user_agent_id=$1")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM core_useragent WHERE id=$1")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    invalidate(&auth).await?;
    Ok((
        StatusCode::NO_CONTENT,
        [(axum::http::header::CONTENT_LENGTH, "0")],
        axum::body::Body::from_stream(futures_util::stream::empty::<
            Result<bytes::Bytes, std::convert::Infallible>,
        >()),
    )
        .into_response())
}
pub(crate) async fn delete(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(raw): Path<String>,
    headers: HeaderMap,
) -> Result<Response, StoreError> {
    auth.account_user(peer, headers, true).await?;
    let permit = reserve(&auth)?;
    let id = existing(&auth, &raw).await?["id"].as_i64().unwrap();
    tokio::spawn(run_delete(auth, id, permit))
        .await
        .map_err(|_| unavailable())?
}
