//! Copied SQLite owner/admin app data. Original Dispatcharr authors retain credit.
use super::*;
use crate::{app_data, json_compat::python_equal};
use axum::{
    extract::{Path, Request},
    http::Method,
};
use sqlx::{types::Json as DbJson, Row};

const EXISTS: &str = "SELECT id FROM accounts_user WHERE id=?";
const SUMMARY: &str = "SELECT key,byte_size FROM accounts_userappdata WHERE user_id=? ORDER BY key";
const READ: &str = "SELECT value,byte_size FROM accounts_userappdata WHERE user_id=? AND key=?";
const USAGE: &str =
    "SELECT COALESCE(SUM(byte_size),0),COUNT(*) FROM accounts_userappdata WHERE user_id=?";
const UPSERT: &str = "INSERT INTO accounts_userappdata(user_id,key,value,byte_size,updated_at)
    VALUES(?,?,?,?,?) ON CONFLICT(user_id,key) DO UPDATE SET value=EXCLUDED.value,
    byte_size=EXCLUDED.byte_size,updated_at=EXCLUDED.updated_at";
const DELETE: &str = "DELETE FROM accounts_userappdata WHERE user_id=? AND key=?";
const CLEAR: &str = "DELETE FROM accounts_userappdata WHERE user_id=?";
pub(crate) const STATEMENTS: [&str; 7] = [EXISTS, SUMMARY, READ, USAGE, UPSERT, DELETE, CLEAR];

pub(super) fn router() -> Router<CopiedSqliteRuntime> {
    Router::new()
        .route(
            "/api/accounts/users/me/app-data/",
            axum::routing::any(list_me),
        )
        .route(
            "/api/accounts/users/me/app-data/{key}/",
            axum::routing::any(key_me),
        )
        .route(
            "/api/accounts/users/{id}/app-data/",
            axum::routing::any(list_user),
        )
        .route(
            "/api/accounts/users/{id}/app-data/{key}/",
            axum::routing::any(key_user),
        )
}

async fn list_me(
    State(runtime): State<CopiedSqliteRuntime>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Result<Response, StoreError> {
    handle(runtime, peer, request, None, None).await
}
async fn key_me(
    State(runtime): State<CopiedSqliteRuntime>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(key): Path<String>,
    request: Request,
) -> Result<Response, StoreError> {
    handle(runtime, peer, request, None, Some(key)).await
}
async fn list_user(
    State(runtime): State<CopiedSqliteRuntime>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(id): Path<i64>,
    request: Request,
) -> Result<Response, StoreError> {
    handle(runtime, peer, request, Some(id), None).await
}
async fn key_user(
    State(runtime): State<CopiedSqliteRuntime>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path((id, key)): Path<(i64, String)>,
    request: Request,
) -> Result<Response, StoreError> {
    handle(runtime, peer, request, Some(id), Some(key)).await
}
async fn handle(
    runtime: CopiedSqliteRuntime,
    peer: SocketAddr,
    request: Request,
    target: Option<i64>,
    key: Option<String>,
) -> Result<Response, StoreError> {
    let held = runtime.allocate()?;
    let actor = held
        .bearer_account_user(peer, request.headers().clone(), false)
        .await?;
    let pool = runtime.identity.pool();
    let method = request.method().clone();
    let body_length = if method == Method::PUT && key.is_some() {
        app_data::content_length(request.headers())?
    } else {
        None
    };
    let id = target.unwrap_or(actor.user().id);
    if id != actor.user().id {
        if actor.user().user_level < 10 {
            return Err(app_data::app_error(
                403,
                "Not allowed to access app data for other users.",
                "forbidden",
            ));
        }
        let exists: Option<i64> = sqlx::query_scalar(EXISTS)
            .bind(id)
            .fetch_optional(pool)
            .await?;
        if exists.is_none() {
            return Err(app_data::user_missing());
        }
    }
    if let Some(key) = &key {
        app_data::validate_key(key)?;
    }
    match (method, key) {
        (Method::GET, None) => {
            let rows: Vec<(String, i64)> = sqlx::query_as(SUMMARY).bind(id).fetch_all(pool).await?;
            let total: i64 = rows.iter().map(|(_, bytes)| bytes).sum();
            let apps: Vec<_> = rows
                .iter()
                .map(|(name, bytes)| json!({"key":name,"bytes":bytes}))
                .collect();
            Ok(
                Json(json!({"limits":app_data::limits(),"total_bytes":total,"apps":apps}))
                    .into_response(),
            )
        }
        (Method::GET, Some(key)) => {
            let data: Option<DbJson<Value>> = sqlx::query_scalar(READ)
                .bind(id)
                .bind(key)
                .fetch_optional(pool)
                .await?;
            let data = data.ok_or_else(|| {
                app_data::app_error(404, "App data not found for this key.", "key_not_found")
            })?;
            Ok(Json(data.0).into_response())
        }
        (Method::DELETE, None) => {
            sqlx::query(CLEAR).bind(id).execute(pool).await?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }
        (Method::DELETE, Some(key)) => {
            sqlx::query(DELETE).bind(id).bind(key).execute(pool).await?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }
        (Method::PUT, Some(key)) => {
            let (value, size) = app_data::payload(request, body_length).await?;
            let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
            let exists: Option<i64> = sqlx::query_scalar(EXISTS)
                .bind(id)
                .fetch_optional(&mut *tx)
                .await?;
            if exists.is_none() {
                return Err(app_data::user_missing());
            }
            let current = sqlx::query(READ)
                .bind(id)
                .bind(&key)
                .fetch_optional(&mut *tx)
                .await?;
            let old_size = if let Some(row) = &current {
                let old: DbJson<Value> = row.try_get("value")?;
                if python_equal(&old.0, &value) {
                    tx.commit().await?;
                    return Ok(Json(value).into_response());
                }
                row.try_get("byte_size")?
            } else {
                0
            };
            let (total, count): (i64, i64) =
                sqlx::query_as(USAGE).bind(id).fetch_one(&mut *tx).await?;
            app_data::verify_quota(current.is_none(), count, total, old_size, size as i64)?;
            let time = Utc::now()
                .naive_utc()
                .format("%Y-%m-%d %H:%M:%S%.6f")
                .to_string();
            sqlx::query(UPSERT)
                .bind(id)
                .bind(key)
                .bind(DbJson(&value))
                .bind(size as i64)
                .bind(time)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            Ok(Json(value).into_response())
        }
        (method, _) => Err(StoreError::StatusBody(
            405,
            json!({"detail":format!("Method \"{method}\" not allowed.")}),
        )),
    }
}
