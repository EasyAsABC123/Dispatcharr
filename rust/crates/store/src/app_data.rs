//! Owner/admin per-app JSON storage with the original source's quota contracts.
use crate::{
    json_compat::{compact_json_bytes, normalize_input_numbers, python_equal},
    AuthService, StoreError, User,
};
use axum::{
    body::to_bytes,
    extract::{ConnectInfo, Path, Request, State},
    http::{HeaderMap, StatusCode},
    routing::get,
    Json, Router,
};
use serde_json::{json, Value};
use sqlx::Row;
use std::net::SocketAddr;

const MAX_VALUE: usize = 128 * 1024;
const MAX_TOTAL: i64 = 1024 * 1024;
const MAX_APPS: i64 = 25;
const MAX_DEPTH: usize = 8;
const MAX_CONTENT: usize = MAX_VALUE + 1024;

pub(crate) fn router(auth: AuthService) -> Router {
    Router::new()
        .route(
            "/api/accounts/users/me/app-data/",
            get(list_me).delete(clear_me),
        )
        .route(
            "/api/accounts/users/me/app-data/{key}/",
            get(get_me).put(put_me).delete(delete_me),
        )
        .route(
            "/api/accounts/users/{id}/app-data/",
            get(list_user).delete(clear_user),
        )
        .route(
            "/api/accounts/users/{id}/app-data/{key}/",
            get(get_user).put(put_user).delete(delete_user),
        )
        .with_state(auth)
}
fn app_error(status: u16, detail: &str, code: &str) -> StoreError {
    StoreError::StatusBody(status, json!({"detail":detail,"code":code}))
}
fn user_missing() -> StoreError {
    app_error(404, "User not found.", "user_not_found")
}
fn limits() -> Value {
    json!({"max_value_bytes":MAX_VALUE,"max_total_bytes":MAX_TOTAL,"max_apps":MAX_APPS,"max_depth":MAX_DEPTH,"key_min_length":8,"key_max_length":64})
}
fn validate_key(key: &str) -> Result<(), StoreError> {
    let bytes = key.as_bytes();
    if !(8..=64).contains(&bytes.len())
        || !bytes[0].is_ascii_alphanumeric()
        || !bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(byte))
    {
        return Err(app_error(400,"App data key must be 8 to 64 characters and match [A-Za-z0-9][A-Za-z0-9._-]* (must start with a letter or digit).","key_invalid"));
    }
    Ok(())
}
fn content_length(headers: &HeaderMap) -> Result<Option<usize>, StoreError> {
    let Some(value) = headers.get("content-length") else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .map_err(|_| {
            app_error(
                400,
                "Invalid Content-Length header.",
                "invalid_content_length",
            )
        })?
        .trim();
    if value.is_empty() {
        return Ok(None);
    }
    if value.starts_with('-') {
        return Err(app_error(
            400,
            "Invalid Content-Length header.",
            "invalid_content_length",
        ));
    }
    let digits = value.strip_prefix('+').unwrap_or(value);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(app_error(
            400,
            "Invalid Content-Length header.",
            "invalid_content_length",
        ));
    }
    let length = digits.parse::<u128>().unwrap_or(u128::MAX);
    if length > MAX_CONTENT as u128 {
        return Err(app_error(
            413,
            &format!("Request body exceeds maximum of {MAX_CONTENT} bytes."),
            "body_too_large",
        ));
    }
    Ok(Some(length as usize))
}
fn validate_value(value: &mut Value) -> Result<usize, StoreError> {
    if !value.is_object() {
        return Err(app_error(
            400,
            "App data value must be a JSON object",
            "value_not_object",
        ));
    }
    let mut pending = vec![(&*value, 0usize)];
    while let Some((value, depth)) = pending.pop() {
        match value {
            Value::String(value) if value.contains('\0') => {
                return Err(app_error(
                    400,
                    "App data value must not contain null characters",
                    "value_has_nul",
                ))
            }
            Value::Object(values) => {
                if depth + 1 > MAX_DEPTH {
                    return Err(app_error(
                        400,
                        &format!("App data value exceeds maximum nesting depth of {MAX_DEPTH}"),
                        "value_too_deep",
                    ));
                }
                for (key, value) in values {
                    if key.contains('\0') {
                        return Err(app_error(
                            400,
                            "App data value must not contain null characters",
                            "value_has_nul",
                        ));
                    }
                    pending.push((value, depth + 1));
                }
            }
            Value::Array(values) => {
                if depth + 1 > MAX_DEPTH {
                    return Err(app_error(
                        400,
                        &format!("App data value exceeds maximum nesting depth of {MAX_DEPTH}"),
                        "value_too_deep",
                    ));
                }
                pending.extend(values.iter().map(|value| (value, depth + 1)));
            }
            _ => {}
        }
    }
    normalize_input_numbers(value).map_err(|_| {
        app_error(
            400,
            "App data value contains non-serializable data",
            "value_not_serializable",
        )
    })?;
    let size = compact_json_bytes(value)
        .map_err(|_| {
            app_error(
                400,
                "App data value contains non-serializable data",
                "value_not_serializable",
            )
        })?
        .len();
    if size > MAX_VALUE {
        return Err(app_error(
            413,
            &format!("App data value exceeds maximum size of {MAX_VALUE} bytes"),
            "value_too_large",
        ));
    }
    Ok(size)
}
async fn target(auth: &AuthService, actor: &User, id: Option<i64>) -> Result<i64, StoreError> {
    let id = id.unwrap_or(actor.id);
    if id == actor.id {
        return Ok(id);
    }
    if actor.user_level < 10 {
        return Err(app_error(
            403,
            "Not allowed to access app data for other users.",
            "forbidden",
        ));
    }
    if auth.store.user_by_id(id).await?.is_none() {
        return Err(user_missing());
    }
    Ok(id)
}
async fn summary(auth: &AuthService, id: i64) -> Result<Value, StoreError> {
    let rows =
        sqlx::query("SELECT key,byte_size FROM accounts_userappdata WHERE user_id=$1 ORDER BY key")
            .bind(id)
            .fetch_all(auth.store.pool())
            .await?;
    let mut apps = Vec::new();
    let mut total = 0i64;
    for row in rows {
        let size = row.try_get::<i32, _>("byte_size")? as i64;
        total += size;
        apps.push(json!({"key":row.try_get::<String,_>("key")?,"bytes":size}));
    }
    Ok(json!({"limits":limits(),"total_bytes":total,"apps":apps}))
}
async fn read_value(auth: &AuthService, id: i64, key: &str) -> Result<Value, StoreError> {
    validate_key(key)?;
    sqlx::query_scalar("SELECT value FROM accounts_userappdata WHERE user_id=$1 AND key=$2")
        .bind(id)
        .bind(key)
        .fetch_optional(auth.store.pool())
        .await?
        .ok_or_else(|| app_error(404, "App data not found for this key.", "key_not_found"))
}
async fn remove_key(auth: &AuthService, id: i64, key: &str) -> Result<StatusCode, StoreError> {
    validate_key(key)?;
    sqlx::query("DELETE FROM accounts_userappdata WHERE user_id=$1 AND key=$2")
        .bind(id)
        .bind(key)
        .execute(auth.store.pool())
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
async fn clear(auth: &AuthService, id: i64) -> Result<StatusCode, StoreError> {
    sqlx::query("DELETE FROM accounts_userappdata WHERE user_id=$1")
        .bind(id)
        .execute(auth.store.pool())
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn write_request(
    auth: &AuthService,
    actor: &User,
    id: Option<i64>,
    key: &str,
    request: Request,
) -> Result<Json<Value>, StoreError> {
    // Source checks body length before target lookup but after authentication.
    let length = content_length(request.headers())?;
    let id = target(auth, actor, id).await?;
    validate_key(key)?;
    let mut value = if length.unwrap_or(0) == 0 {
        json!({})
    } else {
        let content_type = request
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("");
        if content_type.split(';').next().unwrap_or("").trim() != "application/json" {
            return Err(StoreError::StatusBody(
                415,
                json!({"detail":format!("Unsupported media type \"{content_type}\" in request.")}),
            ));
        }
        let bytes = to_bytes(request.into_body(), MAX_CONTENT)
            .await
            .map_err(|_| {
                app_error(
                    413,
                    &format!("Request body exceeds maximum of {MAX_CONTENT} bytes."),
                    "body_too_large",
                )
            })?;
        serde_json::from_slice::<Value>(&bytes).map_err(|error| {
            if error.to_string().contains("recursion limit") {
                app_error(
                    400,
                    &format!("App data value exceeds maximum nesting depth of {MAX_DEPTH}"),
                    "value_too_deep",
                )
            } else if error.to_string().contains("surrogate") {
                app_error(
                    400,
                    "App data value contains non-serializable data",
                    "value_not_serializable",
                )
            } else {
                StoreError::InvalidInput(json!({"detail":"JSON parse error"}))
            }
        })?
    };
    let size = validate_value(&mut value)?;
    let mut transaction = auth.store.pool().begin().await?;
    let exists: Option<i64> =
        sqlx::query_scalar("SELECT id::bigint FROM accounts_user WHERE id=$1 FOR NO KEY UPDATE")
            .bind(id)
            .fetch_optional(&mut *transaction)
            .await?;
    if exists.is_none() {
        return Err(user_missing());
    }
    let current = sqlx::query(
        "SELECT value,byte_size FROM accounts_userappdata WHERE user_id=$1 AND key=$2 FOR UPDATE",
    )
    .bind(id)
    .bind(key)
    .fetch_optional(&mut *transaction)
    .await?;
    if let Some(row) = &current {
        if python_equal(&row.try_get::<Value, _>("value")?, &value) {
            transaction.commit().await?;
            return Ok(Json(value));
        }
    }
    let (total,count):(i64,i64)=sqlx::query_as("SELECT COALESCE(SUM(byte_size),0)::bigint,COUNT(*)::bigint FROM accounts_userappdata WHERE user_id=$1").bind(id).fetch_one(&mut *transaction).await?;
    let existing = if let Some(row) = &current {
        row.try_get::<i32, _>("byte_size")? as i64
    } else {
        0
    };
    if current.is_none() && count >= MAX_APPS {
        return Err(app_error(
            409,
            &format!("User already has the maximum of {MAX_APPS} apps stored"),
            "max_apps",
        ));
    }
    if total - existing + size as i64 > MAX_TOTAL {
        return Err(app_error(
            413,
            &format!("App data would exceed maximum total size of {MAX_TOTAL} bytes"),
            "total_too_large",
        ));
    }
    sqlx::query("INSERT INTO accounts_userappdata(user_id,key,value,byte_size,updated_at) VALUES($1,$2,$3,$4,NOW()) ON CONFLICT(user_id,key) DO UPDATE SET value=EXCLUDED.value,byte_size=EXCLUDED.byte_size,updated_at=EXCLUDED.updated_at")
        .bind(id).bind(key).bind(&value).bind(size as i32).execute(&mut *transaction).await?;
    transaction.commit().await?;
    Ok(Json(value))
}

async fn list_me(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Json<Value>, StoreError> {
    let actor = auth.account_user(peer, headers, false).await?;
    Ok(Json(summary(&auth, actor.id).await?))
}
async fn list_user(
    State(auth): State<AuthService>,
    Path(id): Path<i64>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Json<Value>, StoreError> {
    let actor = auth.account_user(peer, headers, false).await?;
    let id = target(&auth, &actor, Some(id)).await?;
    Ok(Json(summary(&auth, id).await?))
}
async fn clear_me(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<StatusCode, StoreError> {
    let actor = auth.account_user(peer, headers, false).await?;
    clear(&auth, actor.id).await
}
async fn clear_user(
    State(auth): State<AuthService>,
    Path(id): Path<i64>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<StatusCode, StoreError> {
    let actor = auth.account_user(peer, headers, false).await?;
    let id = target(&auth, &actor, Some(id)).await?;
    clear(&auth, id).await
}
async fn get_me(
    State(auth): State<AuthService>,
    Path(key): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Json<Value>, StoreError> {
    let actor = auth.account_user(peer, headers, false).await?;
    Ok(Json(read_value(&auth, actor.id, &key).await?))
}
async fn get_user(
    State(auth): State<AuthService>,
    Path((id, key)): Path<(i64, String)>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Json<Value>, StoreError> {
    let actor = auth.account_user(peer, headers, false).await?;
    let id = target(&auth, &actor, Some(id)).await?;
    Ok(Json(read_value(&auth, id, &key).await?))
}
async fn delete_me(
    State(auth): State<AuthService>,
    Path(key): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<StatusCode, StoreError> {
    let actor = auth.account_user(peer, headers, false).await?;
    remove_key(&auth, actor.id, &key).await
}
async fn delete_user(
    State(auth): State<AuthService>,
    Path((id, key)): Path<(i64, String)>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<StatusCode, StoreError> {
    let actor = auth.account_user(peer, headers, false).await?;
    let id = target(&auth, &actor, Some(id)).await?;
    remove_key(&auth, id, &key).await
}
async fn put_me(
    State(auth): State<AuthService>,
    Path(key): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Result<Json<Value>, StoreError> {
    let actor = auth
        .account_user(peer, request.headers().clone(), false)
        .await?;
    write_request(&auth, &actor, None, &key, request).await
}
async fn put_user(
    State(auth): State<AuthService>,
    Path((id, key)): Path<(i64, String)>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Result<Json<Value>, StoreError> {
    let actor = auth
        .account_user(peer, request.headers().clone(), false)
        .await?;
    write_request(&auth, &actor, Some(id), &key, request).await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn namespaces_and_structure_follow_source_error_codes() {
        assert!(validate_key("org.example.app").is_ok());
        assert!(validate_key("short").is_err());
        assert!(validate_key("-invalid.app").is_err());
        assert!(validate_value(&mut json!([])).is_err());
        assert!(validate_value(&mut json!({"nul":"\0"})).is_err());
        let mut value = json!({});
        for _ in 0..8 {
            value = json!({"nested":value});
        }
        assert!(validate_value(&mut value).is_err());
        assert_eq!(validate_value(&mut json!({"x":"é"})).unwrap(), 10);
    }
    #[test]
    fn content_length_validation_is_bounded_and_source_shaped() {
        let mut headers = HeaderMap::new();
        assert!(content_length(&headers).unwrap().is_none());
        headers.insert("content-length", "-1".parse().unwrap());
        assert!(matches!(
            content_length(&headers),
            Err(StoreError::StatusBody(400, _))
        ));
        headers.insert(
            "content-length",
            (MAX_CONTENT + 1).to_string().parse().unwrap(),
        );
        assert!(matches!(
            content_length(&headers),
            Err(StoreError::StatusBody(413, _))
        ));
        headers.insert("content-length", "00012".parse().unwrap());
        assert_eq!(content_length(&headers).unwrap(), Some(12));
    }
    #[test]
    fn per_value_limit_uses_compact_utf8_bytes_and_python_float_format() {
        let mut exact = json!({"x":"a".repeat(MAX_VALUE-8)});
        assert_eq!(validate_value(&mut exact).unwrap(), MAX_VALUE);
        assert!(matches!(
            validate_value(&mut json!({"x":"a".repeat(MAX_VALUE-7)})),
            Err(StoreError::StatusBody(413, _))
        ));
        assert_eq!(validate_value(&mut json!({"n":1e-6})).unwrap(), 11);
    }
}
