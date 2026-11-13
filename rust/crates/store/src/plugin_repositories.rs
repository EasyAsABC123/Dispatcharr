//! Inert bounded PluginRepo metadata adapter from retained Original Source24.
//! Dispatcharr authors retain application/algorithm credit; see ATTRIBUTION.md.
//! No manifest fetch, discovery, installation, execution or Main activation.
use crate::{
    admin_writes::{boolean, character},
    AuthService, StoreError,
};
use axum::{
    body::{to_bytes, Bytes},
    extract::{ConnectInfo, Request, State},
    http::{Method, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Json, Router,
};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use sqlx::{Connection, Row};
use std::{net::SocketAddr, sync::Arc};

const RAW_LIMIT: usize = 65536;
const RESPONSE_LIMIT: usize = 1024 * 1024;
struct RetainedFrame {
    bytes: Vec<u8>,
    _permit: Arc<tokio::sync::OwnedSemaphorePermit>,
}
impl AsRef<[u8]> for RetainedFrame {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
async fn retained_response(
    result: Result<Response, StoreError>,
    permit: Arc<tokio::sync::OwnedSemaphorePermit>,
) -> Response {
    let response = match result {
        Ok(response) => response,
        Err(error) => error.into_response(),
    };
    let (mut parts, body) = response.into_parts();
    let bytes = match to_bytes(body, RESPONSE_LIMIT).await {
        Ok(bytes) => bytes.to_vec(),
        Err(_) => {
            parts.status = StatusCode::SERVICE_UNAVAILABLE;
            parts.headers.clear();
            parts.headers.insert(
                axum::http::header::CONTENT_TYPE,
                axum::http::HeaderValue::from_static("application/json"),
            );
            b"{\"detail\":\"Plugin repository response capacity unavailable.\"}".to_vec()
        }
    };
    parts.headers.insert(
        axum::http::header::CONTENT_LENGTH,
        axum::http::HeaderValue::from_str(&bytes.len().to_string())
            .expect("bounded decimal length"),
    );
    let bytes = Bytes::from_owner(RetainedFrame {
        bytes,
        _permit: permit,
    });
    Response::from_parts(parts, axum::body::Body::from(bytes))
}

const ROW_LIMIT: usize = 256;
// Bound the transferred text in the SAME statement as the selected value.
const LIST_SQL: &str = "WITH bounded AS (SELECT r.* FROM plugins_pluginrepo r ORDER BY is_official DESC,name LIMIT 257), payload AS (SELECT count(*) AS rows,COALESCE(jsonb_agg(to_jsonb(bounded) ORDER BY is_official DESC,name),'[]'::jsonb)::text AS raw FROM bounded) SELECT rows,octet_length(raw) AS bytes,CASE WHEN rows<=256 AND octet_length(raw)<=65536 THEN raw END AS raw FROM payload";
const TARGET_SQL: &str = "WITH payload AS (SELECT to_jsonb(r)::text AS raw FROM plugins_pluginrepo r WHERE id=$1) SELECT octet_length(raw) AS bytes,CASE WHEN octet_length(raw)<=65536 THEN raw END AS raw FROM payload";
const UPDATE_SQL: &str = "UPDATE plugins_pluginrepo AS r SET public_key=COALESCE($2,public_key),enabled=COALESCE($3,enabled),updated_at=$4 WHERE id=$1 AND octet_length((to_jsonb(r)||jsonb_build_object('public_key',COALESCE($2,public_key),'enabled',COALESCE($3,enabled),'updated_at',$4))::text)<=65536 RETURNING octet_length(to_jsonb(r)::text) AS bytes,CASE WHEN octet_length(to_jsonb(r)::text)<=65536 THEN to_jsonb(r)::text END AS raw";

#[derive(Clone)]
pub struct PluginRepositories {
    auth: AuthService,
}
fn capacity() -> StoreError {
    StoreError::StatusBody(
        503,
        json!({"detail":"Plugin repository metadata capacity unavailable."}),
    )
}
fn missing() -> StoreError {
    StoreError::StatusBody(404, json!({"error":"Repo not found"}))
}
fn field(name: &str, message: &str) -> StoreError {
    StoreError::InvalidInput(json!({name:[message]}))
}
fn unsupported() -> StoreError {
    StoreError::Unsupported("Plugin manifest fetch and execution remain unqualified")
}
fn framework_shape() -> StoreError {
    StoreError::StatusBody(
        500,
        json!({"detail":"Plugin repository request processing failed."}),
    )
}
fn decoded(bytes: i32, raw: Option<String>) -> Result<Value, StoreError> {
    let raw = raw.ok_or_else(capacity)?;
    if bytes < 0 || bytes as usize > RAW_LIMIT || raw.len() != bytes as usize {
        return Err(capacity());
    }
    let (value, _quote) = crate::xc_bounded_user::checked_properties(
        &raw,
        0,
        crate::XcUserReadLimits {
            max_raw_bytes: RAW_LIMIT,
            max_nodes: 2048,
            max_depth: 32,
            work_bytes: 4 * 1024 * 1024,
        },
    )?;
    Ok(value)
}
fn payload(row: sqlx::postgres::PgRow) -> Result<Value, StoreError> {
    decoded(row.try_get("bytes")?, row.try_get("raw")?)
}
fn date(value: &Value) -> Result<Value, StoreError> {
    if value.is_null() {
        return Ok(Value::Null);
    }
    let date = DateTime::parse_from_rfc3339(value.as_str().ok_or_else(framework_shape)?)
        .map_err(|_| framework_shape())?
        .with_timezone(&Utc);
    Ok(date
        .to_rfc3339_opts(
            if date.timestamp_subsec_micros() == 0 {
                SecondsFormat::Secs
            } else {
                SecondsFormat::Micros
            },
            true,
        )
        .into())
}
fn serialized(value: &Value) -> Result<Value, StoreError> {
    let row = value.as_object().ok_or_else(framework_shape)?;
    let mut out = Map::new();
    for key in [
        "id",
        "name",
        "url",
        "is_official",
        "enabled",
        "public_key",
        "signature_verified",
        "last_fetch_status",
    ] {
        out.insert(
            key.into(),
            row.get(key).ok_or_else(framework_shape)?.clone(),
        );
    }
    for key in ["last_fetched", "created_at", "updated_at"] {
        out.insert(key.into(), date(row.get(key).ok_or_else(framework_shape)?)?);
    }
    let empty = json!({});
    let cached = row
        .get("cached_manifest")
        .filter(|v| crate::channels::truthy(Some(v)))
        .unwrap_or(&empty);
    let manifest = cached.get("manifest").unwrap_or(cached);
    if !manifest.is_object() {
        return Err(framework_shape());
    }
    out.insert(
        "registry_url".into(),
        manifest
            .get("registry_url")
            .filter(|v| crate::channels::truthy(Some(v)))
            .cloned()
            .unwrap_or_else(|| json!("")),
    );
    out.insert(
        "plugin_count".into(),
        json!(manifest
            .get("plugins")
            .and_then(Value::as_array)
            .map_or(0, Vec::len)),
    );
    Ok(Value::Object(out))
}
#[derive(Debug, PartialEq)]
struct Changes {
    public_key: Option<String>,
    enabled: Option<bool>,
}
fn changes(value: &Value) -> Result<Changes, StoreError> {
    // Original PUT calls request.data.items() before serializer validation.
    // Fresh Source2 returned exact generic Django500 HTML for this error.
    // Do not alter unmeasured SQL/date/internal/POST framework_shape paths.
    let fields = value.as_object().ok_or(StoreError::UnhandledRestFault)?;
    Ok(Changes {
        public_key: fields
            .get("public_key")
            .map(|v| character("public_key", v, None, true))
            .transpose()?,
        enabled: fields
            .get("enabled")
            .map(|v| boolean("enabled", v))
            .transpose()?,
    })
}
fn parsed(raw: &[u8]) -> Result<Value, StoreError> {
    if raw.len() > RAW_LIMIT {
        return Err(capacity());
    }
    if raw.is_empty() {
        return Ok(json!({}));
    }
    // Validate syntax without allocating Value; then use the already-qualified
    // structural/work scanner BEFORE the bounded Value allocation.
    let mut de = serde_json::Deserializer::from_slice(raw);
    serde::de::IgnoredAny::deserialize(&mut de).and_then(|_|de.end()).map_err(|error|StoreError::InvalidInput(json!({"detail":format!("JSON parse error - {}",crate::source_json_error(raw,error))})))?;
    let text = std::str::from_utf8(raw).map_err(|_| framework_shape())?;
    let (value, _quote) = crate::xc_bounded_user::checked_properties(
        text,
        0,
        crate::XcUserReadLimits {
            max_raw_bytes: RAW_LIMIT,
            max_nodes: 2048,
            max_depth: 32,
            work_bytes: 4 * 1024 * 1024,
        },
    )?;
    Ok(value)
}
impl PluginRepositories {
    pub fn new(auth: AuthService) -> Self {
        Self { auth }
    }
    /// Separate opt-in router only. AuthService::router/Main remain unchanged.
    pub fn router(self) -> Router {
        Router::new()
            .route("/api/plugins/repos/", any(collection))
            .route("/api/plugins/repos/{id}/", any(detail))
            .with_state(self)
    }
    async fn operation(
        self,
        peer: SocketAddr,
        request: Request,
        target: Option<String>,
    ) -> Result<Response, StoreError> {
        // Use the existing actual four writer permits, not a second allowance.
        let permit = Arc::new(
            self.auth
                .user_agent_workers
                .clone()
                .try_acquire_owned()
                .map_err(|_| capacity())?,
        );
        let auth = match self.auth.with_request_writer_permit(permit.clone()) {
            Ok(auth) => auth,
            Err(error) => return Ok(retained_response(Err(error), permit).await),
        };
        // The actor survives HTTP future loss. It owns the real permit through
        // callbacks, auth, SQL Ready/error and eager connection return/close.
        let observer_permit = permit.clone();
        let actor = tokio::spawn(async move {
            let result = Self::owned(auth, peer, request, target).await;
            retained_response(result, permit).await
        });
        match actor.await {
            Ok(response) => Ok(response),
            Err(_) => Ok(retained_response(Err(capacity()), observer_permit).await),
        }
    }
    async fn owned(
        auth: AuthService,
        peer: SocketAddr,
        request: Request,
        target: Option<String>,
    ) -> Result<Response, StoreError> {
        auth.request_started_display_timezone().await;
        let method = request.method().clone();
        let headers = request.headers().clone();
        if ["authorization", "x-api-key"]
            .iter()
            .any(|n| headers.get(*n).is_some_and(|v| v.to_str().is_err()))
        {
            return Err(StoreError::Unauthorized("Invalid authentication header"));
        }
        let policy = crate::auth::request(peer, headers);
        // Explicitly opt into the retained/bounded reads using the already
        // inherited ORIGINAL token; legacy authenticate(None) remains unchanged.
        let reads = auth.retained_rest_reads(crate::RestReadLimits::default())?;
        let user = reads.authenticate(&policy).await?;
        // Same Source auth then network-before-anonymous/role ordering.
        let allowed = reads
            .ui_network_allowed(auth.client_ip(&policy), user.as_ref())
            .await?;
        let user = user.ok_or(StoreError::Unauthorized(
            "Authentication credentials were not provided.",
        ))?;
        let required = if method == Method::GET || method == Method::HEAD {
            1
        } else {
            10
        };
        if !allowed || user.user().user_level < required {
            return Err(StoreError::PermissionDenied);
        }
        // Retire bounded User/JSON before allocating repository DTOs. The
        // actor/frame still retains the SAME original request token.
        drop(user);
        drop(reads);
        let supported = if target.is_some() {
            method == Method::PUT || method == Method::DELETE
        } else {
            method == Method::GET || method == Method::POST
        };
        if !supported {
            return Ok((
                StatusCode::METHOD_NOT_ALLOWED,
                Json(json!({"detail":format!("Method \"{method}\" not allowed.")})),
            )
                .into_response());
        }
        let mut connection = auth.store.pool().acquire().await?;
        let result = Self::sql(&mut connection, request, target).await;
        // fetch_one may decode DataRow before Ready. Physical fence applies
        // even to SQL errors; no timeout/retry pretends a live connection ended.
        let ping = connection.ping().await;
        if ping.is_err() {
            connection.close().await?;
        } else {
            connection.return_to_pool().await;
        }
        let _keep_actual_auth_scope = auth;
        result
    }
    async fn sql(
        connection: &mut sqlx::pool::PoolConnection<sqlx::Postgres>,
        request: Request,
        target: Option<String>,
    ) -> Result<Response, StoreError> {
        let method = request.method().clone();
        if target.is_none() && method == Method::GET {
            let row = sqlx::query(LIST_SQL).fetch_one(&mut **connection).await?;
            let count: i64 = row.try_get("rows")?;
            if count < 0 || count as usize > ROW_LIMIT {
                return Err(capacity());
            }
            let rows = payload(row)?;
            let list = rows.as_array().ok_or_else(framework_shape)?;
            let value = list.iter().map(serialized).collect::<Result<Vec<_>, _>>()?;
            return Ok(Json(value).into_response());
        }
        let id = match target {
            Some(value) => Some(value.parse::<i64>().map_err(|_| missing())?),
            None => None,
        };
        let old = if let Some(id) = id {
            Some(payload(
                sqlx::query(TARGET_SQL)
                    .bind(id)
                    .fetch_optional(&mut **connection)
                    .await?
                    .ok_or_else(missing)?,
            )?)
        } else {
            None
        };
        if method == Method::DELETE {
            if old
                .as_ref()
                .and_then(|r| r.get("is_official"))
                .and_then(Value::as_bool)
                == Some(true)
            {
                return Err(StoreError::InvalidInput(
                    json!({"error":"Cannot delete the official repository"}),
                ));
            }
            let mut tx = connection.begin().await?;
            let deleted = async {
                sqlx::query(
                    "UPDATE plugins_pluginconfig SET source_repo_id=NULL WHERE source_repo_id=$1",
                )
                .bind(id)
                .execute(&mut *tx)
                .await?;
                sqlx::query("DELETE FROM plugins_pluginrepo WHERE id=$1")
                    .bind(id)
                    .execute(&mut *tx)
                    .await?;
                Ok::<_, sqlx::Error>(())
            }
            .await;
            match deleted {
                Ok(()) => tx.commit().await?,
                Err(error) => {
                    tx.rollback().await?;
                    return Err(error.into());
                }
            }
            return Ok(Json(json!({"success":true})).into_response());
        }
        // Lookup above intentionally precedes media type/body parsing. DELETE
        // deliberately never reads its body. Body faults cannot bypass auth.
        let content_type = request
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if !content_type.starts_with("application/json") {
            return Err(StoreError::StatusBody(
                415,
                json!({"detail":format!("Unsupported media type \"{content_type}\" in request.")}),
            ));
        }
        let raw = to_bytes(request.into_body(), RAW_LIMIT)
            .await
            .map_err(|_| capacity())?;
        let value = parsed(&raw)?;
        if method == Method::POST {
            let fields = value.as_object().ok_or_else(framework_shape)?;
            let url = fields
                .get("url")
                .ok_or_else(|| field("url", "This field is required."))?;
            let url = character("url", url, Some(200), false)?;
            let exists: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM plugins_pluginrepo WHERE url=$1)")
                    .bind(&url)
                    .fetch_one(&mut **connection)
                    .await?;
            if exists {
                return Err(field("url", "plugin repo with this url already exists."));
            }
            return Err(unsupported());
        }
        let changes = changes(&value)?;
        let row = sqlx::query(UPDATE_SQL)
            .bind(id)
            .bind(changes.public_key)
            .bind(changes.enabled)
            .bind(Utc::now())
            .fetch_optional(&mut **connection)
            .await?
            .ok_or_else(capacity)?;
        Ok(Json(serialized(&payload(row)?)?).into_response())
    }
}
async fn collection(
    State(service): State<PluginRepositories>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Result<Response, StoreError> {
    service.operation(peer, request, None).await
}
async fn detail(
    State(service): State<PluginRepositories>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    axum::extract::Path(id): axum::extract::Path<String>,
    request: Request,
) -> Result<Response, StoreError> {
    service.operation(peer, request, Some(id)).await
}
#[cfg(test)]
#[path = "plugin_repositories_tests.rs"]
mod tests;
