//! Loopback copied SQLite prototype. Original Dispatcharr authors retain credit.
use super::*;
use crate::{channels::sqlite_list::ChannelProjection, rest_reads::sqlite::ChannelRead};
use crate::{
    sqlite_identity::{LoginPersistence, SqliteIdentity},
    NotificationCache, RestReadLimits, RetainedSqliteRestReads,
};
use axum::{
    extract::{OriginalUri, Path as RoutePath, Query},
    http::Method,
    Extension,
};
use chrono::Utc;

#[path = "sqlite_app_data.rs"]
pub(crate) mod application_data;
#[path = "sqlite_environment.rs"]
mod environment;
#[path = "sqlite_profile.rs"]
pub(crate) mod profile;
use std::{collections::HashMap, path::Path};
use tokio::sync::Semaphore;

#[derive(Clone)]
pub struct CopiedSqliteRuntime {
    identity: Arc<SqliteIdentity>,
    cache: NotificationCache,
    ip_lookup: crate::ip_lookup::IpLookup,
    ip_event_sink: Option<Arc<dyn dispatcharr_contracts::UpdateEventSink>>,
    config: AuthConfig,
    password_workers: Arc<Semaphore>,
    requests: Arc<Semaphore>,
}

impl CopiedSqliteRuntime {
    pub async fn open(path: &Path, config: AuthConfig, redis: &str) -> Result<Self, &'static str> {
        let identity = SqliteIdentity::open_current_login_groups(path)
            .await
            .map_err(|_| "SQLite copied runtime database readiness failed")?;
        let cache = match NotificationCache::connect(redis).await {
            Ok(cache) => cache,
            Err(_) => {
                identity.close().await;
                return Err("SQLite copied runtime Redis readiness failed");
            }
        };
        Ok(Self {
            identity: Arc::new(identity),
            cache,
            ip_lookup: crate::ip_lookup::IpLookup::new(crate::IpLookupConfig::default()),
            ip_event_sink: None,
            config,
            password_workers: Arc::new(Semaphore::new(4)),
            requests: Arc::new(Semaphore::new(4)),
        })
    }

    pub fn with_ip_event_sink(
        mut self,
        sink: Arc<dyn dispatcharr_contracts::UpdateEventSink>,
    ) -> Self {
        self.ip_event_sink = Some(sink);
        self
    }

    pub fn router(&self) -> Router {
        Router::new()
            .merge(application_data::router())
            .route(
                "/api/accounts/token/",
                post(token).layer(axum::middleware::map_response(direct_token_allow)),
            )
            .route("/api/accounts/token/refresh/", axum::routing::any(refresh))
            .route("/api/accounts/users/me/", axum::routing::any(profile::me))
            .route(
                "/api/channels/groups/",
                axum::routing::get(groups).post(group_create),
            )
            .route(
                "/api/channels/groups/{id}/",
                axum::routing::get(group_detail)
                    .put(group_put)
                    .patch(group_patch)
                    .delete(group_delete),
            )
            .route("/api/channels/channels/", axum::routing::get(channels))
            .route(
                "/api/channels/channels/ids/",
                axum::routing::get(channel_projection).layer(Extension(ChannelProjection::Ids)),
            )
            .route(
                "/api/channels/channels/summary/",
                axum::routing::get(channel_projection).layer(Extension(ChannelProjection::Summary)),
            )
            .route(
                "/api/channels/channels/{id}/",
                axum::routing::get(channel_detail),
            )
            .route("/api/core/version/", axum::routing::any(version))
            .route("/api/core/settings/", axum::routing::get(settings))
            .route(
                "/api/core/settings/{id}/",
                axum::routing::get(settings_detail),
            )
            .route(
                "/api/core/settings/env/",
                axum::routing::get(environment::get),
            )
            .route("/healthz", axum::routing::get(readiness))
            .with_state(self.clone())
    }

    pub async fn close(self) -> Result<(), &'static str> {
        let identity =
            Arc::into_inner(self.identity).ok_or("SQLite copied runtime still owns requests")?;
        identity.close().await;
        Ok(())
    }

    fn allocate(&self) -> Result<RetainedSqliteRestReads, StoreError> {
        let permit = self
            .requests
            .clone()
            .try_acquire_owned()
            .map_err(|_| StoreError::RateLimited(1))?;
        RetainedSqliteRestReads::from_owned_request(
            &self.identity,
            self.config.clone(),
            Some(self.cache.clone()),
            &self.requests,
            Arc::new(permit),
            RestReadLimits::default(),
        )
    }

    async fn login(&self, request: &PlaybackRequest, body: Value) -> Result<TokenPair, StoreError> {
        let held = self.allocate()?;
        held.record_login_attempt(request).await?;
        let login = SqliteLogin {
            runtime: self,
            request,
        };
        if !held.login_network_allowed(request).await? {
            login
                .failure_audit(
                    body.get("username").cloned().unwrap_or(json!("unknown")),
                    "Network access denied",
                )
                .await;
            return Err(StoreError::Forbidden);
        }
        login_after_admission(&login, &self.password_workers, &self.config, body).await
    }
}

struct SqliteLogin<'a> {
    runtime: &'a CopiedSqliteRuntime,
    request: &'a PlaybackRequest,
}

impl LoginEffects for SqliteLogin<'_> {
    async fn user_by_name(&self, username: &str) -> Result<Option<User>, StoreError> {
        self.runtime.identity.user_by_username(username).await
    }

    async fn compare_and_save_hash(
        &self,
        user: &User,
        replacement: &str,
    ) -> Result<(), StoreError> {
        self.runtime
            .identity
            .compare_and_save_hash(user, replacement)
            .await?;
        Ok(())
    }

    async fn failure_audit(&self, user: Value, reason: &str) {
        let peer = source_client_ip_for_config(&self.runtime.config, self.request).to_string();
        if self
            .runtime
            .identity
            .record_login_failure(
                user,
                &peer,
                self.request.header("user-agent").unwrap_or("unknown"),
                reason,
                Utc::now(),
            )
            .await
            .is_err()
        {
            tracing::warn!("Authentication audit storage failed");
        }
    }

    async fn successful_login(
        &self,
        raw_username: &str,
        audit_user: Value,
    ) -> Result<(), StoreError> {
        let peer = source_client_ip_for_config(&self.runtime.config, self.request).to_string();
        match self
            .runtime
            .identity
            .record_login_success(
                raw_username,
                audit_user,
                &peer,
                self.request.header("user-agent").unwrap_or("unknown"),
                Utc::now,
            )
            .await?
        {
            LoginPersistence::Saved => Ok(()),
            _ => Err(StoreError::Unsupported(
                "SQLite copied runtime login persistence did not settle",
            )),
        }
    }
}

async fn token(
    State(runtime): State<CopiedSqliteRuntime>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Result<Json<Value>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let body = match direct_token_json(body) {
        Ok(body) => body,
        Err(response) => return response.into_response(),
    };
    direct_token_login_response(runtime.login(&request(peer, headers), body).await)
}

async fn refresh(
    State(runtime): State<CopiedSqliteRuntime>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    incoming: axum::extract::Request,
) -> Response {
    route_compat::dispatch_refresh(peer, incoming, |request, body| async move {
        let held = match runtime.allocate() {
            Ok(held) => held,
            Err(error) => return error.into_response(),
        };
        match held.login_network_allowed(&request).await {
            Ok(true) => {}
            Ok(false) => {
                SqliteLogin {
                    runtime: &runtime,
                    request: &request,
                }
                .failure_audit(
                    json!("token_refresh"),
                    "Network access denied (token refresh)",
                )
                .await;
                return StoreError::StatusBody(403, json!({"error":"Unauthorized"}))
                    .into_response();
            }
            Err(error) => return error.into_response(),
        }
        let token = match refresh_token_json(body) {
            Ok(token) => token,
            Err(error) => return error.into_response(),
        };
        match refresh_for_config(&runtime.config, &token, |id| {
            runtime.identity.user_by_id(id)
        })
        .await
        {
            Ok(body) => Json(body).into_response(),
            Err(error) => error.into_response(),
        }
    })
    .await
}

async fn groups(
    State(runtime): State<CopiedSqliteRuntime>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Response, StoreError> {
    runtime
        .allocate()?
        .channel_group_collection(peer, headers)
        .await
}

use crate::channel_groups::sqlite::writes::Action as GroupAction;

async fn group_detail(
    State(runtime): State<CopiedSqliteRuntime>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    RoutePath(id): RoutePath<String>,
    method: Method,
    headers: HeaderMap,
) -> Result<Response, StoreError> {
    runtime
        .allocate()?
        .channel_group_action(peer, headers, GroupAction::Detail(id, method))
        .await
}

async fn group_create(
    State(runtime): State<CopiedSqliteRuntime>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Result<Json<Value>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, StoreError> {
    runtime
        .allocate()?
        .channel_group_action(peer, headers, GroupAction::Create(body))
        .await
}

async fn group_put(
    State(runtime): State<CopiedSqliteRuntime>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    RoutePath(id): RoutePath<String>,
    headers: HeaderMap,
    body: Result<Json<Value>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, StoreError> {
    runtime
        .allocate()?
        .channel_group_action(peer, headers, GroupAction::Update(id, false, body))
        .await
}

async fn group_patch(
    State(runtime): State<CopiedSqliteRuntime>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    RoutePath(id): RoutePath<String>,
    headers: HeaderMap,
    body: Result<Json<Value>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, StoreError> {
    runtime
        .allocate()?
        .channel_group_action(peer, headers, GroupAction::Update(id, true, body))
        .await
}

async fn group_delete(
    State(runtime): State<CopiedSqliteRuntime>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    RoutePath(id): RoutePath<String>,
    headers: HeaderMap,
) -> Result<Response, StoreError> {
    runtime
        .allocate()?
        .channel_group_action(peer, headers, GroupAction::Delete(id))
        .await
}

async fn channels(
    State(runtime): State<CopiedSqliteRuntime>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let read = read_channel(
        runtime,
        peer,
        headers,
        &uri,
        params,
        ChannelRead::Collection,
    )
    .await;
    channel_read_response(read, crate::channels::COLLECTION_ALLOW)
}

async fn channel_detail(
    State(runtime): State<CopiedSqliteRuntime>,
    RoutePath(id): RoutePath<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let read = read_channel(
        runtime,
        peer,
        headers,
        &uri,
        params,
        ChannelRead::Detail(id),
    )
    .await;
    channel_read_response(read, crate::channels::DETAIL_ALLOW)
}

async fn channel_projection(
    State(runtime): State<CopiedSqliteRuntime>,
    Extension(projection): Extension<ChannelProjection>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let read = read_channel(
        runtime,
        peer,
        headers,
        &uri,
        params,
        ChannelRead::Projection(projection),
    )
    .await
    .and_then(|value| {
        crate::json_compat::default_json_bytes(&value, usize::MAX)
            .map_err(|_| StoreError::Unsupported("Copied SQLite channel projection JSON"))
    });
    let response = match read {
        Ok(body) => (
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            body,
        )
            .into_response(),
        Err(error) => error.into_response(),
    };
    copied_read_headers(
        response,
        axum::http::HeaderValue::from_static("GET, HEAD, OPTIONS"),
    )
}

async fn read_channel(
    runtime: CopiedSqliteRuntime,
    peer: SocketAddr,
    headers: HeaderMap,
    uri: &axum::http::Uri,
    params: HashMap<String, String>,
    operation: ChannelRead,
) -> Result<Value, StoreError> {
    runtime
        .allocate()?
        .channel_read(peer, headers, uri, params, operation)
        .await
}

fn channel_read_response(
    read: Result<Value, StoreError>,
    allow: axum::http::HeaderValue,
) -> Response {
    let response = match read {
        Ok(value) => Json(value).into_response(),
        Err(error) => error.into_response(),
    };
    copied_read_headers(response, allow)
}

fn copied_read_headers(mut response: Response, allow: axum::http::HeaderValue) -> Response {
    response
        .headers_mut()
        .insert(axum::http::header::ALLOW, allow);
    response.headers_mut().insert(
        axum::http::header::VARY,
        axum::http::HeaderValue::from_static("Accept, origin"),
    );
    response
}

async fn version(
    State(runtime): State<CopiedSqliteRuntime>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    method: axum::http::Method,
    headers: HeaderMap,
) -> Response {
    let reply = async {
        if headers.contains_key(axum::http::header::AUTHORIZATION)
            || headers.contains_key("x-api-key")
        {
            runtime
                .allocate()?
                .authenticate_optional_bearer(peer, headers)
                .await?;
        }
        version_document_for_method(method).map(Json)
    }
    .await;
    copied_read_headers(
        reply.into_response(),
        axum::http::HeaderValue::from_static("GET, OPTIONS"),
    )
}

fn version_document_for_method(method: axum::http::Method) -> Result<Value, StoreError> {
    match method {
        axum::http::Method::GET => Ok(crate::startup::version_document()),
        axum::http::Method::OPTIONS => Ok(json!({
            "name": "Version",
            "description": "",
            "renders": ["application/json", "text/html"],
            "parses": ["application/json", "application/x-www-form-urlencoded", "multipart/form-data"]
        })),
        _ => Err(StoreError::StatusBody(
            405,
            json!({
                "detail": format!("Method \"{method}\" not allowed.")
            }),
        )),
    }
}

#[cfg(test)]
#[test]
fn copied_version_preserves_source_head_refusal_and_options_shape() {
    let method = axum::http::Method::HEAD;
    let Err(StoreError::StatusBody(405, head)) = version_document_for_method(method) else {
        panic!("copied Version HEAD must return the Source method refusal");
    };
    assert_eq!(head, json!({"detail": "Method \"HEAD\" not allowed."}));
    assert_eq!(serde_json::to_vec(&head).unwrap().len(), 41);
    let options = version_document_for_method(axum::http::Method::OPTIONS).unwrap();
    assert_eq!(
        serde_json::to_vec(&options).unwrap(),
        br#"{"name":"Version","description":"","renders":["application/json","text/html"],"parses":["application/json","application/x-www-form-urlencoded","multipart/form-data"]}"#,
    );
}

async fn settings(
    State(runtime): State<CopiedSqliteRuntime>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    settings_reply(
        runtime,
        peer,
        headers,
        None,
        crate::channels::COLLECTION_ALLOW,
    )
    .await
}

async fn settings_detail(
    State(runtime): State<CopiedSqliteRuntime>,
    RoutePath(id): RoutePath<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    settings_reply(
        runtime,
        peer,
        headers,
        Some(id),
        crate::channels::DETAIL_ALLOW,
    )
    .await
}

async fn settings_reply(
    runtime: CopiedSqliteRuntime,
    peer: SocketAddr,
    headers: HeaderMap,
    id: Option<String>,
    allow: axum::http::HeaderValue,
) -> Response {
    let reply = async {
        runtime
            .allocate()?
            .core_settings_read(peer, headers, id)
            .await
    }
    .await;
    copied_read_headers(reply.map(Json).into_response(), allow)
}

async fn readiness(State(runtime): State<CopiedSqliteRuntime>) -> (StatusCode, Json<Value>) {
    let ready = runtime.identity.setting("proxy_settings").await.is_ok();
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(
            json!({"status": if ready {"ready"} else {"database_unavailable"},
                        "backend": "rust", "scope": "sqlite_copied_data", "parity_complete": false}),
        ),
    )
}
