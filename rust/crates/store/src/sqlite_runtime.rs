//! Loopback copied SQLite prototype. Original Dispatcharr authors retain credit.
use super::*;
use crate::{channels::sqlite_list::ChannelProjection, rest_reads::sqlite::ChannelRead};
use crate::{
    sqlite_identity::{LoginPersistence, SqliteIdentity},
    NotificationCache, RestReadLimits, RetainedSqliteRestReads,
};
use axum::{
    extract::{OriginalUri, Path as RoutePath, Query, RawQuery},
    http::Method,
    Extension,
};
use chrono::Utc;

#[path = "sqlite_app_data.rs"]
pub(crate) mod application_data;
#[path = "sqlite_channel_logos.rs"]
pub(crate) mod channel_logos;
#[path = "sqlite_core_catalog.rs"]
pub(crate) mod core_catalog;
#[path = "sqlite_environment.rs"]
mod environment;
#[path = "sqlite_epg_collections.rs"]
pub(crate) mod epg_collections;
#[path = "sqlite_m3u_accounts.rs"]
pub(crate) mod m3u_accounts;
#[path = "sqlite_playback.rs"]
pub(crate) mod playback;
#[path = "sqlite_profile.rs"]
pub(crate) mod profile;
#[path = "sqlite_single_from_stream.rs"]
pub(crate) mod single_from_stream;
#[path = "sqlite_stream_filter_options.rs"]
pub(crate) mod stream_filter_options;
#[path = "sqlite_streams.rs"]
pub(crate) mod streams;
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
    waiting_reads: Arc<Semaphore>,
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
            waiting_reads: Arc::new(Semaphore::new(16)),
        })
    }

    pub fn with_ip_event_sink(
        mut self,
        sink: Arc<dyn dispatcharr_contracts::UpdateEventSink>,
    ) -> Self {
        self.ip_event_sink = Some(sink);
        self
    }

    pub fn websocket_identity(&self) -> crate::realtime::CopiedWebsocketIdentity {
        crate::realtime::CopiedWebsocketIdentity {
            owner: Arc::downgrade(&self.identity),
            config: self.config.clone(),
            cache: self.cache.clone(),
        }
    }

    pub fn router(&self) -> Router {
        Router::new()
            .merge(application_data::router())
            .merge(single_from_stream::router())
            .route(
                "/api/accounts/token/",
                post(token).layer(axum::middleware::map_response(direct_token_allow)),
            )
            .route("/api/accounts/token/refresh/", axum::routing::any(refresh))
            .route(
                "/api/accounts/initialize-superuser/",
                axum::routing::get(setup_status).post(setup_status),
            )
            .route("/api/accounts/users/", axum::routing::get(profile::users))
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
            .route(
                "/api/channels/streams/",
                axum::routing::get(channel_streams).layer(Extension(streams::Projection::Page)),
            )
            .route(
                "/api/channels/streams/ids/",
                axum::routing::get(channel_streams).layer(Extension(streams::Projection::Ids)),
            )
            .route(
                "/api/channels/profiles/",
                axum::routing::get(channel_profiles),
            )
            .route(
                "/api/channels/streams/filter-options/",
                axum::routing::get(stream_filter_options),
            )
            .route("/api/channels/logos/", axum::routing::get(channel_logos))
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
                "/api/channels/channels/numbers-in-range/",
                axum::routing::get(channel_number_range),
            )
            .route(
                "/api/channels/channels/{id}/",
                axum::routing::get(channel_detail),
            )
            .route(
                "/api/core/notifications/",
                axum::routing::get(notifications),
            )
            .route(
                "/api/core/notifications/count/",
                axum::routing::get(notification_count),
            )
            .route(
                "/api/core/notifications/{id}/dismiss/",
                axum::routing::post(notification_dismiss),
            )
            .route(
                "/api/core/notifications/dismiss-all/",
                axum::routing::post(notification_dismiss_all),
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
            .route(
                "/api/core/useragents/",
                axum::routing::get(catalog).layer(Extension(core_catalog::Catalog::UserAgents)),
            )
            .route(
                "/api/core/streamprofiles/",
                axum::routing::get(catalog).layer(Extension(core_catalog::Catalog::StreamProfiles)),
            )
            .route(
                "/api/core/outputprofiles/",
                axum::routing::get(catalog).layer(Extension(core_catalog::Catalog::OutputProfiles)),
            )
            .route(
                "/api/m3u/server-groups/",
                axum::routing::get(catalog).layer(Extension(core_catalog::Catalog::ServerGroups)),
            )
            .route(
                "/api/epg/sources/",
                axum::routing::get(epg_collection)
                    .layer(Extension(epg_collections::Collection::Sources)),
            )
            .route(
                "/api/epg/epgdata/",
                axum::routing::get(epg_collection)
                    .layer(Extension(epg_collections::Collection::Data)),
            )
            .route("/api/m3u/accounts/", axum::routing::get(provider_accounts))
            .route("/healthz", axum::routing::get(readiness))
            .route("/health/ready", axum::routing::get(ready_for_requests))
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
        self.read_with_permit(permit)
    }

    async fn allocate_read(&self) -> Result<RetainedSqliteRestReads, StoreError> {
        let permit = acquire_read_permit(&self.requests, &self.waiting_reads).await?;
        self.read_with_permit(permit)
    }

    fn read_with_permit(
        &self,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<RetainedSqliteRestReads, StoreError> {
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

async fn acquire_read_permit(
    active: &Arc<Semaphore>,
    waiting: &Semaphore,
) -> Result<tokio::sync::OwnedSemaphorePermit, StoreError> {
    // The unchanged React bootstrap can issue ten reads concurrently. Keep
    // the existing active limit and bound both waiting clients and wait time.
    if let Ok(permit) = active.clone().try_acquire_owned() {
        return Ok(permit);
    }
    let _waiting = waiting
        .try_acquire()
        .map_err(|_| StoreError::RateLimited(1))?;
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        active.clone().acquire_owned(),
    )
    .await
    .map_err(|_| StoreError::RateLimited(1))?
    .map_err(|_| StoreError::RateLimited(1))
}

#[cfg(test)]
#[tokio::test]
async fn copied_reads_wait_for_capacity_and_keep_waiter_limit() {
    let active = Arc::new(Semaphore::new(4));
    let waiting = Semaphore::new(16);
    let active_requests = active.clone().acquire_many_owned(4).await.unwrap();
    let completions = futures_util::future::join_all((0..10).map(|_| async {
        let permit = acquire_read_permit(&active, &waiting).await.unwrap();
        drop(permit);
    }));
    let settle = async {
        tokio::task::yield_now().await;
        assert_eq!(waiting.available_permits(), 6);
        drop(active_requests);
    };
    tokio::join!(completions, settle);
    assert_eq!(waiting.available_permits(), 16);
    let _held_active = active.clone().acquire_many_owned(4).await.unwrap();
    let _held_waiting = waiting.acquire_many(16).await.unwrap();
    assert!(matches!(
        acquire_read_permit(&active, &waiting).await,
        Err(StoreError::RateLimited(1))
    ));
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
        .allocate_read()
        .await?
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

async fn channel_profiles(
    State(runtime): State<CopiedSqliteRuntime>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> Response {
    let read = async {
        let profiles = runtime
            .allocate_read()
            .await?
            .channel_profile_collection(peer, headers.clone())
            .await?;
        crate::channel_profiles::canonical_json_response(profiles, &uri, &headers)
    }
    .await;
    copied_read_headers(
        read.unwrap_or_else(IntoResponse::into_response),
        axum::http::HeaderValue::from_static("GET, POST, HEAD, OPTIONS"),
    )
}

async fn channel_streams(
    State(runtime): State<CopiedSqliteRuntime>,
    Extension(projection): Extension<streams::Projection>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let read = async {
        runtime
            .allocate_read()
            .await?
            .channel_streams(peer, headers, &uri, params, projection)
            .await
    }
    .await;
    let response = match read {
        Ok(body) => (
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            body,
        )
            .into_response(),
        Err(error) => error.into_response(),
    };
    copied_read_headers(response, projection.allow())
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

async fn channel_number_range(
    State(runtime): State<CopiedSqliteRuntime>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> Response {
    let result = async {
        let owner = runtime.allocate_read().await?;
        let value = owner.channel_number_range(peer, headers, &uri).await?;
        crate::channels::range_reply(&value)
    }
    .await;
    let response = result.unwrap_or_else(StoreError::into_response);
    copied_read_headers(
        response,
        axum::http::HeaderValue::from_static("GET, HEAD, OPTIONS"),
    )
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
        .allocate_read()
        .await?
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

fn setup_document(
    exists: bool,
    ip: &dispatcharr_contracts::SourceIpAddr,
    allowed_ip: &str,
) -> String {
    if exists {
        return crate::startup_bootstrap::CONFIGURED_SETUP_DOCUMENT.into();
    }
    let allowed = crate::startup::setup_allowed(ip.address(), allowed_ip);
    crate::startup_bootstrap::open_setup_document(ip, allowed)
}

async fn setup_status(
    State(runtime): State<CopiedSqliteRuntime>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request_value: axum::extract::Request,
) -> Result<Response, StoreError> {
    let scope = runtime.allocate_read().await?;
    let exists = scope.bootstrap_admin_exists().await?;
    let identity = request(peer, request_value.headers().clone());
    let ip = source_client_ip_for_config(&runtime.config, &identity);
    let override_ip = std::env::var("DISPATCHARR_SETUP_ALLOWED_IP").unwrap_or_default();
    if exists || request_value.method() != Method::POST {
        return Ok(setup_status_response(exists, &ip, &override_ip));
    }
    if !crate::startup::setup_allowed(ip.address(), &override_ip) {
        return Ok(crate::startup_bootstrap::denied(ip.to_string()));
    }
    let crate::startup_bootstrap::BootstrapCredentials {
        username,
        password,
        email,
    } = match crate::startup_bootstrap::parse_credentials(request_value).await {
        Ok(credentials) => credentials,
        Err(response) => return Ok(response),
    };
    let encoded = match crate::auth::hash_for_workers(&runtime.password_workers, password).await {
        Ok(encoded) => encoded,
        Err(_) => {
            return Ok(crate::startup_bootstrap::error(
                "Password exceeds bounded implementation limit or worker capacity.",
            ))
        }
    };
    match scope.bootstrap_create_admin(username, email, encoded).await {
        Ok(()) => Ok(setup_status_response(true, &ip, &override_ip)),
        Err(error) => Ok(crate::startup_bootstrap::sqlite_insert_error(error)),
    }
}

fn setup_status_response(
    exists: bool,
    ip: &dispatcharr_contracts::SourceIpAddr,
    override_ip: &str,
) -> Response {
    (
        [
            (axum::http::header::CONTENT_TYPE, "application/json"),
            (axum::http::header::VARY, "origin"),
        ],
        setup_document(exists, ip, override_ip),
    )
        .into_response()
}

#[cfg(test)]
#[test]
fn copied_setup_keeps_source_json_spacing_and_local_gate() {
    let ip = "127.0.0.1".parse().unwrap();
    assert_eq!(
        setup_document(true, &ip, ""),
        r#"{"superuser_exists": true}"#
    );
    assert_eq!(
        setup_document(false, &ip, ""),
        r#"{"superuser_exists": false, "client_ip": "127.0.0.1", "setup_allowed": true}"#
    );
    assert_eq!(
        setup_document(false, &ip, "192.0.2.1"),
        r#"{"superuser_exists": false, "client_ip": "127.0.0.1", "setup_allowed": false}"#
    );
}

#[cfg(test)]
#[tokio::test]
async fn copied_setup_status_supports_existing_administrator_without_body() {
    use axum::{body::to_bytes, http::header};

    let ip = "192.0.2.1".parse().unwrap();
    let configured = setup_status_response(true, &ip, "127.0.0.1");
    assert_eq!(configured.status(), StatusCode::OK);
    assert_eq!(
        configured.headers()[header::CONTENT_TYPE],
        "application/json"
    );
    assert_eq!(configured.headers()[header::VARY], "origin");
    assert_eq!(
        to_bytes(configured.into_body(), 64).await.unwrap().as_ref(),
        br#"{"superuser_exists": true}"#
    );

    let forbidden = crate::startup_bootstrap::denied(ip.to_string());
    assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);
    let document: Value =
        serde_json::from_slice(&to_bytes(forbidden.into_body(), 1024).await.unwrap()).unwrap();
    assert_eq!(document["setup_allowed"], false);
    assert_eq!(document["client_ip"], ip.to_string());
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
            .allocate_read()
            .await?
            .core_settings_read(peer, headers, id)
            .await
    }
    .await;
    copied_read_headers(reply.map(Json).into_response(), allow)
}

async fn catalog(
    State(runtime): State<CopiedSqliteRuntime>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Extension(catalog): Extension<core_catalog::Catalog>,
) -> Response {
    let reply = async {
        runtime
            .allocate_read()
            .await?
            .core_catalog_read(peer, headers, catalog)
            .await
    }
    .await;
    copied_read_headers(
        reply.map(Json).into_response(),
        crate::channels::COLLECTION_ALLOW,
    )
}

async fn epg_collection(
    State(runtime): State<CopiedSqliteRuntime>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Extension(collection): Extension<epg_collections::Collection>,
) -> Response {
    let reply = async {
        runtime
            .allocate_read()
            .await?
            .epg_collection(peer, headers, collection)
            .await
    }
    .await;
    let response = match reply {
        Ok(body) => (
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            body,
        )
            .into_response(),
        Err(error) => error.into_response(),
    };
    copied_read_headers(response, collection.allow())
}

async fn notifications(
    State(runtime): State<CopiedSqliteRuntime>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Response {
    let include = crate::notifications::include_dismissed(raw.as_deref());
    let read = async {
        runtime
            .allocate_read()
            .await?
            .notification_collection(peer, headers, include)
            .await
    }
    .await;
    let response = match read {
        Ok(body) => (
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            body,
        )
            .into_response(),
        Err(error) => error.into_response(),
    };
    copied_read_headers(response, crate::channels::COLLECTION_ALLOW)
}

async fn notification_count(
    State(runtime): State<CopiedSqliteRuntime>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let result = async {
        let unread = runtime
            .allocate_read()
            .await?
            .notification_count(peer, headers)
            .await?;
        Ok::<_, StoreError>(Json(json!({"unread_count": unread})))
    }
    .await;
    copied_read_headers(
        result.into_response(),
        axum::http::HeaderValue::from_static("GET, HEAD, OPTIONS"),
    )
}

async fn notification_dismiss(
    State(runtime): State<CopiedSqliteRuntime>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    RoutePath(id): RoutePath<String>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let result = async {
        runtime
            .allocate()?
            .dismiss_notification(peer, headers, id, body)
            .await
    }
    .await;
    let response = match result {
        Ok(value) => Json(value).into_response(),
        Err(error) => error.into_response(),
    };
    copied_read_headers(
        response,
        axum::http::HeaderValue::from_static("POST, OPTIONS"),
    )
}

async fn notification_dismiss_all(
    State(runtime): State<CopiedSqliteRuntime>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let result = async {
        runtime
            .allocate()?
            .dismiss_all_notifications(peer, headers)
            .await
    }
    .await;
    copied_read_headers(
        result.map(Json).into_response(),
        axum::http::HeaderValue::from_static("POST, OPTIONS"),
    )
}

async fn provider_accounts(
    State(runtime): State<CopiedSqliteRuntime>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let read = async {
        runtime
            .allocate_read()
            .await?
            .provider_accounts(peer, headers)
            .await
    }
    .await;
    let response = match read {
        Ok(body) => (
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            body,
        )
            .into_response(),
        Err(error) => error.into_response(),
    };
    copied_read_headers(response, crate::channels::COLLECTION_ALLOW)
}

async fn channel_logos(
    State(runtime): State<CopiedSqliteRuntime>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Response {
    let read = async {
        runtime
            .allocate_read()
            .await?
            .channel_logo_collection(peer, headers, &uri)
            .await
    }
    .await;
    let response = match read {
        Ok(body) => (
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            body,
        )
            .into_response(),
        Err(error) => error.into_response(),
    };
    copied_read_headers(response, crate::channels::COLLECTION_ALLOW)
}

async fn stream_filter_options(
    State(runtime): State<CopiedSqliteRuntime>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Response {
    let result = async {
        runtime
            .allocate_read()
            .await?
            .stream_filter_options(peer, headers, raw.as_deref())
            .await
    }
    .await;
    let response = match result {
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

async fn readiness(State(runtime): State<CopiedSqliteRuntime>) -> (StatusCode, Json<Value>) {
    readiness_response(
        runtime
            .identity
            .setting("proxy_settings")
            .await
            .is_err()
            .then_some("database_unavailable"),
    )
}

async fn ready_for_requests(
    State(runtime): State<CopiedSqliteRuntime>,
) -> (StatusCode, Json<Value>) {
    if runtime.identity.setting("proxy_settings").await.is_err() {
        return readiness_response(Some("database_unavailable"));
    }
    let ping = runtime.cache.command::<String>(redis::cmd("PING")).await;
    readiness_response(
        (!matches!(ping, Ok(reply) if reply == "PONG")).then_some("redis_unavailable"),
    )
}

fn readiness_response(failure: Option<&'static str>) -> (StatusCode, Json<Value>) {
    let status = if failure.is_none() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(json!({"status": failure.unwrap_or("ready"),
                        "backend": "rust", "scope": "sqlite_copied_data", "parity_complete": false})),
    )
}

#[cfg(test)]
#[test]
fn copied_readiness_reports_dependency_status() {
    for (failure, expected, state) in [
        (None, StatusCode::OK, "ready"),
        (
            Some("database_unavailable"),
            StatusCode::SERVICE_UNAVAILABLE,
            "database_unavailable",
        ),
        (
            Some("redis_unavailable"),
            StatusCode::SERVICE_UNAVAILABLE,
            "redis_unavailable",
        ),
    ] {
        let (status, Json(body)) = readiness_response(failure);
        assert_eq!(status, expected);
        assert_eq!(
            body,
            json!({"status":state, "backend":"rust", "scope":"sqlite_copied_data",
                   "parity_complete":false})
        );
    }
}

#[cfg(test)]
mod channel_profile_wire_tests {
    use super::*;
    use axum::{
        body::{to_bytes, Body},
        http::{header, Method, Request},
        routing::get,
    };
    use tower::ServiceExt;

    #[tokio::test]
    async fn copied_channel_profile_response_keeps_get_and_head_headers() {
        let router = Router::new().route(
            "/api/channels/profiles/",
            get(|| async {
                let response = crate::channel_profiles::canonical_json_response(
                    json!([{"id":2,"name":"Owned","channels":[8,3]}]),
                    &axum::http::Uri::from_static("/api/channels/profiles/"),
                    &HeaderMap::new(),
                )
                .unwrap();
                copied_read_headers(
                    response,
                    axum::http::HeaderValue::from_static("GET, POST, HEAD, OPTIONS"),
                )
            }),
        );
        let expected = br#"[{"id":2,"name":"Owned","channels":[8,3]}]"#;
        for method in [Method::GET, Method::HEAD] {
            let request = Request::builder()
                .method(method.clone())
                .uri("/api/channels/profiles/")
                .body(Body::empty())
                .unwrap();
            let response = router.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response.headers()[header::ALLOW],
                "GET, POST, HEAD, OPTIONS"
            );
            assert_eq!(response.headers()[header::VARY], "Accept, origin");
            assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
            assert_eq!(
                response.headers()[header::CONTENT_LENGTH],
                expected.len().to_string().as_str()
            );
            let body = to_bytes(response.into_body(), 1024).await.unwrap();
            if method == Method::GET {
                assert_eq!(body.as_ref(), expected);
            } else {
                assert!(body.is_empty());
            }
        }
    }
}
