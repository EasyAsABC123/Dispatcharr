//! Loopback copied SQLite prototype. Original Dispatcharr authors retain credit.
use super::*;
use crate::{
    sqlite_identity::{LoginPersistence, SqliteIdentity},
    NotificationCache, RestReadLimits, RetainedSqliteRestReads,
};
use axum::extract::{OriginalUri, Query};
use chrono::Utc;

#[path = "sqlite_app_data.rs"]
pub(crate) mod application_data;
#[path = "sqlite_profile.rs"]
pub(crate) mod profile;
use std::{collections::HashMap, path::Path};
use tokio::sync::Semaphore;

#[derive(Clone)]
pub struct CopiedSqliteRuntime {
    identity: Arc<SqliteIdentity>,
    cache: NotificationCache,
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
            config,
            password_workers: Arc::new(Semaphore::new(4)),
            requests: Arc::new(Semaphore::new(4)),
        })
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
            .route("/api/channels/groups/", axum::routing::get(groups))
            .route("/api/channels/channels/", axum::routing::get(channels))
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

async fn channels(
    State(runtime): State<CopiedSqliteRuntime>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let read = async {
        runtime
            .allocate()?
            .channel_collection(peer, headers, &uri, params)
            .await
    }
    .await;
    let mut response = match read {
        Ok(value) => Json(value).into_response(),
        Err(error) => error.into_response(),
    };
    response
        .headers_mut()
        .insert(axum::http::header::ALLOW, crate::channels::COLLECTION_ALLOW);
    response.headers_mut().insert(
        axum::http::header::VARY,
        axum::http::HeaderValue::from_static("Accept, origin"),
    );
    response
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
