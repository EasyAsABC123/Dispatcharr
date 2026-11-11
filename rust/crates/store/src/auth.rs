use crate::{cidrs_allow, Store, StoreError, User};
use axum::{
    extract::{ConnectInfo, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use dispatcharr_contracts::{ChannelControl, PlaybackRequest};
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{HashMap, VecDeque},
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[derive(Clone)]
pub struct AuthConfig {
    pub signing_key: String,
    /// None preserves upstream's default private/loopback proxy trust.
    /// Some("none") disables all forwarded-header trust.
    pub trusted_proxies: Option<String>,
    pub access_lifetime: Duration,
    pub refresh_lifetime: Duration,
}

impl AuthConfig {
    pub fn new(signing_key: String) -> Result<Self, StoreError> {
        if signing_key.is_empty() {
            return Err(StoreError::Unsupported("JWT signing key is required"));
        }
        Ok(Self {
            signing_key,
            trusted_proxies: None,
            access_lifetime: Duration::from_secs(1800),
            refresh_lifetime: Duration::from_secs(86400),
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Claims {
    pub token_type: String,
    pub exp: u64,
    #[serde(default)]
    pub iat: u64,
    pub jti: String,
    /// SimpleJWT versions have emitted both numeric and string IDs.
    #[serde(default)]
    pub user_id: Value,
}

impl Claims {
    pub fn id(&self) -> Option<i64> {
        self.user_id
            .as_i64()
            .or_else(|| self.user_id.as_str()?.parse().ok())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TokenPair {
    pub access: String,
    pub refresh: String,
}

#[derive(Clone)]
pub struct AuthService {
    pub(crate) store: Store,
    config: AuthConfig,
    attempts: Arc<Mutex<AttemptHistory>>,
    password_workers: Arc<tokio::sync::Semaphore>,
    pub(crate) user_agent_workers: Arc<tokio::sync::Semaphore>,
    pub(crate) channel_control: Option<Arc<dyn ChannelControl>>,
    pub(crate) notification_cache: Option<crate::NotificationCache>,
    pub(crate) event_sink: Option<Arc<dyn dispatcharr_contracts::UpdateEventSink>>,
    // Request-local ownership only; global AuthService remains unscoped.
    retained_scope: Option<Arc<dyn Send + Sync>>,
    pub(crate) images: crate::image_proxy::ImageRuntime,
    pub(crate) ip_lookup: crate::ip_lookup::IpLookup,
}

type AttemptHistory = HashMap<(IpAddr, &'static str), VecDeque<Instant>>;

impl AuthService {
    pub fn new(store: Store, config: AuthConfig) -> Self {
        Self {
            store,
            config,
            attempts: Arc::new(Mutex::new(HashMap::new())),
            password_workers: Arc::new(tokio::sync::Semaphore::new(4)),
            user_agent_workers: Arc::new(tokio::sync::Semaphore::new(4)),
            channel_control: None,
            notification_cache: None,
            event_sink: None,
            retained_scope: None,
            images: crate::image_proxy::ImageRuntime::new(),
            ip_lookup: crate::ip_lookup::IpLookup::new(crate::IpLookupConfig::default()),
        }
    }

    /// Preserve ordinary constructor compatibility; media ownership remains in
    /// the application rather than coupling the SQL store to a playback engine.
    pub fn with_channel_control(mut self, control: Arc<dyn ChannelControl>) -> Self {
        self.channel_control = Some(control);
        self
    }

    pub fn with_notification_cache(mut self, cache: crate::NotificationCache) -> Self {
        self.notification_cache = Some(match self.retained_scope.as_ref() {
            Some(scope) => cache.with_retained_scope(scope.clone()),
            None => cache,
        });
        self
    }

    pub fn with_event_sink(
        mut self,
        sink: Arc<dyn dispatcharr_contracts::UpdateEventSink>,
    ) -> Self {
        self.event_sink = Some(match self.retained_scope.as_ref() {
            Some(scope) => Arc::new(RetainedAuthSink {
                inner: sink,
                guard: scope.clone(),
            }),
            None => sink,
        });
        self
    }

    /// Create on an existing per-request clone, never on the app singleton.
    /// Policy/cache/connection budgets stay shared. Retain request ownership
    /// through awaited SQL, supervised cache replies and event publication even
    /// when an inner logical timeout returns before physical I/O completion.
    /// This bridge must pass delayed-cache/resource gates before VOD app use.
    pub fn with_retained_scope(mut self, guard: Arc<dyn Send + Sync>) -> Self {
        let combined: Arc<dyn Send + Sync> = match self.retained_scope.take() {
            Some(previous) => Arc::new((previous, guard)),
            None => guard,
        };
        if let Some(cache) = self.notification_cache.take() {
            self.notification_cache = Some(cache.with_retained_scope(combined.clone()));
        }
        if let Some(sink) = self.event_sink.take() {
            self.event_sink = Some(Arc::new(RetainedAuthSink {
                inner: sink,
                guard: combined.clone(),
            }));
        }
        self.retained_scope = Some(combined);
        self
    }

    pub fn router(self) -> Router {
        let accounts = crate::accounts::router(self.clone());
        let core = crate::core_reads::router(self.clone())
            .merge(crate::core_utilities::router(self.clone()))
            .merge(crate::notifications::router(self.clone()));
        let channel_profiles = crate::channel_profiles::router(self.clone());
        let channels = crate::channels::router(self.clone());
        let channel_groups = crate::channel_groups::router(self.clone());
        let streams =
            crate::streams::router(self.clone()).merge(crate::logos::router(self.clone()));
        let startup = crate::startup::router(self.clone());
        let providers = crate::provider_reads::router(self.clone());
        Router::new()
            .route("/api/accounts/token/", post(login))
            .route("/api/accounts/auth/login/", post(login_alias))
            .route("/api/accounts/token/refresh/", post(refresh))
            .route("/api/accounts/auth/logout/", post(logout))
            .with_state(self)
            .merge(accounts)
            .merge(core)
            .merge(channel_profiles)
            .merge(channels)
            .merge(channel_groups)
            .merge(streams)
            .merge(startup)
            .merge(providers)
    }

    /// Same peer trust used by forwarded client-IP resolution. Export origin
    /// builders must check this socket peer before using forwarded headers.
    pub fn is_trusted_proxy(&self, peer: IpAddr) -> bool {
        cidrs_allow(
            self.config.trusted_proxies.as_deref().unwrap_or(
                "127.0.0.0/8,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,::1/128,fc00::/7,fe80::/10",
            ),
            dispatcharr_contracts::SourceIpAddr::from(peer).address(),
        )
    }
    pub fn source_client_ip(
        &self,
        request: &PlaybackRequest,
    ) -> dispatcharr_contracts::SourceIpAddr {
        let peer = dispatcharr_contracts::SourceIpAddr::from(request.peer_ip);
        if !self.is_trusted_proxy(peer.address()) {
            return peer;
        }
        if let Some(ip) = request
            .header("x-real-ip")
            .and_then(|value| value.parse::<dispatcharr_contracts::SourceIpAddr>().ok())
        {
            return ip;
        }
        if let Some(forwarded) = request.header("x-forwarded-for") {
            for value in forwarded.split(',').rev() {
                if let Ok(ip) = value.parse::<dispatcharr_contracts::SourceIpAddr>() {
                    if !self.is_trusted_proxy(ip.address()) {
                        return ip;
                    }
                }
            }
        }
        peer
    }
    pub fn client_ip(&self, request: &PlaybackRequest) -> IpAddr {
        self.source_client_ip(request).address()
    }
    pub fn client_ip_text(&self, request: &PlaybackRequest) -> String {
        self.source_client_ip(request).to_string()
    }

    pub(crate) fn explicit_trusted_proxies(&self) -> Option<&str> {
        self.config.trusted_proxies.as_deref()
    }

    pub(crate) fn throttle(
        &self,
        ip: IpAddr,
        kind: &'static str,
        record: bool,
    ) -> Result<(), StoreError> {
        let now = Instant::now();
        let mut buckets = self.attempts.lock().unwrap();
        buckets.retain(|_, history| {
            history
                .back()
                .is_some_and(|time| now.duration_since(*time) < Duration::from_secs(60))
        });
        // Fail closed instead of unbounded attacker-controlled bucket allocation.
        if !buckets.contains_key(&(ip, kind)) && buckets.len() >= 10_000 {
            return Err(StoreError::RateLimited(60));
        }
        let history = buckets.entry((ip, kind)).or_default();
        while history
            .front()
            .is_some_and(|time| now.duration_since(*time) >= Duration::from_secs(60))
        {
            history.pop_front();
        }
        let limit = if kind == "login" { 3 } else { 10 };
        if history.len() >= limit {
            let wait = history.front().map_or(60, |time| {
                60u64.saturating_sub(now.duration_since(*time).as_secs())
            });
            return Err(StoreError::RateLimited(wait.max(1)));
        }
        if record {
            history.push_back(now);
        }
        Ok(())
    }

    pub fn decode_token(&self, token: &str, kind: &str) -> Result<Claims, StoreError> {
        let mut validation = Validation::new(Algorithm::HS256);
        validation.leeway = 0;
        validation.validate_nbf = true;
        let raw = decode::<Value>(
            token,
            &DecodingKey::from_secret(self.config.signing_key.as_bytes()),
            &validation,
        )
        .map_err(|error| {
            token_error(
                kind,
                if matches!(
                    error.kind(),
                    jsonwebtoken::errors::ErrorKind::ExpiredSignature
                ) {
                    "Token is expired"
                } else {
                    "Token is invalid"
                },
            )
        })?
        .claims;
        if raw.get("jti").is_none() {
            return Err(token_error(kind, "Token has no id"));
        }
        if raw.get("token_type").is_none() {
            return Err(token_error(kind, "Token has no type"));
        }
        if raw.get("token_type").and_then(Value::as_str) != Some(kind) {
            return Err(token_error(kind, "Token has wrong type"));
        }
        let claims: Claims =
            serde_json::from_value(raw).map_err(|_| token_error(kind, "Token is invalid"))?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| StoreError::Unauthorized("Token is invalid or expired"))?
            .as_secs();
        // PyJWT used by SimpleJWT rejects exp == now and future issued-at.
        if claims.exp <= now || claims.iat > now {
            return Err(token_error(
                kind,
                if claims.exp <= now {
                    "Token is expired"
                } else {
                    "Token is invalid"
                },
            ));
        }
        if claims.id().is_none() {
            return Err(StoreError::AuthenticationBody(
                json!({"detail":"Token contained no recognizable user identification","code":"token_not_valid"}),
            ));
        }
        Ok(claims)
    }

    pub(crate) fn mint(&self, id: Value, kind: &str) -> Result<String, StoreError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| StoreError::Unsupported("clock before epoch"))?
            .as_secs();
        let lifetime = if kind == "refresh" {
            self.config.refresh_lifetime
        } else {
            self.config.access_lifetime
        };
        let claims = Claims {
            token_type: kind.to_string(),
            exp: now + lifetime.as_secs(),
            iat: now,
            jti: uuid::Uuid::new_v4().simple().to_string(),
            user_id: id,
        };
        encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(self.config.signing_key.as_bytes()),
        )
        .map_err(|_| StoreError::Unsupported("token signing failed"))
    }

    pub async fn authenticate(
        &self,
        request: &PlaybackRequest,
    ) -> Result<Option<User>, StoreError> {
        let auth: Vec<_> = request
            .header("authorization")
            .unwrap_or("")
            .split_whitespace()
            .collect();
        if auth.first().is_some_and(|scheme| *scheme == "Bearer") {
            if auth.len() != 2 {
                return Err(StoreError::AuthenticationBody(json!({
                    "detail":"Authorization header must contain two space-delimited values",
                    "code":"bad_authorization_header"
                })));
            }
            let claims = self.decode_token(auth[1], "access")?;
            return match self.store.user_by_id(claims.id().unwrap()).await? {
                Some(user) if user.is_active => Ok(Some(user)),
                Some(_) => Err(StoreError::AuthenticationBody(
                    json!({"detail":"User is inactive","code":"user_inactive"}),
                )),
                None => Err(StoreError::AuthenticationBody(
                    json!({"detail":"User not found","code":"user_not_found"}),
                )),
            };
        }
        let key = request
            .header("x-api-key")
            .filter(|value| !value.is_empty())
            .or_else(|| {
                if auth.len() == 2 && auth[0].eq_ignore_ascii_case("apikey") {
                    Some(auth[1])
                } else {
                    None
                }
            });
        if let Some(key) = key {
            self.shared_auth_budget(
                self.source_client_ip(request),
                crate::auth_throttle::Scope::ApiKey,
                false,
                false,
            )
            .await?;
            match self.store.user_by_api_key(key).await? {
                Some(user) if user.is_active => return Ok(Some(user)),
                user => {
                    if self
                        .shared_auth_budget(
                            self.source_client_ip(request),
                            crate::auth_throttle::Scope::ApiKey,
                            true,
                            false,
                        )
                        .await?
                    {
                        self.auth_event(
                            request,
                            "login_failed",
                            json!(user
                                .as_ref()
                                .map_or("api_key", |user| user.username.as_str())),
                            Some("API key rate limited"),
                        )
                        .await;
                        self.auth_lockout_notice(&crate::auth_events::API_NOTICE)
                            .await;
                    }
                    return Err(StoreError::Unauthorized(if user.is_some() {
                        "User inactive"
                    } else {
                        "Invalid API key"
                    }));
                }
            }
        }
        Ok(None)
    }

    pub async fn login(
        &self,
        request: &PlaybackRequest,
        body: Value,
    ) -> Result<TokenPair, StoreError> {
        let ip = self.client_ip(request);
        if let Err(error) = self
            .shared_auth_budget(
                self.source_client_ip(request),
                crate::auth_throttle::Scope::Login,
                true,
                false,
            )
            .await
        {
            if let StoreError::Throttled { wait, .. } = &error {
                self.login_lockout(
                    request,
                    body.get("username").cloned().unwrap_or(Value::Null),
                    *wait,
                )
                .await;
            }
            return Err(error);
        }
        if !self.source_network_allowed(ip, "UI", None).await? {
            self.auth_event(
                request,
                "login_failed",
                body.get("username").cloned().unwrap_or(json!("unknown")),
                Some("Network access denied"),
            )
            .await;
            return Err(StoreError::Forbidden);
        }
        if !body.is_object() {
            // Original custom token view calls request.data.get before DRF
            // serializer validation and therefore crashes for non-mappings.
            return Err(StoreError::StatusBody(
                500,
                json!({"detail":"Invalid login payload."}),
            ));
        }
        let mut errors = serde_json::Map::new();
        let mut values = serde_json::Map::new();
        for name in ["username", "password"] {
            match body.get(name) {
                None => {
                    errors.insert(name.to_string(), json!(["This field is required."]));
                }
                Some(value) => match crate::admin_writes::character(name, value, None, false) {
                    Ok(value) => {
                        values.insert(name.into(), Value::String(value));
                    }
                    Err(StoreError::InvalidInput(value)) => {
                        if let Some(value) = value.get(name) {
                            errors.insert(name.into(), value.clone());
                        }
                    }
                    Err(error) => return Err(error),
                },
            }
        }
        if !errors.is_empty() {
            let body_error = Value::Object(errors);
            let reason = format!(
                "Authentication error: {}",
                crate::auth_events::validation_exception(&body_error)
                    .chars()
                    .take(100)
                    .collect::<String>()
            );
            self.auth_event(
                request,
                "login_failed",
                crate::auth_events::audit_username(&body),
                Some(&reason),
            )
            .await;
            return Err(StoreError::InvalidInput(body_error));
        }
        let username = values["username"].as_str().unwrap();
        let password = values["password"].as_str().unwrap().to_owned();
        let user = self.store.user_by_username(username).await?;
        let encoded = user
            .as_ref()
            .map(|user| user.password.clone())
            .unwrap_or_else(|| {
                "pbkdf2_sha256$1200000$missing$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
                    .to_owned()
            });
        let worker = self
            .password_workers
            .clone()
            .try_acquire_owned()
            .map_err(|_| StoreError::RateLimited(1))?;
        let previous_hash = encoded.clone();
        let (matched, upgraded_hash) = tokio::task::spawn_blocking(move || {
            let _worker = worker;
            crate::passwords::verify_and_upgrade(&password, &encoded)
        })
        .await
        .map_err(|_| StoreError::Unsupported("password verification worker failed"))?;
        if let (Some(user), Some(upgraded_hash)) = (user.as_ref(), upgraded_hash) {
            // Avoid overwriting a password changed concurrently after lookup.
            // No other user fields or timestamps are changed by Django's setter.
            sqlx::query("UPDATE accounts_user SET password=$1 WHERE id=$2 AND password=$3")
                .bind(upgraded_hash)
                .bind(user.id)
                .bind(previous_hash)
                .execute(self.store.pool())
                .await?;
        }
        let Some(user) = user.filter(|user| user.is_active && matched) else {
            self.auth_event(
                request,
                "login_failed",
                crate::auth_events::audit_username(&body),
                Some("Authentication error: No active account found with the given credentials"),
            )
            .await;
            return Err(StoreError::Unauthorized(
                "No active account found with the given credentials",
            ));
        };
        // The original custom view looks up the untrimmed request username
        // after authentication. Padded names may authenticate successfully
        // without updating last_login; do not silently change that behavior.
        if crate::channels::truthy(body.get("username")) {
            let raw = &body["username"];
            let logged_username = if let Some(value) = raw.as_str() {
                value.to_owned()
            } else {
                crate::admin_writes::character("username", raw, None, false)?
            };
            let saved = sqlx::query("UPDATE accounts_user SET last_login=NOW() WHERE username=$1")
                .bind(logged_username)
                .execute(self.store.pool())
                .await?;
            if saved.rows_affected() > 0 {
                self.auth_event(request, "login_success", body["username"].clone(), None)
                    .await;
            }
        }
        Ok(TokenPair {
            access: self.mint(json!(user.id.to_string()), "access")?,
            refresh: self.mint(json!(user.id.to_string()), "refresh")?,
        })
    }

    pub(crate) async fn hash_password(&self, password: String) -> Result<String, StoreError> {
        let worker = self
            .password_workers
            .clone()
            .try_acquire_owned()
            .map_err(|_| StoreError::RateLimited(1))?;
        tokio::task::spawn_blocking(move || {
            let _worker = worker;
            crate::passwords::encode_password(&password)
        })
        .await
        .map_err(|_| StoreError::Unsupported("password verification worker failed"))?
        .ok_or_else(|| {
            StoreError::InvalidInput(
                json!({"password":["Password exceeds implementation size limit."]}),
            )
        })
    }

    pub async fn refresh(
        &self,
        request: &PlaybackRequest,
        token: &str,
    ) -> Result<Value, StoreError> {
        self.refresh_network(request).await?;
        self.refresh_after_network(token).await
    }

    async fn refresh_network(&self, request: &PlaybackRequest) -> Result<(), StoreError> {
        if !self
            .source_network_allowed(self.client_ip(request), "UI", None)
            .await?
        {
            self.auth_event(
                request,
                "login_failed",
                json!("token_refresh"),
                Some("Network access denied (token refresh)"),
            )
            .await;
            return Err(StoreError::StatusBody(403, json!({"error":"Unauthorized"})));
        }
        Ok(())
    }

    async fn refresh_after_network(&self, token: &str) -> Result<Value, StoreError> {
        let claims = self.decode_token(token, "refresh")?;
        let user = self
            .store
            .user_by_id(claims.id().unwrap())
            .await?
            .filter(|user| user.is_active)
            .ok_or(StoreError::AuthenticationBody(json!({"detail":"No active account found for the given token.","code":"no_active_account"})))?;
        Ok(json!({"access": self.mint(json!(user.id.to_string()), "access")?}))
    }
}

pub(crate) fn request(peer: SocketAddr, headers: HeaderMap) -> PlaybackRequest {
    PlaybackRequest {
        channel_id: String::new(),
        query: Vec::new(),
        peer_ip: peer.ip(),
        headers: dispatcharr_contracts::source_headers(
            headers
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_bytes())),
        ),
    }
}

async fn login(
    State(service): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Result<Json<Value>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let body = match body {
        Ok(Json(body)) => body,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"detail":"JSON parse error"})),
            )
                .into_response()
        }
    };
    match service.login(&request(peer, headers), body).await {
        Ok(pair) => Json(pair).into_response(),
        Err(error) => error.into_response(),
    }
}

async fn login_alias(
    State(service): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Result<Json<Value>, axum::extract::rejection::JsonRejection>,
) -> Response {
    // AuthViewSet authenticates before delegating to TokenObtainPairView,
    // whereas the direct token view explicitly has no authentication classes.
    if let Err(error) = service.authenticate(&request(peer, headers.clone())).await {
        return error.into_response();
    }
    login(State(service), ConnectInfo(peer), headers, body).await
}

async fn refresh(
    State(service): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Result<Json<Value>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let request = request(peer, headers);
    // The source checks UI policy before accessing/parsing request.data.
    if let Err(error) = service.refresh_network(&request).await {
        return error.into_response();
    }
    let body = match body {
        Ok(Json(body)) => body,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"detail":"JSON parse error"})),
            )
                .into_response()
        }
    };
    if !body.is_object() {
        return StoreError::InvalidInput(json!({"non_field_errors":[format!("Invalid data. Expected a dictionary, but got {}.",crate::channel_memberships::datatype(&body))]})).into_response();
    }
    let Some(value) = body.get("refresh") else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"refresh":["This field is required."]})),
        )
            .into_response();
    };
    let token = match crate::admin_writes::character("refresh", value, None, false) {
        Ok(token) => token,
        Err(error) => return error.into_response(),
    };
    match service.refresh_after_network(&token).await {
        Ok(body) => Json(body).into_response(),
        Err(error) => error.into_response(),
    }
}

async fn logout(
    State(service): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let playback_request = request(peer, headers.clone());
    match service.account_user(peer, headers, false).await {
        Ok(user) => {
            service
                .auth_event(&playback_request, "logout", json!(user.username), None)
                .await;
            Json(json!({"message":"Logout successful"})).into_response()
        }
        Err(error) => error.into_response(),
    }
}

impl IntoResponse for StoreError {
    fn into_response(self) -> Response {
        let (status, body) = match self {
            Self::UiPermissionFault => {
                let mut response = (StatusCode::INTERNAL_SERVER_ERROR,
                    [("content-type", "text/html; charset=utf-8"), ("vary", "origin")],
                    "\n<!doctype html>\n<html lang=\"en\">\n<head>\n  <title>Server Error (500)</title>\n</head>\n<body>\n  <h1>Server Error (500)</h1><p></p>\n</body>\n</html>\n").into_response();
                response.headers_mut().insert("content-length", axum::http::HeaderValue::from_static("145"));
                return response;
            }
            Self::Database(_) => (
                StatusCode::SERVICE_UNAVAILABLE,
                json!({"error":"Database unavailable"}),
            ),
            Self::Unauthorized(detail) => (StatusCode::UNAUTHORIZED, json!({"detail":detail})),
            Self::AuthenticationBody(body) => (StatusCode::UNAUTHORIZED, body),
            Self::Forbidden => (StatusCode::FORBIDDEN, json!({"error":"Forbidden"})),
            Self::PermissionDenied => (
                StatusCode::FORBIDDEN,
                json!({"detail":"You do not have permission to perform this action."}),
            ),
            Self::NotFound => (StatusCode::NOT_FOUND, json!({"detail":"Not found."})),
            Self::Unsupported(detail) => (StatusCode::NOT_IMPLEMENTED, json!({"error":detail})),
            Self::XcDenied {
                status,
                retry_after,
            } => {
                let mut response = (
                    StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                    Json(crate::xc_auth::failure_body(status)),
                )
                    .into_response();
                if let Some(wait) = retry_after {
                    if let Ok(value) = wait.to_string().parse() {
                        response.headers_mut().insert("retry-after", value);
                    }
                }
                return response;
            }
            Self::RateLimited(wait) => {
                return (
                    StatusCode::TOO_MANY_REQUESTS,
                    [("retry-after", wait.to_string())],
                    Json(json!({"detail":"Too many login attempts. Try again later."})),
                )
                    .into_response()
            }
            Self::Throttled {
                body, retry_after, ..
            } => {
                let mut response = (StatusCode::TOO_MANY_REQUESTS, Json(body)).into_response();
                if let Some(wait) = retry_after {
                    if let Ok(value) = wait.to_string().parse() {
                        response.headers_mut().insert("retry-after", value);
                    }
                }
                return response;
            }
            Self::InvalidInput(body) => (StatusCode::BAD_REQUEST, body),
            Self::StatusBody(status, body) => (
                StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                body,
            ),
        };
        let mut response = (status, Json(body)).into_response();
        if status == StatusCode::UNAUTHORIZED {
            response.headers_mut().insert(
                "www-authenticate",
                axum::http::HeaderValue::from_static("Bearer realm=\"api\""),
            );
        }
        response
    }
}

fn token_error(kind: &str, message: &'static str) -> StoreError {
    StoreError::AuthenticationBody(if kind == "access" {
        json!({"detail":"Given token not valid for any token type","code":"token_not_valid","messages":[{"token_class":"AccessToken","token_type":"access","message":message}]})
    } else {
        json!({"detail":message,"code":"token_not_valid"})
    })
}

/// Physical publication belongs to the same caller scope. The real M7 bus
/// overrides send_retained to move that guard into each owned Redis operation.
/// A second supplied scope composes with the authentication scope.
struct RetainedAuthSink {
    inner: Arc<dyn dispatcharr_contracts::UpdateEventSink>,
    guard: Arc<dyn Send + Sync>,
}
impl dispatcharr_contracts::UpdateEventSink for RetainedAuthSink {
    fn send<'a>(
        &'a self,
        group: &'a str,
        message: Value,
    ) -> dispatcharr_contracts::BoxFuture<'a, Result<(), dispatcharr_contracts::UpdateEventError>>
    {
        self.inner.send_retained(group, message, self.guard.clone())
    }
    fn send_retained<'a>(
        &'a self,
        group: &'a str,
        message: Value,
        guard: Arc<dyn Send + Sync>,
    ) -> dispatcharr_contracts::BoxFuture<'a, Result<(), dispatcharr_contracts::UpdateEventError>>
    {
        self.inner
            .send_retained(group, message, Arc::new((self.guard.clone(), guard)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn service() -> AuthService {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://unused:unused@localhost/unused")
            .unwrap();
        AuthService::new(
            Store::from_pool(pool),
            AuthConfig::new("test-signing-secret-only".into()).unwrap(),
        )
    }

    fn request_for(peer: &str, headers: Vec<(&str, &str)>) -> PlaybackRequest {
        PlaybackRequest {
            channel_id: String::new(),
            query: Vec::new(),
            peer_ip: peer.parse().unwrap(),
            headers: headers
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
        }
    }

    #[tokio::test]
    async fn minted_tokens_have_simplejwt_types_lifetimes_and_unique_ids() {
        let service = service();
        let access = service.mint(json!("42"), "access").unwrap();
        let first = service.decode_token(&access, "access").unwrap();
        let second = service
            .decode_token(&service.mint(json!(42), "access").unwrap(), "access")
            .unwrap();
        assert_eq!(first.id(), Some(42));
        assert_eq!(second.id(), Some(42));
        assert_eq!(first.exp - first.iat, 1800);
        assert_ne!(first.jti, second.jti);
        assert!(service.decode_token(&access, "refresh").is_err());
        let refresh = service
            .decode_token(&service.mint(json!("42"), "refresh").unwrap(), "refresh")
            .unwrap();
        assert_eq!(refresh.exp - refresh.iat, 86400);
    }

    #[tokio::test]
    async fn signature_algorithm_expiry_and_future_issued_at_are_enforced() {
        let service = service();
        let valid = service.mint(json!(42), "access").unwrap();
        let mut claims = service.decode_token(&valid, "access").unwrap();
        let key = EncodingKey::from_secret(service.config.signing_key.as_bytes());
        claims.exp = claims.iat;
        let expired = encode(&Header::new(Algorithm::HS256), &claims, &key).unwrap();
        assert!(service.decode_token(&expired, "access").is_err());
        claims.exp += 3600;
        claims.iat += 1800;
        let future = encode(&Header::new(Algorithm::HS256), &claims, &key).unwrap();
        assert!(service.decode_token(&future, "access").is_err());
        claims.iat -= 1800;
        let wrong_algorithm = encode(&Header::new(Algorithm::HS512), &claims, &key).unwrap();
        assert!(service.decode_token(&wrong_algorithm, "access").is_err());
        let wrong_key = encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(b"wrong"),
        )
        .unwrap();
        assert!(service.decode_token(&wrong_key, "access").is_err());
    }

    #[tokio::test]
    async fn forwarded_headers_follow_only_trusted_peer_and_rightmost_untrusted_hop() {
        let mut service = service();
        let public = request_for("203.0.113.1", vec![("x-real-ip", "127.0.0.1")]);
        assert_eq!(service.client_ip(&public), public.peer_ip);
        let private = request_for("10.0.0.1", vec![("x-real-ip", "192.168.0.10")]);
        assert_eq!(
            service.client_ip(&private),
            "192.168.0.10".parse::<IpAddr>().unwrap()
        );
        let chain = request_for(
            "10.0.0.1",
            vec![("x-forwarded-for", "198.51.100.1, 203.0.113.2, 10.0.0.2")],
        );
        assert_eq!(
            service.client_ip(&chain),
            "203.0.113.2".parse::<IpAddr>().unwrap()
        );
        service.config.trusted_proxies = Some("none".into());
        assert_eq!(service.client_ip(&private), private.peer_ip);
    }

    #[tokio::test]
    async fn mapped_ipv4_is_normalized_for_acl_rate_limits_and_client_metadata() {
        let service = service();
        assert_eq!(
            service.client_ip(&request_for("::ffff:203.0.113.1", Vec::new())),
            "203.0.113.1".parse::<IpAddr>().unwrap()
        );
        assert_eq!(
            service.client_ip(&request_for(
                "10.0.0.1",
                vec![("x-real-ip", "::ffff:192.168.1.2")]
            )),
            "192.168.1.2".parse::<IpAddr>().unwrap()
        );
    }

    #[tokio::test]
    async fn local_login_budget_counts_attempts_but_api_budget_only_failures() {
        let service = service();
        let ip = "127.0.0.1".parse().unwrap();
        for _ in 0..3 {
            assert!(service.throttle(ip, "login", true).is_ok());
        }
        assert!(matches!(
            service.throttle(ip, "login", true),
            Err(StoreError::RateLimited(_))
        ));
        for _ in 0..20 {
            assert!(service.throttle(ip, "api-key", false).is_ok());
        }
        for _ in 0..10 {
            assert!(service.throttle(ip, "api-key", true).is_ok());
        }
        assert!(matches!(
            service.throttle(ip, "api-key", false),
            Err(StoreError::RateLimited(_))
        ));
    }

    #[tokio::test]
    async fn malformed_bearer_matches_actual_simplejwt_code_before_database_work() {
        let service = service();
        for raw in ["Bearer", "Bearer a b"] {
            let error = service
                .authenticate(&request_for("127.0.0.1", vec![("authorization", raw)]))
                .await
                .err()
                .expect("Malformed Bearer must fail before database work");
            let response = error.into_response();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            assert_eq!(
                response.headers()["www-authenticate"],
                "Bearer realm=\"api\""
            );
            let body = axum::body::to_bytes(response.into_body(), 4096)
                .await
                .unwrap();
            // Actual Original unified-source HTTP cases12/13,107 exact bytes.
            assert_eq!(body.as_ref(), br#"{"detail":"Authorization header must contain two space-delimited values","code":"bad_authorization_header"}"#);
        }
    }

    #[tokio::test]
    async fn api_key_no_credentials_remains_anonymous_without_database_query() {
        let service = service();
        assert!(service
            .authenticate(&request_for("127.0.0.1", Vec::new()))
            .await
            .unwrap()
            .is_none());
        assert!(service
            .authenticate(&request_for(
                "127.0.0.1",
                vec![("authorization", "Basic ignored")]
            ))
            .await
            .unwrap()
            .is_none());
        assert!(service
            .authenticate(&request_for(
                "127.0.0.1",
                vec![("authorization", "Bearer malformed")]
            ))
            .await
            .is_err());
    }
}
