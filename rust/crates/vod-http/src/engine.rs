//! Ported from Dispatcharr's VOD view/session behavior. Original authors and
//! contributors retain full credit; see repository ATTRIBUTION.md and LICENSE.
use crate::{
    resolver::*,
    sessions::{SessionPool, Ticket},
};
use axum::{
    body::Body,
    extract::{ConnectInfo, Path, State},
    http::{HeaderMap, Request, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use bytes::Bytes;
use dispatcharr_contracts::PlaybackRequest;
use dispatcharr_coordination::Coordinator;
use dispatcharr_store::{AuthService, Store, StoreError};
use futures_util::{stream, StreamExt};
use serde_json::json;
use std::{
    collections::HashMap,
    io,
    net::SocketAddr,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

#[derive(Clone)]
pub struct VodConfig {
    pub max_sessions: usize,
    pub max_requests: usize,
    pub idle_grace: Duration,
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    /// Upper bound on transport bytes exposed in one body frame.
    pub frame_bytes: usize,
    pub shutdown_timeout: Duration,
}
impl Default for VodConfig {
    fn default() -> Self {
        Self {
            max_sessions: 1024,
            max_requests: 2048,
            idle_grace: Duration::from_secs(1),
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(10),
            frame_bytes: 64 * 1024,
            shutdown_timeout: Duration::from_secs(5),
        }
    }
}
struct Inner {
    resolver: Arc<dyn VodResolver>,
    sessions: SessionPool,
    client: reqwest::Client,
    config: VodConfig,
}
#[derive(Clone)]
pub struct VodEngine {
    inner: Arc<Inner>,
}
impl VodEngine {
    pub async fn new(
        store: Store,
        auth: AuthService,
        coordinator: Coordinator,
        redis_url: &str,
        config: VodConfig,
    ) -> Result<Self, StoreError> {
        Self::with_resolver(
            Arc::new(SqlVodResolver::new(store, auth)),
            coordinator,
            redis_url,
            config,
        )
        .await
    }
    pub async fn with_resolver(
        resolver: Arc<dyn VodResolver>,
        coordinator: Coordinator,
        redis_url: &str,
        config: VodConfig,
    ) -> Result<Self, StoreError> {
        if config.max_sessions == 0
            || config.max_requests == 0
            || config.frame_bytes == 0
            || config.frame_bytes > 1024 * 1024
            || config.read_timeout.is_zero()
            || config.connect_timeout.is_zero()
            || config.shutdown_timeout.is_zero()
        {
            return Err(StoreError::Unsupported("Invalid bounded VOD configuration"));
        }
        let sessions = SessionPool::connect(
            redis_url,
            coordinator,
            config.max_sessions,
            config.max_requests,
            config.idle_grace,
        )
        .await?;
        let client = reqwest::Client::builder()
            .connect_timeout(config.connect_timeout)
            .read_timeout(config.read_timeout)
            .redirect(reqwest::redirect::Policy::limited(30))
            .pool_max_idle_per_host(4)
            .build()
            .map_err(|_| StoreError::Unsupported("VOD HTTP client setup failed"))?;
        Ok(Self {
            inner: Arc::new(Inner {
                resolver,
                sessions,
                client,
                config,
            }),
        })
    }
    /// Stop admission and every supervised provider body under one global
    /// deadline. False leaves ledger/reaper recovery necessary after an outage.
    pub async fn shutdown(&self) -> bool {
        self.inner.sessions.stop_admission();
        tokio::time::timeout(
            self.inner.config.shutdown_timeout,
            self.inner.sessions.drain(),
        )
        .await
        .is_ok()
            && self.inner.sessions.cleanup_succeeded()
    }
    pub fn router(self) -> Router {
        Router::new()
            .route(
                "/proxy/vod/stop_client/",
                axum::routing::post(stop_client_route),
            )
            .route("/proxy/vod/{kind}/{content_id}", get(play).head(head))
            .route(
                "/proxy/vod/{kind}/{content_id}/{session_id}",
                get(play).head(head),
            )
            .route(
                "/proxy/vod/{kind}/{content_id}/{profile_id}/",
                get(play).head(head),
            )
            .route(
                "/proxy/vod/{kind}/{content_id}/{session_id}/{profile_id}/",
                get(play).head(head),
            )
            .with_state(self)
    }
    pub async fn respond(&self, mut request: VodRequest) -> Response {
        match self.prepare(&mut request).await {
            Ok(response) => response,
            Err(error) => error_response(error),
        }
    }
    async fn prepare(&self, request: &mut VodRequest) -> Result<Response, StoreError> {
        if self.inner.sessions.stopped() {
            return Err(StoreError::StatusBody(
                503,
                json!({"error":"VOD is shutting down"}),
            ));
        }
        let mut user = self.inner.resolver.authorize(request).await?;
        request.playback.peer_ip = self.inner.resolver.client_ip(&request.playback);
        if let Some(id) = &request.session_id {
            if !valid_session(id) {
                return Err(StoreError::NotFound);
            }
            if user.is_none() {
                if let Some(user_id) = self.inner.sessions.user_for_session(id).await? {
                    user = self.inner.resolver.restore_user(user_id).await?;
                }
            }
        }
        // Django mints the redirect before resolving a UUID/content record.
        if request.session_id.is_none() && !self.inner.resolver.default_redirect().await? {
            let session = format!(
                "vod_{}_{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                uuid::Uuid::new_v4().simple()
            );
            if let Some(user) = user {
                self.inner.sessions.remember_user(&session, user.id).await?;
            }
            return Ok(session_redirect(request, &session));
        }
        let source = self.inner.resolver.resolve(request, user).await?;
        if source.redirect && request.session_id.is_none() {
            if let Some(id) = self.inner.sessions.idle_session(&source, request).await {
                if let Some(user) = &source.user {
                    self.inner.sessions.remember_user(&id, user.id).await?;
                }
                return Ok(session_redirect(request, &id));
            }
            // Source Redirect checks capacity without probing or reserving.
            // Existing remote idle-session adoption remains an explicit gate.
            let location = self.inner.sessions.redirect_candidate(&source).await?;
            return Ok(Response::builder()
                .status(302)
                .header("location", iri_location(&location))
                .body(Body::empty())
                .unwrap());
        }
        let id = request.session_id.as_deref().ok_or(StoreError::NotFound)?;
        let ticket = self.inner.sessions.enter(id, source, request).await?;
        let mut upstream_request = self
            .inner
            .client
            .get(&ticket.session.candidate.url)
            .header("user-agent", &ticket.session.candidate.user_agent)
            .header("accept-encoding", "identity");
        let original_range = request
            .playback
            .header("range")
            .filter(|value| !value.is_empty());
        if let Some(range) = original_range {
            let total = ticket.session.content_length.load(Ordering::Relaxed);
            let validated = if total > 0 {
                validate_range(range, total)
            } else {
                Some(range.into())
            };
            let Some(range) = validated else {
                return Ok(range_not_satisfiable());
            };
            upstream_request = upstream_request.header("range", range);
        }
        // The original VOD path forwards Range, but does not forward HTTP
        // conditional validators. Preserve that compatibility behavior.
        for name in ["accept", "referer", "origin"] {
            if let Some(value) = request.playback.header(name) {
                upstream_request = upstream_request.header(name, value);
            }
        }
        let upstream = tokio::select! {
            response = upstream_request.send() => response.map_err(|_| StoreError::StatusBody(500,json!("Streaming error: VOD provider request failed")))?,
            _ = stopped(&ticket) => return Err(StoreError::StatusBody(503,json!({"error":"VOD session stopped"}))),
        };
        if upstream.status().is_client_error() || upstream.status().is_server_error() {
            // Avoid reflecting provider credentials from requests' exception URL.
            return Ok((
                StatusCode::INTERNAL_SERVER_ERROR,
                [
                    ("content-type", "text/html; charset=utf-8"),
                    ("allow", "GET, OPTIONS"),
                ],
                "Streaming error: VOD provider request failed",
            )
                .into_response());
        }
        let discovered_total = upstream
            .headers()
            .get("content-range")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.rsplit('/').next()?.parse::<u64>().ok())
            .or_else(|| upstream.content_length())
            .unwrap_or(0);
        let _ = ticket.session.content_length.compare_exchange(
            0,
            discovered_total,
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
        let total = ticket.session.content_length.load(Ordering::Relaxed);
        let status = if original_range.is_some() {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        };
        let headers = compatibility_headers(upstream.headers(), original_range, total);
        let metadata = [
            ("content_length", total.to_string()),
            (
                "content_type",
                upstream
                    .headers()
                    .get("content-type")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .into(),
            ),
            ("final_url", upstream.url().as_str().into()),
        ];
        self.inner
            .sessions
            .update(&ticket.session, &metadata)
            .await?;
        // Preserve Python's Range response contract, including its documented
        // suffix/conditional quirks. Correcting these is outside the 1:1 port.
        // A single bounded channel decouples cancellation from downstream body
        // polls. A stalled viewer must not keep a stopped provider lease alive.
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let stop = ticket.session.stop.subscribe();
        let retry = RetryRequest {
            client: self.inner.client.clone(),
            final_url: upstream.url().as_str().into(),
            original_url: ticket.session.candidate.url.clone(),
            user_agent: ticket.session.candidate.user_agent.clone(),
            range: original_range.map(str::to_owned),
            headers: ["accept", "referer", "origin"]
                .into_iter()
                .filter_map(|name| {
                    request
                        .playback
                        .header(name)
                        .map(|value| (name.to_owned(), value.to_owned()))
                })
                .collect(),
        };
        let task = tokio::spawn(pump(
            upstream,
            ticket,
            sender,
            self.inner.config.frame_bytes,
            retry,
        ));
        let state = BodyState {
            receiver,
            stop,
            _cancel: CancelTask(task),
        };
        let body = stream::try_unfold(state, |mut state| async move {
            if *state.stop.borrow() {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "VOD session stopped",
                ));
            }
            let next = tokio::select! { next=state.receiver.recv()=>next, _=state.stop.changed()=>return Err(io::Error::new(io::ErrorKind::Interrupted,"VOD session stopped")) };
            if *state.stop.borrow() {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "VOD session stopped",
                ));
            }
            match next {
                Some(Ok(chunk)) => Ok(Some((chunk, state))),
                Some(Err(error)) => Err(error),
                None => Ok(None),
            }
        });
        let mut response = Response::new(Body::from_stream(body));
        *response.status_mut() = status;
        *response.headers_mut() = headers;
        Ok(response)
    }
}
struct CancelTask(tokio::task::JoinHandle<()>);
impl Drop for CancelTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}
struct BodyState {
    receiver: tokio::sync::mpsc::Receiver<Result<Bytes, io::Error>>,
    stop: tokio::sync::watch::Receiver<bool>,
    _cancel: CancelTask,
}
struct RetryRequest {
    client: reqwest::Client,
    final_url: String,
    original_url: String,
    user_agent: String,
    range: Option<String>,
    headers: Vec<(String, String)>,
}
impl RetryRequest {
    async fn reopen(&self, offset: u64, total: u64) -> Result<reqwest::Response, ()> {
        let end = self
            .range
            .as_deref()
            .and_then(|value| value.strip_prefix("bytes="))
            .and_then(|value| value.split_once('-'))
            .map(|(_, end)| end)
            .unwrap_or("");
        let range = format!("bytes={offset}-{end}");
        let range = if total > 0 {
            validate_range(&range, total).ok_or(())?
        } else {
            range
        };
        let send = |url: &str| {
            let mut request = self
                .client
                .get(url)
                .header("user-agent", &self.user_agent)
                .header("accept-encoding", "identity")
                .header("range", &range);
            for (name, value) in &self.headers {
                request = request.header(name, value);
            }
            request.send()
        };
        let mut response = send(&self.final_url).await.map_err(|_| ())?;
        if response.status().is_client_error() || response.status().is_server_error() {
            drop(response);
            response = send(&self.original_url).await.map_err(|_| ())?;
        }
        if response.status().is_client_error() || response.status().is_server_error() {
            Err(())
        } else {
            Ok(response)
        }
    }
}
type ProviderStream =
    std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<Bytes, reqwest::Error>> + Send>>;
async fn pump(
    upstream: reqwest::Response,
    ticket: Ticket,
    sender: tokio::sync::mpsc::Sender<Result<Bytes, io::Error>>,
    frame_bytes: usize,
    retry: RetryRequest,
) {
    let mut upstream: ProviderStream = Box::pin(upstream.bytes_stream());
    let mut stop = ticket.session.stop.subscribe();
    let mut failures = 0;
    let mut sent_bytes = 0u64;
    let range_start = retry
        .range
        .as_deref()
        .and_then(|value| value.strip_prefix("bytes="))
        .and_then(|value| value.split_once('-'))
        .and_then(|(start, _)| start.parse::<u64>().ok())
        .unwrap_or(0);
    loop {
        if *stop.borrow() {
            return;
        }
        let next = tokio::select! { result=upstream.next()=>result, _=stop.changed()=>return };
        let Some(next) = next else {
            return;
        };
        let mut chunk = match next {
            Ok(chunk) => chunk,
            Err(_) => {
                // Source permits three consecutive reopen attempts; any
                // successfully delivered payload resets that failure streak.
                loop {
                    failures += 1;
                    if failures > 3 {
                        let error = io::Error::new(
                            io::ErrorKind::ConnectionReset,
                            "VOD provider body failed",
                        );
                        tokio::select! { _=sender.send(Err(error))=>{}, _=stop.changed()=>{} }
                        return;
                    }
                    let Some(offset) = range_start.checked_add(sent_bytes) else {
                        return;
                    };
                    let response = tokio::select! { result=retry.reopen(offset,ticket.session.content_length.load(Ordering::Relaxed))=>result, _=stop.changed()=>return };
                    if let Ok(response) = response {
                        upstream = Box::pin(response.bytes_stream());
                        break;
                    }
                }
                continue;
            }
        };
        while !chunk.is_empty() {
            let frame = chunk.split_to(chunk.len().min(frame_bytes));
            let count = frame.len() as u64;
            if *stop.borrow() {
                return;
            }
            let sent = tokio::select! { result=sender.send(Ok(frame))=>result.is_ok(), _=stop.changed()=>false };
            if !sent {
                return;
            }
            sent_bytes = sent_bytes.saturating_add(count);
            failures = 0;
            ticket.session.bytes.fetch_add(count, Ordering::Relaxed);
        }
    }
}
async fn stopped(ticket: &Ticket) {
    let mut stop = ticket.session.stop.subscribe();
    if *stop.borrow() {
        return;
    }
    while stop.changed().await.is_ok() {
        if *stop.borrow() {
            return;
        }
    }
}
// Literal compatibility port of RedisBackedVODConnection._validate_range_header.
fn validate_range(value: &str, total: u64) -> Option<String> {
    let Some(part) = value.strip_prefix("bytes=") else {
        return Some(value.into());
    };
    let Some((start, end)) = part.split_once('-') else {
        return Some(value.into());
    };
    let start = if start.is_empty() {
        0
    } else {
        match start.parse::<u64>() {
            Ok(v) => v,
            Err(_) => return Some(value.into()),
        }
    };
    let end = if end.is_empty() {
        total.saturating_sub(1)
    } else {
        match end.parse::<u64>() {
            Ok(v) => v.min(total.saturating_sub(1)),
            Err(_) => return Some(value.into()),
        }
    };
    if start >= total || start > end {
        None
    } else {
        Some(format!("bytes={start}-{end}"))
    }
}
fn compatibility_headers(source: &HeaderMap, range: Option<&str>, total: u64) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in [
        ("cache-control", "no-cache"),
        ("pragma", "no-cache"),
        ("x-content-type-options", "nosniff"),
        ("connection", "keep-alive"),
        ("allow", "GET, OPTIONS"),
    ] {
        headers.insert(
            axum::http::HeaderName::from_static(name),
            axum::http::HeaderValue::from_static(value),
        );
    }
    headers.insert(
        "content-type",
        source
            .get("content-type")
            .cloned()
            .unwrap_or_else(|| axum::http::HeaderValue::from_static("video/mp4")),
    );
    if total > 0 {
        headers.insert(
            "accept-ranges",
            axum::http::HeaderValue::from_static("bytes"),
        );
        let bounds = range
            .and_then(|value| value.strip_prefix("bytes="))
            .and_then(|value| value.split_once('-'))
            .and_then(|(start, end)| {
                Some((
                    if start.is_empty() {
                        0
                    } else {
                        start.parse::<u64>().ok()?
                    },
                    if end.is_empty() {
                        total - 1
                    } else {
                        end.parse::<u64>().ok()?
                    },
                ))
            });
        if let Some((start, end)) = bounds {
            if let Some(length) = end.checked_sub(start).and_then(|v| v.checked_add(1)) {
                headers.insert("content-length", length.to_string().parse().unwrap());
                headers.insert(
                    "content-range",
                    format!("bytes {start}-{end}/{total}").parse().unwrap(),
                );
            }
        } else {
            headers.insert("content-length", total.to_string().parse().unwrap());
        }
    }
    headers
}
fn range_not_satisfiable() -> Response {
    (
        StatusCode::RANGE_NOT_SATISFIABLE,
        [
            ("content-type", "text/html; charset=utf-8"),
            ("allow", "GET, OPTIONS"),
        ],
        "Requested Range Not Satisfiable",
    )
        .into_response()
}
// Django iri_to_uri preserves URI syntax while quoting Unicode and controls.
// This also prevents malformed DB credentials/kinds from panicking HeaderValue.
fn iri_location(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || b"-._~/#%[]=:;$&()+,!?*@'".contains(&byte) {
            encoded.push(byte as char);
        } else {
            use std::fmt::Write;
            let _ = write!(encoded, "%{byte:02X}");
        }
    }
    encoded
}
fn session_redirect(request: &VodRequest, session: &str) -> Response {
    // Preserve the source helper's literal path append, including its bare
    // profile duplication. Such redirects fall through the app React route;
    // correcting this URL is a separate behavior change after migration.
    let mut location = format!("/proxy/vod/{}/{}", request.kind, request.content_id);
    if let Some(profile) = request.profile_id {
        location.push_str(&format!("/{profile}"));
    }
    location.push_str(&format!("/{session}"));
    if let Some(profile) = request.profile_id.filter(|profile| *profile != 0) {
        location.push_str(&format!("/{profile}/"));
    }
    let query = url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(
            request
                .playback
                .query
                .iter()
                .filter(|(key, _)| key != "session_id" && key != "token")
                .map(|(key, value)| (key, value)),
        )
        .finish();
    if !query.is_empty() {
        location.push('?');
        location.push_str(&query);
    }
    Response::builder()
        .status(301)
        .header("location", iri_location(&location))
        .header(
            "cache-control",
            if request.playback.query.iter().any(|(key, _)| key == "token") {
                "no-store"
            } else {
                "private, max-age=21600"
            },
        )
        .header("content-type", "text/html; charset=utf-8")
        .header("allow", "GET, OPTIONS")
        .body(Body::empty())
        .unwrap()
}
fn error_response(error: StoreError) -> Response {
    let mut response = match error {
        StoreError::StatusBody(code, serde_json::Value::String(value))
            if (code == 503 && value == "No available stream") || code == 500 =>
        {
            (
                StatusCode::from_u16(code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                [("content-type", "text/html; charset=utf-8")],
                value,
            )
                .into_response()
        }
        error => error.into_response(),
    };
    response.headers_mut().insert(
        "allow",
        axum::http::HeaderValue::from_static("GET, OPTIONS"),
    );
    response
}
fn valid_session(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|v| v.is_ascii_alphanumeric() || v == b'_' || v == b'-')
}
fn playback(request: &Request<Body>, content: String) -> Result<PlaybackRequest, StoreError> {
    let peer = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .ok_or(StoreError::Unsupported(
            "HTTP socket peer metadata is required",
        ))?
        .0
        .ip();
    Ok(PlaybackRequest {
        channel_id: content,
        query: url::form_urlencoded::parse(request.uri().query().unwrap_or("").as_bytes())
            .into_owned()
            .collect(),
        headers: request
            .headers()
            .iter()
            .filter_map(|(key, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (key.as_str().to_owned(), value.to_owned()))
            })
            .collect(),
        peer_ip: peer,
    })
}
async fn play(
    State(engine): State<VodEngine>,
    Path(path): Path<HashMap<String, String>>,
    request: Request<Body>,
) -> Response {
    let content_id = path.get("content_id").cloned().unwrap_or_default();
    if uuid::Uuid::parse_str(&content_id).is_err()
        || path
            .get("profile_id")
            .is_some_and(|value| value.parse::<u64>().is_err())
    {
        return error_response(StoreError::NotFound);
    }
    let playback = match playback(&request, content_id.clone()) {
        Ok(playback) => playback,
        Err(error) => return error_response(error),
    };
    engine
        .respond(VodRequest {
            playback,
            kind: path.get("kind").cloned().unwrap_or_default(),
            content_id,
            session_id: path.get("session_id").cloned(),
            profile_id: path.get("profile_id").and_then(|value| value.parse().ok()),
        })
        .await
}
async fn head() -> Response {
    (
        StatusCode::METHOD_NOT_ALLOWED,
        [("allow", "GET, OPTIONS")],
        axum::Json(json!({"detail":"Method \"HEAD\" not allowed."})),
    )
        .into_response()
}
async fn stop_client_route(State(engine): State<VodEngine>, request: Request<Body>) -> Response {
    let mut response = stop_client(State(engine), request).await;
    response.headers_mut().insert(
        "allow",
        axum::http::HeaderValue::from_static("POST, OPTIONS"),
    );
    response
}
async fn stop_client(State(engine): State<VodEngine>, request: Request<Body>) -> Response {
    let identity = match playback(&request, String::new()) {
        Ok(identity) => identity,
        Err(error) => return error_response(error),
    };
    if let Err(error) = engine.inner.resolver.admin(&identity).await {
        return error_response(error);
    }
    let body = match axum::body::to_bytes(request.into_body(), 16 * 1024).await {
        Ok(value) => value,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                axum::Json(json!({"error":"Invalid JSON"})),
            )
                .into_response()
        }
    };
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                axum::Json(json!({"error":"Invalid JSON"})),
            )
                .into_response()
        }
    };
    let Some(id) = value
        .get("client_id")
        .and_then(|v| v.as_str())
        .filter(|v| !v.is_empty())
    else {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(json!({"error":"No client_id provided"})),
        )
            .into_response();
    };
    match engine.inner.sessions.stop(id).await {
        Ok(true)=>(StatusCode::OK,axum::Json(json!({"message":"VOD client stop signal sent","client_id":id,"stop_key":format!("vod_proxy:client:{id}:stop")}))).into_response(),
        Ok(false)=>(StatusCode::NOT_FOUND,axum::Json(json!({"error":"Connection not found"}))).into_response(),
        Err(error)=>error_response(error),
    }
}

#[cfg(test)]
mod header_tests {
    use super::{iri_location, session_redirect};
    use crate::VodRequest;
    use dispatcharr_contracts::PlaybackRequest;
    #[test]
    fn bare_profile_mint_preserves_python_duplicate_path_and_zero_truthiness() {
        for (profile, expected) in [
            (None, "/proxy/vod/movie/fixture/session"),
            (Some(2), "/proxy/vod/movie/fixture/2/session/2/"),
            (Some(0), "/proxy/vod/movie/fixture/0/session"),
        ] {
            let request = VodRequest {
                playback: PlaybackRequest {
                    channel_id: String::new(),
                    query: vec![],
                    headers: vec![],
                    peer_ip: "127.0.0.1".parse().unwrap(),
                },
                kind: "movie".into(),
                content_id: "fixture".into(),
                session_id: None,
                profile_id: profile,
            };
            assert_eq!(
                session_redirect(&request, "session").headers()["location"],
                expected
            );
        }
    }
    #[test]
    fn django_location_encoding_preserves_uri_syntax_and_quotes_unicode_controls() {
        assert_eq!(
            iri_location("http://provider.invalid/movie/名/a b/1.mp4?x=1&y='v'#frag"),
            "http://provider.invalid/movie/%E5%90%8D/a%20b/1.mp4?x=1&y='v'#frag"
        );
        assert_eq!(
            iri_location("/proxy/vod/é/uuid?value=%20\r\n\t"),
            "/proxy/vod/%C3%A9/uuid?value=%20%0D%0A%09"
        );
    }
}
