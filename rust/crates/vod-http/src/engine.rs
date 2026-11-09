//! Ported from Dispatcharr's VOD view/session behavior. Original authors and
//! contributors retain full credit; see repository ATTRIBUTION.md and LICENSE.
use crate::{resolver::*, sessions::{SessionPool, Ticket}};
use axum::{body::Body, extract::{ConnectInfo, Path, State}, http::{HeaderMap, Request, StatusCode}, response::{IntoResponse, Response}, routing::get, Router};
use bytes::Bytes;
use dispatcharr_contracts::PlaybackRequest;
use dispatcharr_coordination::Coordinator;
use dispatcharr_store::{AuthService, Store, StoreError};
use futures_util::{stream, StreamExt};
use serde_json::json;
use std::{collections::HashMap, io, net::{IpAddr, Ipv4Addr, SocketAddr}, sync::{Arc, atomic::Ordering}, time::Duration};

#[derive(Clone)]
pub struct VodConfig {
    pub max_sessions: usize,
    pub max_requests: usize,
    pub idle_grace: Duration,
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    /// Upper bound on transport bytes exposed in one body frame.
    pub frame_bytes: usize,
}
impl Default for VodConfig {
    fn default() -> Self { Self { max_sessions: 1024, max_requests: 2048, idle_grace: Duration::from_secs(1), connect_timeout: Duration::from_secs(5), read_timeout: Duration::from_secs(15), frame_bytes: 64*1024 } }
}
struct Inner { resolver: Arc<dyn VodResolver>, sessions: SessionPool, client: reqwest::Client, config: VodConfig }
#[derive(Clone)]
pub struct VodEngine { inner: Arc<Inner> }
impl VodEngine {
    pub async fn new(store: Store, auth: AuthService, coordinator: Coordinator, redis_url: &str, config: VodConfig) -> Result<Self, StoreError> {
        Self::with_resolver(Arc::new(SqlVodResolver::new(store, auth)), coordinator, redis_url, config).await
    }
    pub async fn with_resolver(resolver: Arc<dyn VodResolver>, coordinator: Coordinator, redis_url: &str, config: VodConfig) -> Result<Self, StoreError> {
        if config.max_sessions == 0 || config.max_requests == 0 || config.frame_bytes == 0 || config.frame_bytes > 1024*1024 || config.read_timeout.is_zero() || config.connect_timeout.is_zero() {
            return Err(StoreError::Unsupported("Invalid bounded VOD configuration"));
        }
        let sessions = SessionPool::connect(redis_url, coordinator, config.max_sessions, config.max_requests, config.idle_grace).await?;
        let client = reqwest::Client::builder().connect_timeout(config.connect_timeout).read_timeout(config.read_timeout)
            .redirect(reqwest::redirect::Policy::limited(5)).pool_max_idle_per_host(4).build()
            .map_err(|_| StoreError::Unsupported("VOD HTTP client setup failed"))?;
        Ok(Self { inner: Arc::new(Inner { resolver, sessions, client, config }) })
    }
    pub fn router(self) -> Router {
        Router::new()
            .route("/proxy/vod/stop_client/", axum::routing::post(stop_client))
            .route("/proxy/vod/{kind}/{content_id}", get(play).head(head))
            .route("/proxy/vod/{kind}/{content_id}/{session_id}", get(play).head(head))
            .route("/proxy/vod/{kind}/{content_id}/{profile_id}/", get(play).head(head))
            .route("/proxy/vod/{kind}/{content_id}/{session_id}/{profile_id}/", get(play).head(head))
            .with_state(self)
    }
    pub async fn respond(&self, mut request: VodRequest) -> Response {
        match self.prepare(&mut request).await { Ok(response) => response, Err(error) => error_response(error) }
    }
    async fn prepare(&self, request: &mut VodRequest) -> Result<Response, StoreError> {
        let mut user = self.inner.resolver.authorize(request).await?;
        if let Some(id) = &request.session_id {
            if !valid_session(id) { return Err(StoreError::NotFound); }
            if user.is_none() {
                if let Some(user_id) = self.inner.sessions.user_for_session(id).await? {
                    user = self.inner.resolver.restore_user(user_id).await?;
                }
            }
        }
        // Django mints the redirect before resolving a UUID/content record.
        if request.session_id.is_none() && !self.inner.resolver.default_redirect().await? {
            let session = format!("vod_{}_{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs(), &uuid::Uuid::new_v4().simple().to_string()[..4]);
            if let Some(user) = user { self.inner.sessions.remember_user(&session, user.id).await?; }
            let mut location = format!("/proxy/vod/{}/{}/{}", request.kind, request.content_id, session);
            if let Some(profile) = request.profile_id { location.push_str(&format!("/{profile}/")); }
            let query = url::form_urlencoded::Serializer::new(String::new()).extend_pairs(request.playback.query.iter().filter(|(key,_)| key != "session_id" && key != "token").map(|(key,value)| (key,value))).finish();
            if !query.is_empty() { location.push('?'); location.push_str(&query); }
            return Ok(Response::builder().status(301).header("location", location).header("cache-control", if request.playback.query.iter().any(|(key,_)| key == "token") { "no-store" } else { "private, max-age=21600" }).body(Body::empty()).unwrap());
        }
        let source = self.inner.resolver.resolve(request, user).await?;
        if source.redirect && request.session_id.is_none() {
            // Scoped lease checks shared capacity; redirects do not hold a slot.
            // Existing idle-session adoption remains an explicit M4 gate.
            let location = self.inner.sessions.redirect_candidate(&source).await?;
            return Ok(Response::builder().status(302).header("location", location).body(Body::empty()).unwrap());
        }
        let id = request.session_id.as_deref().ok_or(StoreError::NotFound)?;
        let ticket = self.inner.sessions.enter(id, source, request).await?;
        let mut upstream_request = self.inner.client.get(&ticket.session.candidate.url).header("user-agent", &ticket.session.candidate.user_agent).header("accept-encoding", "identity");
        for name in ["range", "if-range", "if-none-match", "if-modified-since", "accept", "referer", "origin"] {
            if let Some(value) = request.playback.header(name) { upstream_request = upstream_request.header(name, value); }
        }
        let upstream = tokio::select! {
            response = upstream_request.send() => response.map_err(|_| StoreError::StatusBody(502,json!({"error":"VOD provider unavailable"})))?,
            _ = stopped(&ticket) => return Err(StoreError::StatusBody(503,json!({"error":"VOD session stopped"}))),
        };
        let status = upstream.status();
        let headers = forwarded_headers(upstream.headers());
        let metadata = [("content_length", upstream.content_length().map(|v|v.to_string()).unwrap_or_default()),
            ("content_type", upstream.headers().get("content-type").and_then(|v|v.to_str().ok()).unwrap_or("").into()),
            ("final_url", upstream.url().as_str().into())];
        self.inner.sessions.update(&ticket.session, &metadata).await?;
        // Provider status is preserved, including genuine 206/416. Never invent
        // 206 simply because a client sent Range (the Python source does).
        let state = StreamState { upstream: Box::pin(upstream.bytes_stream()), pending: Bytes::new(), ticket, frame_bytes: self.inner.config.frame_bytes };
        let body = stream::try_unfold(state, |mut state| async move {
            loop {
                if *state.ticket.session.stop.borrow() { return Err(io::Error::new(io::ErrorKind::Interrupted, "VOD session stopped")); }
                if !state.pending.is_empty() {
                    let chunk = state.pending.split_to(state.pending.len().min(state.frame_bytes));
                    state.ticket.session.bytes.fetch_add(chunk.len() as u64, Ordering::Relaxed);
                    return Ok(Some((chunk, state)));
                }
                let mut stop = state.ticket.session.stop.subscribe();
                let next = tokio::select! {
                    result = state.upstream.next() => result,
                    _ = stop.changed() => return Err(io::Error::new(io::ErrorKind::Interrupted, "VOD session stopped")),
                };
                match next {
                    Some(Ok(chunk)) => state.pending = chunk,
                    Some(Err(_)) => return Err(io::Error::new(io::ErrorKind::ConnectionReset, "VOD provider body failed")),
                    None => return Ok(None),
                }
            }
        });
        let mut response = Response::new(Body::from_stream(body));
        *response.status_mut() = status;
        *response.headers_mut() = headers;
        Ok(response)
    }
}
type ProviderStream = std::pin::Pin<Box<dyn futures_util::Stream<Item=Result<Bytes,reqwest::Error>> + Send>>;
struct StreamState { upstream: ProviderStream, pending: Bytes, ticket: Ticket, frame_bytes: usize }
async fn stopped(ticket: &Ticket) { let mut stop = ticket.session.stop.subscribe(); if *stop.borrow() { return; } while stop.changed().await.is_ok() { if *stop.borrow() { return; } } }
fn forwarded_headers(source: &HeaderMap) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for name in ["content-type", "content-length", "content-range", "accept-ranges", "etag", "last-modified", "cache-control", "expires", "content-disposition", "content-encoding", "vary"] {
        if let Some(value) = source.get(name) { headers.insert(axum::http::HeaderName::from_static(name), value.clone()); }
    }
    headers
}
fn error_response(error: StoreError) -> Response {
    if let StoreError::StatusBody(503, serde_json::Value::String(ref value)) = error {
        if value == "No available stream" { return (StatusCode::SERVICE_UNAVAILABLE, [("content-type", "text/html; charset=utf-8")], "No available stream").into_response(); }
    }
    error.into_response()
}
fn valid_session(value: &str) -> bool { !value.is_empty() && value.len() <= 128 && value.bytes().all(|v| v.is_ascii_alphanumeric() || v == b'_' || v == b'-') }
fn playback(request: &Request<Body>, content: String) -> PlaybackRequest {
    PlaybackRequest { channel_id: content, query: url::form_urlencoded::parse(request.uri().query().unwrap_or("").as_bytes()).into_owned().collect(),
        headers: request.headers().iter().filter_map(|(key,value)| value.to_str().ok().map(|value|(key.as_str().to_owned(),value.to_owned()))).collect(),
        peer_ip: request.extensions().get::<ConnectInfo<SocketAddr>>().map(|ip|ip.0.ip()).unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST)) }
}
async fn play(State(engine): State<VodEngine>, Path(path): Path<HashMap<String,String>>, request: Request<Body>) -> Response {
    let content_id = path.get("content_id").cloned().unwrap_or_default();
    engine.respond(VodRequest { playback: playback(&request, content_id.clone()), kind: path.get("kind").cloned().unwrap_or_default(), content_id,
        session_id: path.get("session_id").cloned(), profile_id: path.get("profile_id").and_then(|value|value.parse().ok()) }).await
}
async fn head() -> Response { (StatusCode::METHOD_NOT_ALLOWED, [("allow","GET, OPTIONS")], axum::Json(json!({"detail":"Method \"HEAD\" not allowed."}))).into_response() }
async fn stop_client(State(engine): State<VodEngine>, request: Request<Body>) -> Response {
    let identity = playback(&request,String::new());
    if let Err(error) = engine.inner.resolver.admin(&identity).await { return error_response(error); }
    let body = match axum::body::to_bytes(request.into_body(),16*1024).await { Ok(value)=>value,Err(_)=>return (StatusCode::BAD_REQUEST,axum::Json(json!({"error":"Invalid JSON"}))).into_response() };
    let value: serde_json::Value = match serde_json::from_slice(&body) { Ok(value)=>value,Err(_)=>return (StatusCode::BAD_REQUEST,axum::Json(json!({"error":"Invalid JSON"}))).into_response() };
    let Some(id) = value.get("client_id").and_then(|v|v.as_str()).filter(|v|!v.is_empty()) else { return (StatusCode::BAD_REQUEST,axum::Json(json!({"error":"No client_id provided"}))).into_response(); };
    match engine.inner.sessions.stop(id).await {
        Ok(true)=>(StatusCode::OK,axum::Json(json!({"message":"VOD client stop signal sent","client_id":id,"stop_key":format!("vod_proxy:client:{id}:stop")}))).into_response(),
        Ok(false)=>(StatusCode::NOT_FOUND,axum::Json(json!({"error":"Connection not found"}))).into_response(),
        Err(error)=>error_response(error),
    }
}
