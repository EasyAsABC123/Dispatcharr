//! Initial native API-capability archive transport, ported from Timeshift views.
//! Original Dispatcharr application credit belongs to upstream contributors.
//! Full direct/XC, seek/adoption/admission/statistics parity remains unfinished.
use crate::{
    ArchiveClient, ArchiveOpen, ArchivePool, ArchivePoolConfig, ArchivePoolRequest, ArchiveRequest,
    ArchiveTicket, CatchupSessions, CatchupSource, SessionBinding, SqlCatchupResolver,
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
use dispatcharr_contracts::{BoxFuture, PlaybackRequest};
use dispatcharr_coordination::Coordinator;
use dispatcharr_store::{AuthService, Store, StoreError, User};
use dispatcharr_timeshift_core::{
    downstream_length_headers, parse_client_range, representation_length, LengthRequest,
};
use futures_util::stream;
use serde_json::json;
use std::{
    collections::BTreeMap,
    io,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::mpsc;

pub trait ArchiveResolver: Send + Sync {
    fn authenticate<'a>(
        &'a self,
        request: &'a PlaybackRequest,
    ) -> BoxFuture<'a, Result<Option<User>, StoreError>>;
    fn network<'a>(
        &'a self,
        request: &'a PlaybackRequest,
    ) -> BoxFuture<'a, Result<bool, StoreError>>;
    fn source(&self, binding: SessionBinding) -> BoxFuture<'_, Result<CatchupSource, StoreError>>;
    fn client_ip(&self, request: &PlaybackRequest) -> IpAddr {
        request.peer_ip
    }
}
pub struct SqlArchiveResolver {
    store: Store,
    auth: AuthService,
    source: SqlCatchupResolver,
}
impl SqlArchiveResolver {
    pub fn new(store: Store, auth: AuthService) -> Self {
        Self {
            source: SqlCatchupResolver::new(store.clone()),
            store,
            auth,
        }
    }
}
impl ArchiveResolver for SqlArchiveResolver {
    fn authenticate<'a>(
        &'a self,
        request: &'a PlaybackRequest,
    ) -> BoxFuture<'a, Result<Option<User>, StoreError>> {
        Box::pin(async move {
            let user = self.auth.authenticate(request).await?;
            if user.is_some() {
                return Ok(user);
            }
            if let Some(token) = request.parameter("token").filter(|value| !value.is_empty()) {
                if self.auth.decode_token(token, "access").is_ok() {
                    let mut adapted = request.clone();
                    adapted
                        .headers
                        .retain(|(name, _)| !name.eq_ignore_ascii_case("authorization"));
                    adapted
                        .headers
                        .push(("authorization".into(), format!("Bearer {token}")));
                    return self.auth.authenticate(&adapted).await;
                }
            }
            Ok(None)
        })
    }
    fn network<'a>(
        &'a self,
        request: &'a PlaybackRequest,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            self.store
                .network_allowed(self.auth.client_ip(request), "STREAMS", None)
                .await
        })
    }
    fn source(&self, binding: SessionBinding) -> BoxFuture<'_, Result<CatchupSource, StoreError>> {
        Box::pin(async move {
            self.source
                .resolve(
                    binding.user,
                    binding.channel,
                    binding.start,
                    binding.duration.as_deref(),
                )
                .await
        })
    }
    fn client_ip(&self, request: &PlaybackRequest) -> IpAddr {
        self.auth.client_ip(request)
    }
}
#[derive(Clone)]
pub struct CatchupMediaConfig {
    pub pool: ArchivePoolConfig,
    pub max_upstreams: usize,
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    pub body_frame_bytes: usize,
}
impl Default for CatchupMediaConfig {
    fn default() -> Self {
        Self {
            pool: ArchivePoolConfig::default(),
            max_upstreams: 1024,
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(10),
            body_frame_bytes: 65536,
        }
    }
}
struct Inner {
    resolver: Arc<dyn ArchiveResolver>,
    sessions: CatchupSessions,
    pool: ArchivePool,
    client: ArchiveClient,
    config: CatchupMediaConfig,
}
#[derive(Clone)]
pub struct CatchupMedia {
    inner: Arc<Inner>,
}
impl CatchupMedia {
    pub async fn new(
        store: Store,
        auth: AuthService,
        sessions: CatchupSessions,
        coordinator: Coordinator,
        redis_url: &str,
        config: CatchupMediaConfig,
    ) -> Result<Self, StoreError> {
        Self::with_resolver(
            Arc::new(SqlArchiveResolver::new(store, auth)),
            sessions,
            coordinator,
            redis_url,
            config,
        )
        .await
    }
    pub async fn with_resolver(
        resolver: Arc<dyn ArchiveResolver>,
        sessions: CatchupSessions,
        coordinator: Coordinator,
        redis_url: &str,
        config: CatchupMediaConfig,
    ) -> Result<Self, StoreError> {
        if config.body_frame_bytes == 0 || config.body_frame_bytes > 262144 {
            return Err(StoreError::Unsupported("invalid archive body frame budget"));
        }
        let client = ArchiveClient::new(
            config.max_upstreams,
            config.connect_timeout,
            config.read_timeout,
        )
        .map_err(StoreError::Unsupported)?;
        let pool = ArchivePool::connect(redis_url, coordinator, config.pool.clone()).await?;
        Ok(Self {
            inner: Arc::new(Inner {
                resolver,
                sessions,
                pool,
                client,
                config,
            }),
        })
    }
    pub fn router(self) -> Router {
        Router::new()
            .route("/proxy/catchup/{channel}", get(play).head(head))
            .with_state(self)
    }
    pub async fn shutdown(&self) -> bool {
        self.inner.pool.shutdown().await
    }
    pub async fn respond(&self, channel: uuid::Uuid, request: PlaybackRequest) -> Response {
        match self.response(channel, request).await {
            Ok(response) => response,
            Err(error) => error_response(error),
        }
    }
    async fn response(
        &self,
        channel: uuid::Uuid,
        request: PlaybackRequest,
    ) -> Result<Response, StoreError> {
        let authenticated = self.inner.resolver.authenticate(&request).await?;
        if !self.inner.resolver.network(&request).await? {
            return Err(StoreError::Forbidden);
        }
        let Some(sid) = request
            .parameter("session_id")
            .filter(|value| !value.is_empty())
        else {
            return Err(if authenticated.is_none() {
                StoreError::StatusBody(401, json!({"error":"Authentication required"}))
            } else {
                StoreError::Unsupported("direct-auth archive mint/adoption is not yet implemented")
            });
        };
        let Some(binding) = self.inner.sessions.resolve(sid, channel).await? else {
            return Err(if authenticated.is_none() {
                StoreError::StatusBody(401, json!({"error":"Invalid or expired playback session"}))
            } else {
                StoreError::Unsupported(
                    "direct-auth archive pool restoration is not yet implemented",
                )
            });
        };
        if authenticated
            .as_ref()
            .is_some_and(|user| user.id != binding.user.id)
        {
            return Err(StoreError::StatusBody(403, json!("Access denied")));
        }
        let source = self.inner.resolver.source(binding).await?;
        let range = request.header("range").filter(|value| !value.is_empty());
        let mut last_failure = StoreError::StatusBody(503, json!("No available stream slot"));
        let mut rejected_account = None;
        for candidate in &source.candidates {
            if rejected_account == Some(candidate.account_id) {
                continue;
            }
            let pool_request = descriptor(
                sid,
                &source,
                candidate,
                &request,
                self.inner.resolver.client_ip(&request),
            );
            let ticket = match self.inner.pool.open(pool_request).await {
                Ok(ticket) => ticket,
                Err(StoreError::StatusBody(503, message))
                    if message == json!("No available stream") =>
                {
                    continue
                }
                Err(error) => return Err(error),
            };
            let cached = self.inner.pool.format_index(candidate.account_id).await;
            let setup_stopped = ticket.stopped();
            let setup_lost = ticket.lease_lost();
            let opened = tokio::select! {
                result=self.inner.client.open(ArchiveRequest {urls:&candidate.urls,cached_format:cached,final_url:None,user_agent:&candidate.user_agent,range})=>result,
                _=wait_stopped(setup_stopped)=>{ticket.abort().await?;return Err(StoreError::StatusBody(503,json!("Archive session stopped")));},
                _=wait_stopped(setup_lost)=>{ticket.abort().await?;return Err(StoreError::StatusBody(503,json!("Archive provider lease lost")));},
            };
            match opened {
                Ok(ArchiveOpen::Unsatisfiable { content_range }) => {
                    ticket.abort().await?;
                    let mut response = Response::builder()
                        .status(416)
                        .header("accept-ranges", "bytes")
                        .header("allow", "GET, OPTIONS")
                        .header("x-content-type-options", "nosniff");
                    if let Some(value) = content_range {
                        response = response.header("content-range", value);
                    }
                    return response
                        .body(Body::empty())
                        .map_err(|_| StoreError::Unsupported("invalid provider range header"));
                }
                Ok(ArchiveOpen::Media(media)) => {
                    if let Some(index) = media.format_index {
                        self.inner
                            .pool
                            .remember_format(candidate.account_id, index)
                            .await?;
                    }
                    let status = media.status();
                    let headers = media.headers().clone();
                    let length = representation_length(
                        headers
                            .get("content-range")
                            .and_then(|value| value.to_str().ok()),
                        headers
                            .get("content-length")
                            .and_then(|value| value.to_str().ok()),
                    );
                    let final_url = media.final_url().to_owned();
                    ticket.start(&final_url, length, range).await?;
                    let lengths = downstream_length_headers(LengthRequest {
                        range,
                        status: status.as_u16(),
                        representation_length: length,
                        upstream_range: headers
                            .get("content-range")
                            .and_then(|value| value.to_str().ok()),
                        upstream_length: headers
                            .get("content-length")
                            .and_then(|value| value.to_str().ok()),
                        streaming: true,
                    })
                    .map_err(|_| {
                        StoreError::Unsupported(
                            "provider range integer exceeds bounded implementation",
                        )
                    })?;
                    let mut downstream = HeaderMap::new();
                    downstream.insert(
                        "content-type",
                        headers
                            .get("content-type")
                            .cloned()
                            .unwrap_or_else(|| axum::http::HeaderValue::from_static("video/mp2t")),
                    );
                    downstream.insert(
                        "x-accel-buffering",
                        axum::http::HeaderValue::from_static("no"),
                    );
                    downstream.insert(
                        "x-content-type-options",
                        axum::http::HeaderValue::from_static("nosniff"),
                    );
                    downstream.insert(
                        "allow",
                        axum::http::HeaderValue::from_static("GET, OPTIONS"),
                    );
                    for (key, value) in lengths {
                        downstream.insert(
                            axum::http::HeaderName::from_bytes(key.as_bytes()).map_err(|_| {
                                StoreError::Unsupported("invalid provider length header")
                            })?,
                            value.parse().map_err(|_| {
                                StoreError::Unsupported("invalid provider length header")
                            })?,
                        );
                    }
                    return Ok((
                        status,
                        downstream,
                        supervised_body(media, ticket, self.inner.config.body_frame_bytes),
                    )
                        .into_response());
                }
                Err(error) => {
                    if error.decisive {
                        rejected_account = Some(candidate.account_id);
                    }
                    last_failure =
                        StoreError::StatusBody(error.status.as_u16(), json!(error.message));
                    ticket.abort().await?;
                }
            }
        }
        Err(last_failure)
    }
}
async fn wait_stopped(mut receiver: tokio::sync::watch::Receiver<bool>) {
    let _ = receiver.wait_for(|value| *value).await;
}
fn descriptor(
    sid: &str,
    source: &CatchupSource,
    candidate: &crate::CatchupCandidate,
    request: &PlaybackRequest,
    ip: IpAddr,
) -> ArchivePoolRequest {
    let safe = source.timestamp.replace([':', '/'], "-");
    let media_id = format!("{}_{}", source.channel.id, safe);
    let virtual_channel_id = format!("{}_{}", media_id, candidate.provider_stream_id);
    let pool_fields = BTreeMap::from([
        ("media_id".into(), media_id),
        ("user_id".into(), source.user.id.to_string()),
        ("client_ip".into(), ip.to_string()),
        (
            "client_user_agent".into(),
            request.header("user-agent").unwrap_or("").into(),
        ),
        ("account_id".into(), candidate.account_id.to_string()),
        ("profile_id".into(), candidate.profile_id.to_string()),
        ("stream_id".into(), candidate.provider_stream_id.clone()),
        (
            "dispatcharr_stream_id".into(),
            candidate.stream_id.to_string(),
        ),
        (
            "provider_timestamp".into(),
            candidate.provider_timestamp.clone(),
        ),
        (
            "provider_tz_name".into(),
            candidate.provider_timezone.clone().unwrap_or_default(),
        ),
    ]);
    let mut client_fields = BTreeMap::from([
        (
            "user_agent".into(),
            request
                .header("user-agent")
                .filter(|value| !value.is_empty())
                .unwrap_or("unknown")
                .into(),
        ),
        ("ip_address".into(), ip.to_string()),
        ("user_id".into(), source.user.id.to_string()),
        ("username".into(), source.user.username.clone()),
        ("programme_start".into(), source.timestamp.clone()),
        ("programme_vid".into(), virtual_channel_id.clone()),
    ]);
    if let Some((start, _)) = parse_client_range(request.header("range")) {
        if start == 0 {
            client_fields.insert("playback_base_secs".into(), "0.0".into());
        }
    }
    let channel_fields = BTreeMap::from([
        ("channel_id".into(), source.channel.id.to_string()),
        ("channel_uuid".into(), source.channel.uuid.to_string()),
        ("channel_name".into(), source.channel.effective_name.clone()),
        (
            "stream_name".into(),
            format!("Catch-up @ {} UTC", source.timestamp),
        ),
        (
            "url".into(),
            redacted(candidate.urls.first().map(String::as_str).unwrap_or("")),
        ),
        ("m3u_profile".into(), candidate.profile_id.to_string()),
    ]);
    ArchivePoolRequest {
        session_id: sid.into(),
        channel_id: source.channel.id,
        virtual_channel_id,
        provider_profile_id: candidate.profile_id,
        provider_max_streams: candidate.max_streams,
        stream_limit: source.user.stream_limit.max(0) as u32,
        limit_policy: source.limit_policy.clone(),
        pool_fields,
        client_fields,
        channel_fields,
    }
}
fn redacted(value: &str) -> String {
    url::Url::parse(value)
        .ok()
        .and_then(|url| {
            url.host_str()
                .map(|host| format!("{}://{host}/...", url.scheme()))
        })
        .unwrap_or_default()
}
struct AbortBody(tokio::task::AbortHandle);
impl Drop for AbortBody {
    fn drop(&mut self) {
        self.0.abort();
    }
}
fn supervised_body(
    mut media: crate::ArchiveMedia,
    ticket: ArchiveTicket,
    frame_bytes: usize,
) -> Body {
    let (tx, rx) = mpsc::channel::<Result<Bytes, io::Error>>(1);
    let mut stopped = ticket.stopped();
    let mut lost = ticket.lease_lost();
    let task = tokio::spawn(async move {
        let stream = async {
            while let Ok(Some(chunk)) = media.next_chunk().await {
                let mut offset = 0;
                while offset < chunk.len() {
                    let end = (offset + frame_bytes).min(chunk.len());
                    let frame = chunk.slice(offset..end);
                    let count = frame.len();
                    if tx.send(Ok(frame)).await.is_err() {
                        return;
                    }
                    ticket.forwarded(count);
                    offset = end;
                }
            }
        };
        let heartbeat = async {
            let mut tick = tokio::time::interval(Duration::from_millis(500));
            let mut last_stats = Instant::now();
            loop {
                tick.tick().await;
                if !matches!(ticket.alive().await, Ok(true)) {
                    return;
                }
                if last_stats.elapsed() >= Duration::from_secs(5) {
                    if !matches!(ticket.heartbeat().await, Ok(true)) {
                        return;
                    }
                    last_stats = Instant::now();
                }
            }
        };
        tokio::select! {_=stream=>{},_=heartbeat=>{},_=stopped.wait_for(|value|*value)=>{},_=lost.wait_for(|value|*value)=>{}}
        if ticket.total_bytes() > 0 {
            let _ = ticket.heartbeat().await;
        }
        drop(media);
        drop(ticket);
    });
    let abort = AbortBody(task.abort_handle());
    Body::from_stream(stream::unfold(
        (rx, abort),
        |(mut receiver, abort)| async move {
            receiver.recv().await.map(|item| (item, (receiver, abort)))
        },
    ))
}
fn error_response(error: StoreError) -> Response {
    let mut response = match error {
        StoreError::StatusBody(status, serde_json::Value::String(message)) => (
            StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            [("content-type", "text/html; charset=utf-8")],
            message,
        )
            .into_response(),
        error => error.into_response(),
    };
    response.headers_mut().insert(
        "allow",
        axum::http::HeaderValue::from_static("GET, OPTIONS"),
    );
    response.headers_mut().insert(
        "x-content-type-options",
        axum::http::HeaderValue::from_static("nosniff"),
    );
    response
}
async fn play(
    State(engine): State<CatchupMedia>,
    Path(channel): Path<String>,
    request: Request<Body>,
) -> Response {
    let Some(peer) = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|info| info.0.ip())
    else {
        return error_response(StoreError::Unsupported(
            "HTTP socket peer metadata is required",
        ));
    };
    let Ok(channel) = uuid::Uuid::parse_str(&channel) else {
        return error_response(StoreError::NotFound);
    };
    let identity = PlaybackRequest {
        channel_id: channel.to_string(),
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
                    .map(|value| (key.as_str().into(), value.into()))
            })
            .collect(),
        peer_ip: peer,
    };
    engine.respond(channel, identity).await
}
async fn head() -> Response {
    (
        StatusCode::METHOD_NOT_ALLOWED,
        [
            ("allow", "GET, OPTIONS"),
            ("x-content-type-options", "nosniff"),
        ],
        axum::Json(json!({"detail":"Method \"HEAD\" not allowed."})),
    )
        .into_response()
}
