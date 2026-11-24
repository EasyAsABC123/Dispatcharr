//! Ported from Dispatcharr's VOD view/session behavior. Original authors and
//! contributors retain full credit; see repository ATTRIBUTION.md and LICENSE.
use crate::{
    resolver::*,
    sessions::{SessionPool, Ticket},
};
use axum::{
    body::Body,
    extract::{ConnectInfo, Path, State},
    http::{header, HeaderMap, HeaderValue, Request, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use bytes::Bytes;
use dispatcharr_contracts::PlaybackRequest;
use dispatcharr_coordination::Coordinator;
use dispatcharr_store::{AuthService, RequestAuthMethod, Store, StoreError, User};
use futures_util::{stream, StreamExt};
use serde_json::json;
use std::{
    collections::HashMap,
    future::{poll_fn, Future},
    io,
    net::SocketAddr,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

fn signed_restart_subject(user: Option<&User>, method: Option<RequestAuthMethod>) -> Option<i64> {
    match method {
        Some(RequestAuthMethod::SignedBearerHeader) => user.map(|user| user.id),
        _ => None,
    }
}

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
        crate::worker_identity::initialize();
        let sessions = SessionPool::connect(
            redis_url,
            coordinator,
            config.max_sessions,
            config.max_requests,
            config.idle_grace,
        )
        .await?;
        let builder = reqwest::Client::builder()
            .connect_timeout(config.connect_timeout)
            .read_timeout(config.read_timeout)
            .redirect(reqwest::redirect::Policy::limited(30))
            .pool_max_idle_per_host(4);
        let client = dispatcharr_upstream_http::with_playback_requests_trust(builder)
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
    /// Opt-in recovery bridge for the wider generation and entry cases.
    pub fn with_redis_recovery(self, namespace: &str) -> Result<Self, StoreError> {
        self.inner.sessions.enable_recovery(namespace)?;
        Ok(self)
    }
    /// Limit ordinary Redis restart recovery to explicitly reauthenticated
    /// local sessions with the same content and retained provider.
    pub fn with_authenticated_redis_restart(self, namespace: &str) -> Result<Self, StoreError> {
        self.inner
            .sessions
            .enable_authenticated_restart(namespace)?;
        Ok(self)
    }
    /// Attach the bounded original lifecycle publisher before any playback.
    pub async fn with_events(
        self,
        store: Store,
        auth: AuthService,
        redis_url: &str,
        sink: Arc<dyn dispatcharr_contracts::UpdateEventSink>,
    ) -> Result<Self, StoreError> {
        let producer = crate::events::Producer::connect(store, auth, redis_url, sink, 2048).await?;
        self.inner.sessions.install_events(producer)?;
        Ok(self)
    }
    pub fn event_diagnostics(&self) -> serde_json::Value {
        self.inner.sessions.event_diagnostics()
    }
    /// Stop admission and every supervised provider body under one global
    /// deadline. False leaves ledger/reaper recovery necessary after an outage.
    pub async fn shutdown(&self) -> bool {
        let deadline = tokio::time::Instant::now() + self.inner.config.shutdown_timeout;
        self.inner.sessions.stop_admission();
        let drained = tokio::time::timeout_at(deadline, self.inner.sessions.drain())
            .await
            .is_ok();
        if !drained {
            self.inner.sessions.cancel_events();
            return false;
        }
        let events = self
            .inner
            .sessions
            .finish_events(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await;
        events && self.inner.sessions.cleanup_succeeded()
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
    /// Trusted in-process entry for the XC gateway. That gateway checks XC_API
    /// credentials and the per-kind VOD flag on every request, including follows.
    pub async fn respond_xc(&self, mut request: VodRequest, user: User, path: &str) -> Response {
        if let Err(error) = self.inner.resolver.authenticate_method(&request).await {
            return error_response(error);
        }
        match self.prepare_entry(&mut request, Some((user, path))).await {
            Ok(response) => response,
            Err(error) => error_response(error),
        }
    }

    async fn prepare(&self, request: &mut VodRequest) -> Result<Response, StoreError> {
        self.prepare_entry(request, None).await
    }

    async fn prepare_entry(
        &self,
        request: &mut VodRequest,
        xc: Option<(User, &str)>,
    ) -> Result<Response, StoreError> {
        if self.inner.sessions.stopped() {
            return Err(StoreError::StatusBody(
                503,
                json!({"error":"VOD is shutting down"}),
            ));
        }
        let (mut user, xc_path, method) = match xc {
            Some((user, path)) => (Some(user), Some(path), None),
            None => {
                let (user, method) = self.inner.resolver.authorize_entry(request).await?;
                (user, None, method)
            }
        };
        let restart_user = signed_restart_subject(user.as_ref(), method);
        let redirect = |request: &VodRequest, session: &str| match xc_path {
            Some(path) => xc_session_redirect(request, session, path),
            None => session_redirect(request, session),
        };
        request.resolved_client_ip = Some(self.inner.resolver.client_ip_text(&request.playback));
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
            return Ok(redirect(request, &session));
        }
        let source = self.inner.resolver.resolve(request, user).await?;
        if source.redirect && request.session_id.is_none() {
            if let Some(id) = self.inner.sessions.idle_session(&source, request).await {
                if let Some(user) = &source.user {
                    self.inner.sessions.remember_user(&id, user.id).await?;
                }
                return Ok(redirect(request, &id));
            }
            // Source Redirect checks capacity without probing or reserving.
            // Existing remote idle-session adoption remains an explicit gate.
            let location = self.inner.sessions.redirect_candidate(&source).await?;
            return Ok(provider_redirect(&location));
        }
        let id = request.session_id.as_deref().ok_or(StoreError::NotFound)?;
        let ticket = self
            .inner
            .sessions
            .enter_with_resolver(
                id,
                source,
                request,
                self.inner.resolver.clone(),
                restart_user,
            )
            .await?;
        let provider_url = ticket.session.provider_url();
        let mut upstream_request = self
            .inner
            .client
            .get(provider_url.as_ref())
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
        let content_type = vod_content_type(upstream.headers(), &ticket.session.candidate.url);
        let metadata = [
            ("content_length", total.to_string()),
            ("content_type", content_type.to_str().unwrap_or("").into()),
            ("final_url", upstream.url().as_str().into()),
        ];
        self.inner
            .sessions
            .update(&ticket.session, &metadata)
            .await?;
        let headers = successful_headers(content_type, original_range, total)?;
        // Preserve Python's Range response contract, including its documented
        // suffix/conditional quirks. Correcting these is outside the 1:1 port.
        // A single bounded channel decouples cancellation from downstream body
        // polls. A stalled viewer must not keep a stopped provider lease alive.
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let stop = ticket.session.stop.subscribe();
        let retry = RetryRequest {
            client: self.inner.client.clone(),
            final_url: upstream.url().as_str().into(),
            original_url: provider_url.into_owned(),
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
        let provider_body_length = upstream.content_length();
        let progress = ticket.progress();
        let (start_body, body_started) = tokio::sync::oneshot::channel();
        // Upstream EOF can leave a frame queued for the downstream body.
        // Keep the reader Ticket through that frame's body completion.
        let (finish_body, body_finished) = tokio::sync::oneshot::channel();
        let scoped_cancellation = ticket.source_blocks();
        let task = tokio::spawn(pump(
            upstream,
            ticket,
            sender,
            self.inner.config.clone(),
            retry,
            body_started,
            body_finished,
        ));
        let state = BodyState {
            receiver,
            stop,
            _scoped_cancel: scoped_cancellation.then(|| CancelTask(task)),
            start_body: Some(start_body),
            finish_body: Some(finish_body),
            progress,
            pending_bytes: 0,
            remaining: provider_body_length,
        };
        let body = stream::try_unfold(state, |mut state| async move {
            if state.progress.acknowledge(state.pending_bytes).await {
                state.finish();
                return Ok(None);
            }
            state.pending_bytes = 0;
            if let Some(start) = state.start_body.take() {
                let _ = start.send(());
            }
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
                Some(Ok(chunk)) => {
                    state.remaining = state
                        .remaining
                        .and_then(|left| left.checked_sub(chunk.len() as u64));
                    if state.remaining == Some(0) {
                        // Hyper can retire a known-length body after this frame
                        // without another EOF poll. Commit its final Source
                        // generator yield here so that progress is not lost.
                        if !state.progress.acknowledge(chunk.len()).await {
                            state.progress.finish().await;
                        }
                        state.finish();
                    } else {
                        state.pending_bytes = chunk.len();
                    }
                    Ok(Some((chunk, state)))
                }
                Some(Err(error)) => Err(error),
                None => {
                    state.progress.finish().await;
                    state.finish();
                    Ok(None)
                }
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
    // Drop completion first, so closing the receiver can distinguish a
    // client departure from a finished body even on another runtime thread.
    finish_body: Option<tokio::sync::oneshot::Sender<()>>,
    receiver: tokio::sync::mpsc::Receiver<Result<Bytes, io::Error>>,
    stop: tokio::sync::watch::Receiver<bool>,
    // Recovery keeps its established hard cancellation and ledger ownership.
    _scoped_cancel: Option<CancelTask>,
    start_body: Option<tokio::sync::oneshot::Sender<()>>,
    progress: crate::sessions::Progress,
    pending_bytes: usize,
    remaining: Option<u64>,
}
impl BodyState {
    fn finish(&mut self) {
        if let Some(sender) = self.finish_body.take() {
            let _ = sender.send(());
        }
    }
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

async fn provider_pull<F: Future>(
    next: F,
    downstream_closed: impl Future<Output = ()>,
    stop: &mut tokio::sync::watch::Receiver<bool>,
    read_timeout: Duration,
    claim_departure: impl FnOnce() -> Option<tokio::sync::oneshot::Receiver<()>>,
) -> Option<F::Output> {
    if *stop.borrow() {
        return None;
    }
    let mut next = std::pin::pin!(next);
    let mut began = None;
    tokio::select! {
        biased;
        _ = stop.changed() => return None,
        _ = downstream_closed => {},
        result = poll_fn(|cx| {
            let result = next.as_mut().poll(cx);
            if result.is_pending() {
                began.get_or_insert_with(tokio::time::Instant::now);
            }
            result
        }) => return Some(result),
    }
    // Only a genuinely pending poll can outlive the body. Keep that exact
    // future inside the same pump and deadline, ignoring bytes or errors.
    // This path never resumes the pump, opens another response or sends data.
    let deadline = began?.checked_add(read_timeout)?;
    if *stop.borrow() || tokio::time::Instant::now() >= deadline {
        return None;
    }
    let replacement = claim_departure()?;
    tokio::select! {
        biased;
        _ = stop.changed() => {},
        _ = replacement => {},
        _ = tokio::time::timeout_at(deadline, next) => {},
    }
    None
}

async fn pump(
    upstream: reqwest::Response,
    ticket: Ticket,
    sender: tokio::sync::mpsc::Sender<Result<Bytes, io::Error>>,
    config: VodConfig,
    retry: RetryRequest,
    body_started: tokio::sync::oneshot::Receiver<()>,
    mut body_finished: tokio::sync::oneshot::Receiver<()>,
) {
    let mut stop = ticket.session.stop.subscribe();
    let source_blocks = ticket.source_blocks();
    let mut clean_eof = false;
    async {
    let mut upstream: ProviderStream = Box::pin(upstream.bytes_stream());
    if *stop.borrow() {
        return;
    }
    let started = tokio::select! {result=body_started=>result.is_ok(),_=stop.changed()=>false};
    if !started || *stop.borrow() {
        return;
    }
    tokio::select! {
        biased;
        _ = stop.changed() => return,
        _ = sender.closed() => return,
        _ = ticket.begin_body() => {},
    }
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
        let Some(next) = provider_pull(upstream.next(), sender.closed(), &mut stop, config.read_timeout, || {
            if source_blocks || !matches!(body_finished.try_recv(), Err(tokio::sync::oneshot::error::TryRecvError::Closed)) {
                return None;
            }
            ticket.departing_pull()
        }).await else { return };
        if sender.is_closed() {
            return;
        }
        let Some(next) = next else {
            clean_eof = true;
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
                    let response = tokio::select! {
                        biased;
                        _=stop.changed()=>return,
                        _=sender.closed()=>return,
                        result=retry.reopen(offset,ticket.session.content_length.load(Ordering::Relaxed))=>result,
                    };
                    if let Ok(response) = response {
                        upstream = Box::pin(response.bytes_stream());
                        break;
                    }
                }
                continue;
            }
        };
        while !chunk.is_empty() {
            // Preserve each Source8192-byte logical boundary even when provider
            // network fragments or the configured frame size are irregular.
            // This only splits owned Bytes; it adds no accumulation buffer.
            let boundary=if source_blocks {8192-(sent_bytes%8192)as usize}else{config.frame_bytes};
            let frame = chunk.split_to(chunk.len().min(config.frame_bytes).min(boundary));
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
        }
    }
    }.await;
    if !sender.is_closed() {
        settle_pump_body(sender, body_finished, &mut stop, clean_eof || source_blocks).await;
    }
}

async fn settle_pump_body(
    sender: tokio::sync::mpsc::Sender<Result<Bytes, io::Error>>,
    body_finished: tokio::sync::oneshot::Receiver<()>,
    stop: &mut tokio::sync::watch::Receiver<bool>,
    wait_for_body: bool,
) {
    // Let the receiver observe EOF before waiting for its completion signal.
    drop(sender);
    if wait_for_body && !*stop.borrow() {
        tokio::select! { _ = body_finished => {}, _ = stop.changed() => {} }
    }
}
#[cfg(test)]
mod normal_eof_ownership {
    use super::*;
    use std::task::Poll;

    #[tokio::test]
    async fn closure_before_the_first_read_never_claims_a_departing_pull() {
        let (_owner, mut stop) = tokio::sync::watch::channel(false);
        let result: Option<()> = provider_pull(
            async { panic!("unpolled provider read started") },
            std::future::ready(()),
            &mut stop,
            Duration::from_secs(1),
            || panic!("unstarted read retained"),
        )
        .await;
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn a_departing_read_waits_for_the_same_provider_future_and_discards_its_value() {
        let (_owner, mut stop) = tokio::sync::watch::channel(false);
        let (close, mut closed) = tokio::sync::mpsc::channel::<()>(1);
        let (origin, input) = tokio::sync::oneshot::channel();
        let (_joined, replacement) = tokio::sync::oneshot::channel();
        let read = provider_pull(
            input,
            close.closed(),
            &mut stop,
            Duration::from_secs(1),
            || Some(replacement),
        );
        tokio::pin!(read);
        assert_eq!(futures_util::poll!(&mut read), Poll::Pending);
        closed.close();
        assert_eq!(futures_util::poll!(&mut read), Poll::Pending);
        origin.send(b"owned input").unwrap();
        assert_eq!(read.await, None);
    }

    #[tokio::test]
    async fn a_departed_nonlast_viewer_does_not_retain_its_provider_read() {
        let (_owner, mut stop) = tokio::sync::watch::channel(false);
        let (close, mut closed) = tokio::sync::mpsc::channel::<()>(1);
        let (origin, input) = tokio::sync::oneshot::channel::<()>();
        let read = provider_pull(
            input,
            close.closed(),
            &mut stop,
            Duration::from_secs(1),
            || None,
        );
        tokio::pin!(read);
        assert_eq!(futures_util::poll!(&mut read), Poll::Pending);
        closed.close();
        assert_eq!(read.await, None);
        assert!(origin.is_closed());
    }

    #[tokio::test(start_paused = true)]
    async fn departed_pull_stall_is_bounded_from_first_read_poll() {
        let (_owner, mut stop) = tokio::sync::watch::channel(false);
        let (close, mut closed) = tokio::sync::mpsc::channel::<()>(1);
        let (origin, input) = tokio::sync::oneshot::channel::<()>();
        let (_joined, replacement) = tokio::sync::oneshot::channel();
        let read = provider_pull(
            input,
            close.closed(),
            &mut stop,
            Duration::from_secs(10),
            || Some(replacement),
        );
        tokio::pin!(read);
        assert_eq!(futures_util::poll!(&mut read), Poll::Pending);
        tokio::time::advance(Duration::from_secs(8)).await;
        closed.close();
        assert_eq!(futures_util::poll!(&mut read), Poll::Pending);
        tokio::time::advance(Duration::from_secs(2)).await;
        assert_eq!(read.await, None);
        assert!(origin.is_closed());
    }

    #[tokio::test]
    async fn hard_stop_and_replacement_cancel_the_departing_pull() {
        for hard_stop in [true, false] {
            let (owner, mut stop) = tokio::sync::watch::channel(false);
            let (close, mut closed) = tokio::sync::mpsc::channel::<()>(1);
            let (origin, input) = tokio::sync::oneshot::channel::<()>();
            let (joined, replacement) = tokio::sync::oneshot::channel();
            let read = provider_pull(
                input,
                close.closed(),
                &mut stop,
                Duration::from_secs(10),
                || Some(replacement),
            );
            tokio::pin!(read);
            assert_eq!(futures_util::poll!(&mut read), Poll::Pending);
            closed.close();
            assert_eq!(futures_util::poll!(&mut read), Poll::Pending);
            if hard_stop {
                owner.send_replace(true);
                assert_eq!(read.await, None);
                drop(joined);
            } else {
                drop(joined);
                assert_eq!(read.await, None);
            }
            assert!(origin.is_closed());
        }
    }

    #[tokio::test]
    async fn a_buffered_final_frame_holds_the_retiring_pump_until_body_completion() {
        let (sender, mut body) = tokio::sync::mpsc::channel(1);
        sender
            .send(Ok(Bytes::from_static(b"final frame")))
            .await
            .unwrap();
        let (body_completed, body_finished) = tokio::sync::oneshot::channel();
        let (_session_stop, mut stop) = tokio::sync::watch::channel(false);
        let clean_eof = settle_pump_body(sender, body_finished, &mut stop, true);
        tokio::pin!(clean_eof);

        assert_eq!(futures_util::poll!(&mut clean_eof), Poll::Pending);
        assert_eq!(body.recv().await.unwrap().unwrap(), b"final frame"[..]);
        assert!(body.recv().await.is_none());
        assert_eq!(futures_util::poll!(&mut clean_eof), Poll::Pending);
        body_completed.send(()).unwrap();
        assert_eq!(futures_util::poll!(&mut clean_eof), Poll::Ready(()));
    }

    #[tokio::test]
    async fn a_dropped_body_releases_a_pump_waiting_at_clean_eof() {
        let (sender, mut body) = tokio::sync::mpsc::channel(1);
        let (body_completed, body_finished) = tokio::sync::oneshot::channel();
        let (_session_stop, mut stop) = tokio::sync::watch::channel(false);
        let (body_owner, owner_retired) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let _owned = body_owner;
            settle_pump_body(sender, body_finished, &mut stop, true).await;
        });

        assert!(body.recv().await.is_none());
        drop(body_completed);
        assert!(owner_retired.await.is_err());
        task.await.unwrap();
    }

    #[tokio::test]
    async fn an_ordinary_non_eof_retirement_can_leave_without_the_body_signal() {
        let (sender, _body) = tokio::sync::mpsc::channel(1);
        let (_body_completed, body_finished) = tokio::sync::oneshot::channel();
        let (_session_stop, mut stop) = tokio::sync::watch::channel(false);
        let other_terminal = settle_pump_body(sender, body_finished, &mut stop, false);
        tokio::pin!(other_terminal);
        assert_eq!(futures_util::poll!(&mut other_terminal), Poll::Ready(()));
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
// The original forwards headers it cannot normalize to the provider.
fn range_bounds(value: &str, total: u64) -> Option<(u64, u64)> {
    let (start, end) = value.strip_prefix("bytes=")?.split_once('-')?;
    let start = if start.is_empty() {
        0
    } else {
        start.parse::<u64>().ok()?
    };
    let last = total.saturating_sub(1);
    let end = if end.is_empty() {
        last
    } else {
        end.parse::<u64>().ok()?.min(last)
    };
    Some((start, end))
}

fn outside_range(start: u64, end: u64, total: u64) -> bool {
    start >= total || start > end
}

// Original tests the parsed start against the known file length before
// touching the end. On the fast u64 path this is already resolved by
// range_bounds. Only the failed tuple needs the Python spelling/overflow path.
fn source_start_is_outside(value: &str, total: u64) -> bool {
    let Some((start, _)) = value
        .strip_prefix("bytes=")
        .and_then(|value| value.split_once('-'))
    else {
        return false;
    };
    let Some(start) = dispatcharr_ingest_core::source_integer_literal(start) else {
        return false;
    };
    // A minus cannot occur before the split delimiter. Every remaining
    // overflow here is a positive Python integer greater than total.
    start.parse::<u64>().map_or(true, |start| start >= total)
}

pub(crate) fn range_is_unsatisfiable(value: &str, total: u64) -> bool {
    total > 0
        && range_bounds(value, total).map_or_else(
            || source_start_is_outside(value, total),
            |(start, end)| outside_range(start, end, total),
        )
}

// Literal compatibility port of RedisBackedVODConnection._validate_range_header.
fn validate_range(value: &str, total: u64) -> Option<String> {
    match range_bounds(value, total) {
        Some((start, end)) if outside_range(start, end, total) => None,
        Some((start, end)) => Some(format!("bytes={start}-{end}")),
        None if source_start_is_outside(value, total) => None,
        None => Some(value.into()),
    }
}
// Original uses the provider reply header when present, then the originally
// resolved stream path. A redirect's destination or URL query does not change
// which explicit provider extension determines the fallback media type.
fn vod_content_type(source: &HeaderMap, provider_url: &str) -> HeaderValue {
    if let Some(value) = source
        .get(header::CONTENT_TYPE)
        .filter(|value| !value.is_empty())
    {
        return value.clone();
    }
    let mime = provider_extension_mime(provider_url).unwrap_or("video/mp4");
    HeaderValue::from_static(mime)
}

fn provider_extension_mime(provider_url: &str) -> Option<&'static str> {
    let url = url::Url::parse(provider_url).ok()?;
    let leaf = url.path().rsplit('/').next()?;
    // urllib.parse.urlparse separates the last segment's semicolon params.
    let file = leaf.split(';').next()?;
    let (stem, extension) = file.rsplit_once('.')?;
    if stem.bytes().all(|byte| byte == b'.') {
        return None;
    }
    let mime = match extension.to_ascii_lowercase().as_str() {
        "mp4" => "video/mp4",
        "mkv" => "video/x-matroska",
        "avi" => "video/x-msvideo",
        "mov" => "video/quicktime",
        "wmv" => "video/x-ms-wmv",
        "flv" => "video/x-flv",
        "webm" => "video/webm",
        "m4v" => "video/x-m4v",
        "3gp" => "video/3gpp",
        "ts" => "video/mp2t",
        "m3u8" => "application/x-mpegURL",
        "mpg" | "mpeg" => "video/mpeg",
        _ => return None,
    };
    Some(mime)
}

fn successful_headers(
    content_type: HeaderValue,
    range: Option<&str>,
    total: u64,
) -> Result<HeaderMap, StoreError> {
    let mut headers = compatibility_headers(content_type, range, total);
    headers.insert(
        "x-worker-id",
        crate::worker_identity::current().map_err(|_| {
            StoreError::StatusBody(
                500,
                json!("Streaming error: invalid worker response header"),
            )
        })?,
    );
    Ok(headers)
}
fn compatibility_headers(content_type: HeaderValue, range: Option<&str>, total: u64) -> HeaderMap {
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
    headers.insert(header::CONTENT_TYPE, content_type);
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
pub(crate) const RANGE_NOT_SATISFIABLE: &str = "Requested Range Not Satisfiable";

fn range_not_satisfiable() -> Response {
    (
        StatusCode::RANGE_NOT_SATISFIABLE,
        [
            ("content-type", "text/html; charset=utf-8"),
            ("allow", "GET, OPTIONS"),
        ],
        RANGE_NOT_SATISFIABLE,
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
// Source stream_vod returns Django HttpResponseRedirect here. Its empty
// HTML response and GET-view Allow metadata are observable even though the
// client follows Location. Keep streamed/XC-shared response headers separate.
fn provider_redirect(location: &str) -> Response {
    Response::builder()
        .status(302)
        .header("location", iri_location(location))
        .header("content-type", "text/html; charset=utf-8")
        .header("allow", "OPTIONS, GET")
        .body(Body::empty())
        .unwrap()
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
    session_redirect_response(request, &location)
}

fn xc_session_redirect(request: &VodRequest, session: &str, path: &str) -> Response {
    // Django QueryDict preserves the first key order and collects all values
    // for each key before appending the new session. The XC path remains the
    // credential-protected entry for every subsequent range request.
    let mut keys = HashMap::new();
    let mut values: Vec<(&str, Vec<&str>)> = Vec::new();
    for (key, value) in &request.playback.query {
        if matches!(key.as_str(), "token" | "session_id") {
            continue;
        }
        let index = *keys.entry(key.as_str()).or_insert(values.len());
        if index == values.len() {
            values.push((key, Vec::new()));
        }
        values[index].1.push(value);
    }
    let query = url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(
            values
                .into_iter()
                .flat_map(|(key, values)| values.into_iter().map(move |value| (key, value))),
        )
        .append_pair("session_id", session)
        .finish()
        .replace('*', "%2A")
        .replace("%7E", "~");
    session_redirect_response(request, &format!("{path}?{query}"))
}

fn session_redirect_response(request: &VodRequest, location: &str) -> Response {
    Response::builder()
        .status(301)
        .header("location", iri_location(location))
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
        StoreError::StatusBody(416, serde_json::Value::String(message))
            if message == RANGE_NOT_SATISFIABLE =>
        {
            return range_not_satisfiable();
        }
        StoreError::Forbidden => (
            StatusCode::FORBIDDEN,
            [(header::CONTENT_TYPE, "application/json")],
            r#"{"error": "Forbidden"}"#,
        )
            .into_response(),
        StoreError::StatusBody(code, serde_json::Value::String(value))
            if (code == 503 && value == "No available stream")
                || code == 500
                || (code == 429 && value == "Connection limit exceeded for profile") =>
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
        headers: dispatcharr_contracts::source_headers(
            request
                .headers()
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_bytes())),
        ),
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
    let mut response = engine
        .respond(VodRequest {
            resolved_client_ip: None,
            playback,
            kind: path.get("kind").cloned().unwrap_or_default(),
            content_id,
            session_id: path.get("session_id").cloned(),
            profile_id: path.get("profile_id").and_then(|value| value.parse().ok()),
        })
        .await;
    // Original @api_view(GET) finalization varies even streaming/HTML error
    // responses by Accept. Keep this at the proxy route, not shared XC respond.
    response.headers_mut().insert(
        axum::http::header::VARY,
        axum::http::HeaderValue::from_static("Accept, origin"),
    );
    response
}
fn head_not_allowed() -> Response {
    (
        StatusCode::METHOD_NOT_ALLOWED,
        [("allow", "GET, OPTIONS")],
        axum::Json(json!({"detail":"Method \"HEAD\" not allowed."})),
    )
        .into_response()
}
async fn head_method_response(resolver: &dyn VodResolver, request: &VodRequest) -> Response {
    let mut response = match resolver.authenticate_method(request).await {
        Ok(()) => head_not_allowed(),
        Err(error) => error_response(error),
    };
    response.headers_mut().insert(
        "allow",
        axum::http::HeaderValue::from_static("OPTIONS, GET"),
    );
    response.headers_mut().insert(
        axum::http::header::VARY,
        axum::http::HeaderValue::from_static("Accept, origin"),
    );
    response
}
fn source_head_route_matches(content_id: &str, profile: Option<&str>) -> bool {
    // Actual pinned Django UUIDConverter regex and IntConverter/Python limit.
    // Rust UUID and u64 parsers accept/reject different lexical domains.
    content_id.len() == 36
        && content_id.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
            }
        })
        && profile.is_none_or(|value| {
            !value.is_empty() && value.len() <= 4300 && value.bytes().all(|b| b.is_ascii_digit())
        })
}
async fn head(
    State(engine): State<VodEngine>,
    Path(path): Path<HashMap<String, String>>,
    request: Request<Body>,
) -> Response {
    let content_id = path.get("content_id").cloned().unwrap_or_default();
    // Non-UUID and non-integer route fallthrough is a separate SPA boundary.
    // Preserve its prior response rather than authenticate a Django non-match.
    if !source_head_route_matches(&content_id, path.get("profile_id").map(String::as_str)) {
        return head_not_allowed();
    }
    let playback = match playback(&request, content_id.clone()) {
        Ok(playback) => playback,
        Err(error) => return error_response(error),
    };
    head_method_response(
        engine.inner.resolver.as_ref(),
        &VodRequest {
            resolved_client_ip: None,
            playback,
            kind: path.get("kind").cloned().unwrap_or_default(),
            content_id,
            session_id: path.get("session_id").cloned(),
            profile_id: path.get("profile_id").and_then(|value| value.parse().ok()),
        },
    )
    .await
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
                .into_response();
        }
    };
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                axum::Json(json!({"error":"Invalid JSON"})),
            )
                .into_response();
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
mod method_auth_tests {
    use super::*;
    use dispatcharr_contracts::BoxFuture;
    use dispatcharr_store::User;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn vod_forbidden_keeps_source_wire_body() {
        let response = error_response(StoreError::Forbidden);
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
        let body = axum::body::to_bytes(response.into_body(), 32)
            .await
            .unwrap();
        assert_eq!(&body[..], br#"{"error": "Forbidden"}"#);
    }

    #[test]
    fn xc_redirect_keeps_path_and_source_query_order() {
        let request = VodRequest {
            resolved_client_ip: None,
            playback: PlaybackRequest {
                channel_id: String::new(),
                query: [
                    ("extra", "~*"),
                    ("profile_id", "4"),
                    ("extra", "again"),
                    ("token", ""),
                    ("session_id", "old"),
                ]
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
                headers: Vec::new(),
                peer_ip: "127.0.0.1".parse().unwrap(),
            },
            kind: "episode".into(),
            content_id: String::new(),
            session_id: None,
            profile_id: Some(4),
        };
        let response = xc_session_redirect(&request, "owned-session", "/series/u/p/42.mp4");
        assert_eq!(response.status(), StatusCode::MOVED_PERMANENTLY);
        assert_eq!(
            response.headers()["location"],
            "/series/u/p/42.mp4?extra=~%2A&extra=again&profile_id=4&session_id=owned-session"
        );
        assert_eq!(response.headers()["cache-control"], "no-store");
    }

    struct AuthOnly {
        calls: AtomicUsize,
        deny: bool,
    }
    impl VodResolver for AuthOnly {
        fn authenticate_method<'a>(
            &'a self,
            request: &'a VodRequest,
        ) -> BoxFuture<'a, Result<(), StoreError>> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                assert_eq!(request.playback.parameter("token"), Some("preserved-query"));
                if self.deny {
                    Err(StoreError::Unauthorized("owned method authentication"))
                } else {
                    Ok(())
                }
            })
        }
        fn authorize<'a>(
            &'a self,
            _: &'a VodRequest,
        ) -> BoxFuture<'a, Result<Option<User>, StoreError>> {
            panic!("HEAD must not enter view permission/network logic")
        }
        fn resolve<'a>(
            &'a self,
            _: &'a VodRequest,
            _: Option<User>,
        ) -> BoxFuture<'a, Result<VodSource, StoreError>> {
            panic!("HEAD must not resolve a provider or media source")
        }
        fn default_redirect(&self) -> BoxFuture<'_, Result<bool, StoreError>> {
            panic!("HEAD must not create or redirect sessions")
        }
    }
    fn request() -> VodRequest {
        VodRequest {
            resolved_client_ip: None,
            playback: PlaybackRequest {
                channel_id: "12345678-1234-5678-1234-567812345678".into(),
                headers: vec![("authorization".into(), "Bearer not-a-valid-jwt".into())],
                query: vec![("token".into(), "preserved-query".into())],
                peer_ip: "127.0.0.1".parse().unwrap(),
            },
            kind: "movie".into(),
            content_id: "12345678-1234-5678-1234-567812345678".into(),
            session_id: Some("owned-method-head".into()),
            profile_id: None,
        }
    }
    #[test]
    fn django_route_fence_rejects_rust_uuid_extensions_and_plus_profiles() {
        let canonical = "abcdefab-1234-5678-abcd-123456789abc";
        assert!(source_head_route_matches(canonical, None));
        for extended in [
            canonical.replace('-', ""),
            canonical.to_uppercase(),
            format!("urn:uuid:{canonical}"),
        ] {
            assert!(uuid::Uuid::parse_str(&extended).is_ok());
            assert!(!source_head_route_matches(&extended, None));
        }
        assert!(!source_head_route_matches(canonical, Some("+1")));
        assert!(!source_head_route_matches(canonical, Some("")));
        assert!(!source_head_route_matches(canonical, Some("١")));
        assert!(source_head_route_matches(canonical, Some("01")));
    }
    #[test]
    fn django_profile_match_does_not_require_machine_integer_representability() {
        let canonical = "abcdefab-1234-5678-abcd-123456789abc";
        let above_u64 = "18446744073709551616";
        assert!(above_u64.parse::<u64>().is_err());
        assert!(source_head_route_matches(canonical, Some(above_u64)));
        assert!(source_head_route_matches(
            canonical,
            Some(&"0".repeat(4300))
        ));
        assert!(!source_head_route_matches(
            canonical,
            Some(&"0".repeat(4301))
        ));
    }
    #[tokio::test]
    async fn authentication_failure_precedes_method_error_without_entering_view() {
        let resolver = AuthOnly {
            calls: AtomicUsize::new(0),
            deny: true,
        };
        let response = head_method_response(&resolver, &request()).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response.headers()["www-authenticate"],
            "Bearer realm=\"api\""
        );
        assert_eq!(response.headers()["allow"], "OPTIONS, GET");
        assert_eq!(response.headers()["vary"], "Accept, origin");
        assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn successful_method_authentication_still_rejects_head_without_view_work() {
        let resolver = AuthOnly {
            calls: AtomicUsize::new(0),
            deny: false,
        };
        let response = head_method_response(&resolver, &request()).await;
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert!(!response.headers().contains_key("www-authenticate"));
        assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            json!({"detail":"Method \"HEAD\" not allowed."})
        );
    }
    #[tokio::test]
    async fn actual_sql_resolver_invalid_bearer_matches_source_without_sql() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused:unused@127.0.0.1:9/not_contacted")
            .unwrap();
        let store = Store::from_pool(pool);
        let auth = AuthService::new(
            store.clone(),
            dispatcharr_store::AuthConfig::new("owned-head-method-signing-key".into()).unwrap(),
        );
        let resolver = SqlVodResolver::new(store.clone(), auth);
        let response = head_method_response(&resolver, &request()).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response.headers()["www-authenticate"],
            "Bearer realm=\"api\""
        );
        assert_eq!(response.headers()["allow"], "OPTIONS, GET");
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        assert_eq!(body.len(), 172);
        assert_eq!(store.pool().size(), 0);
    }
}

#[cfg(test)]
mod header_tests {
    #[tokio::test]
    async fn worker_id_is_only_added_by_success_builder_and_cannot_be_spoofed() {
        use axum::http::{HeaderMap, HeaderValue};
        let mut provider = HeaderMap::new();
        provider.insert("content-type", HeaderValue::from_static("video/mp4"));
        provider.insert("x-worker-id", HeaderValue::from_static("provider-spoof"));
        let content_type = super::vod_content_type(&provider, "http://fixture.invalid/archive.mkv");
        let full = super::successful_headers(content_type.clone(), None, 64).unwrap();
        let range = super::successful_headers(content_type, Some("bytes=0-15"), 64).unwrap();
        assert_eq!(
            full["x-worker-id"],
            crate::worker_identity::current().unwrap()
        );
        assert_ne!(full["x-worker-id"], "provider-spoof");
        assert_eq!(full["x-worker-id"], range["x-worker-id"]);
        assert_eq!(full["connection"], "keep-alive");
        assert_eq!(range["content-range"], "bytes 0-15/64");
        assert_eq!(range["content-length"], "16");
        for error in [
            dispatcharr_store::StoreError::NotFound,
            dispatcharr_store::StoreError::StatusBody(
                403,
                serde_json::json!({"error":"Forbidden"}),
            ),
            dispatcharr_store::StoreError::StatusBody(
                429,
                serde_json::json!("Connection limit exceeded for profile"),
            ),
            dispatcharr_store::StoreError::StatusBody(
                500,
                serde_json::json!("Streaming error: VOD provider request failed"),
            ),
        ] {
            assert!(!super::error_response(error)
                .headers()
                .contains_key("x-worker-id"));
        }
        assert_eq!(super::head_not_allowed().status(), 405);
        assert!(!super::head_not_allowed()
            .headers()
            .contains_key("x-worker-id"));
        assert!(!super::range_not_satisfiable()
            .headers()
            .contains_key("x-worker-id"));
    }
    #[test]
    fn missing_provider_mime_uses_original_resolved_extension_for_full_and_range() {
        use axum::http::{header::CONTENT_TYPE, HeaderMap, HeaderValue};

        let mut provider = HeaderMap::new();
        for (url, expected) in [
            (
                "http://fixture.invalid/item.MKV?query=.mp4",
                "video/x-matroska",
            ),
            ("http://fixture.invalid/item.WEBM;type=unique", "video/webm"),
            ("http://fixture.invalid/item.ts#source.webm", "video/mp2t"),
            ("http://fixture.invalid/item.m3u8", "application/x-mpegURL"),
            ("http://fixture.invalid/item.mpeg", "video/mpeg"),
            ("http://fixture.invalid/item.unknown?q=.mkv", "video/mp4"),
            ("http://fixture.invalid/.mkv", "video/mp4"),
            ("http://fixture.invalid/...", "video/mp4"),
        ] {
            let resolved = super::vod_content_type(&provider, url);
            assert_eq!(resolved, expected);
            assert_eq!(
                super::successful_headers(resolved.clone(), None, 32).unwrap()[CONTENT_TYPE],
                expected
            );
            let ranged = super::successful_headers(resolved, Some("bytes=8-15"), 32).unwrap();
            assert_eq!(ranged[CONTENT_TYPE], expected);
            assert_eq!(ranged["content-range"], "bytes 8-15/32");
        }

        let provider_url = "http://fixture.invalid/item.mkv";
        provider.insert(CONTENT_TYPE, HeaderValue::from_static(""));
        assert_eq!(
            super::vod_content_type(&provider, provider_url),
            "video/x-matroska"
        );
        provider.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
        assert_eq!(
            super::vod_content_type(&provider, provider_url),
            "application/octet-stream"
        );
    }

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
                resolved_client_ip: None,
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

#[cfg(test)]
mod provider_redirect_header_tests {
    use super::*;
    #[tokio::test]
    async fn transformed_provider_redirect_retains_django_html_get_metadata() {
        let response = provider_redirect("https://owned.invalid/media");
        assert_eq!(response.status(), StatusCode::FOUND);
        assert_eq!(
            response.headers()["location"],
            "https://owned.invalid/media"
        );
        assert_eq!(
            response.headers()["content-type"],
            "text/html; charset=utf-8"
        );
        assert_eq!(response.headers()["allow"], "OPTIONS, GET");
        assert!(!response.headers().contains_key("cache-control"));
        assert!(axum::body::to_bytes(response.into_body(), 1)
            .await
            .unwrap()
            .is_empty());
    }
}

#[cfg(test)]
mod existing_range_admission {
    use super::*;

    #[test]
    fn side_effect_free_gate_matches_ordinary_provider_range_validation() {
        for (range, rejected, normalized) in [
            ("bytes=100-", true, None),
            ("bytes=4-3", true, None),
            ("bytes=98-200", false, Some("bytes=98-99")),
            ("bytes=-3", false, Some("bytes=0-3")),
            ("bytes=1-", false, Some("bytes=1-99")),
            ("bytes=1-3,4-5", false, Some("bytes=1-3,4-5")),
            ("bytes=broken", false, Some("bytes=broken")),
        ] {
            assert_eq!(range_is_unsatisfiable(range, 100), rejected);
            assert_eq!(validate_range(range, 100).as_deref(), normalized);
            assert!(!range_is_unsatisfiable(range, 0));
        }
    }

    #[test]
    fn source_rejects_an_outside_start_even_with_an_unparsed_end() {
        for range in [
            "bytes=100-invalid",
            "bytes=18446744073709551616-",
            "bytes=18446744073709551616-invalid",
            "bytes=+1_00-invalid",
            "bytes= 100-invalid",
        ] {
            assert!(range_is_unsatisfiable(range, 100), "{range}");
            assert_eq!(validate_range(range, 100), None, "{range}");
            assert!(
                !range_is_unsatisfiable(range, 0),
                "length is unknown without a previous reply"
            );
        }
        for range in [
            "bytes=99-invalid",
            "bytes=invalid-",
            "bytes=+1_0-invalid",
            "Bytes=18446744073709551616-",
            "bytes=18446744073709551616",
        ] {
            assert!(!range_is_unsatisfiable(range, 100), "{range}");
            assert_eq!(
                validate_range(range, 100).as_deref(),
                Some(range),
                "{range}"
            );
        }
        let too_long = format!("bytes={}-", "9".repeat(4301));
        assert!(!range_is_unsatisfiable(&too_long, 100));
        assert_eq!(validate_range(&too_long, 100), Some(too_long));
    }

    #[tokio::test]
    async fn early_admission_416_preserves_literal_existing_response() {
        let response = error_response(StoreError::StatusBody(416, json!(RANGE_NOT_SATISFIABLE)));
        let original = range_not_satisfiable();
        assert_eq!(response.status(), original.status());
        assert_eq!(response.headers(), original.headers());
        let body = axum::body::to_bytes(response.into_body(), 128)
            .await
            .unwrap();
        assert_eq!(body, RANGE_NOT_SATISFIABLE);
    }
}

#[cfg(test)]
mod authenticated_restart_tests {
    use super::{signed_restart_subject, RequestAuthMethod, User};

    #[test]
    fn authenticated_restart_requires_explicit_signed_header_provenance() {
        let user = User {
            id: 42,
            username: "fixture-viewer".into(),
            password: "!".into(),
            is_active: true,
            user_level: 0,
            stream_limit: 1,
            custom_properties: serde_json::json!({}),
        };
        let signed = Some(RequestAuthMethod::SignedBearerHeader);
        assert_eq!(signed_restart_subject(Some(&user), signed), Some(42));
        assert_eq!(signed_restart_subject(None, signed), None);
        assert_eq!(
            signed_restart_subject(Some(&user), Some(RequestAuthMethod::ApiKey)),
            None
        );
        // Query, restored optional identity, XC, and fixture resolvers may
        // carry a resolved user without provenance from the original header.
        assert_eq!(signed_restart_subject(Some(&user), None), None);
    }
}
