//! Independent original Dispatcharr guide adapter; no application route enabled.
//! Original authors retain full application credit. Physical gates are separate.
use crate::epg_grid::stored_standard_entity;
use axum::{
    body::Body,
    extract::{ConnectInfo, Request, State},
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use bytes::Bytes;
use dispatcharr_store::{AuthService, Store, StoreError};
use serde_json::json;
use std::{convert::Infallible, net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};
const WORK_BYTES: usize = 96 * 1024 * 1024;
const MAX_INPUT_BYTES: usize = 64 * 1024;
// Bound retained inputs independently of the unchanged active workspaces.
// Ten callers cover the measured startup burst without admitting unlimited work.
const MAX_OWNED_READS: usize = 10;
#[derive(Clone, Copy)]
pub struct GridReadLimits {
    pub concurrent_responses: usize,
    pub memory_bytes: usize,
    pub deadline: Duration,
}
impl Default for GridReadLimits {
    fn default() -> Self {
        Self {
            concurrent_responses: 2,
            memory_bytes: 192 * 1024 * 1024,
            deadline: Duration::from_secs(5),
        }
    }
}
struct Memory {
    _permit: OwnedSemaphorePermit,
}
struct Frame {
    data: Vec<u8>,
    _memory: Arc<Memory>,
}
impl AsRef<[u8]> for Frame {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}
struct Inner {
    store: Store,
    auth: AuthService,
    limits: GridReadLimits,
    requests: Arc<Semaphore>,
    owned: Arc<Semaphore>,
    memory: Arc<Semaphore>,
    clock: Arc<dyn Fn() -> chrono::DateTime<chrono::Utc> + Send + Sync>,
}
#[derive(Clone)]
pub struct GridGateway {
    inner: Arc<Inner>,
}
fn unavailable() -> StoreError {
    StoreError::StatusBody(503, json!({"detail":"Guide read capacity unavailable."}))
}
struct Prepared {
    frames: Vec<Vec<u8>>,
    status: StatusCode,
    headers: HeaderMap,
}
struct Finished {
    frames: Vec<Bytes>,
    status: StatusCode,
    headers: HeaderMap,
}
enum Mode {
    Frames,
    Http(Method),
}
impl GridGateway {
    pub fn new(
        store: Store,
        auth: AuthService,
        limits: GridReadLimits,
    ) -> Result<Self, StoreError> {
        if !(1..=8).contains(&limits.concurrent_responses)
            || !(WORK_BYTES..=512 * 1024 * 1024).contains(&limits.memory_bytes)
            || !(Duration::from_millis(100)..=Duration::from_secs(30)).contains(&limits.deadline)
        {
            return Err(unavailable());
        }
        Ok(Self {
            inner: Arc::new(Inner {
                store,
                auth,
                limits,
                requests: Arc::new(Semaphore::new(limits.concurrent_responses)),
                owned: Arc::new(Semaphore::new(MAX_OWNED_READS)),
                memory: Arc::new(Semaphore::new(limits.memory_bytes)),
                clock: Arc::new(chrono::Utc::now),
            }),
        })
    }
    pub fn with_clock(
        mut self,
        clock: impl Fn() -> chrono::DateTime<chrono::Utc> + Send + Sync + 'static,
    ) -> Result<Self, StoreError> {
        Arc::get_mut(&mut self.inner).ok_or_else(unavailable)?.clock = Arc::new(clock);
        Ok(self)
    }
    pub fn router(self) -> Router {
        Router::new()
            .route("/api/epg/grid/", any(handle))
            .with_state(self)
    }
    /// Counts admitted waiting/authentication/SQL/rollback work, not detached frames.
    pub fn physical_operations(&self) -> usize {
        MAX_OWNED_READS - self.inner.owned.available_permits()
    }
    pub fn available_memory_bytes(&self) -> usize {
        self.inner.memory.available_permits()
    }
    pub async fn shutdown(&self, grace: Duration) -> bool {
        // Accepted waiters retain their bounded inputs and may finish during
        // the grace period. Closing work permits here would discard them.
        self.inner.owned.close();
        let until = tokio::time::Instant::now() + grace;
        while self.physical_operations() != 0 && tokio::time::Instant::now() < until {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let drained = self.physical_operations() == 0;
        if drained {
            self.inner.requests.close();
            self.inner.memory.close();
        }
        drained
    }
    pub async fn read(
        &self,
        peer: SocketAddr,
        headers: HeaderMap,
        params: Vec<(String, String)>,
    ) -> Result<Vec<Bytes>, StoreError> {
        Ok(self
            .owned(peer, headers, params, Mode::Frames, self.admit()?)
            .await?
            .frames)
    }
    fn admit(&self) -> Result<OwnedSemaphorePermit, StoreError> {
        self.inner
            .owned
            .clone()
            .try_acquire_owned()
            .map_err(|_| unavailable())
    }
    async fn owned(
        &self,
        peer: SocketAddr,
        headers: HeaderMap,
        params: Vec<(String, String)>,
        mode: Mode,
        owner: OwnedSemaphorePermit,
    ) -> Result<Finished, StoreError> {
        let input = params
            .iter()
            .try_fold(0usize, |n, (k, v)| n.checked_add(k.len() + v.len()));
        let header_bytes = headers.iter().try_fold(0usize, |n, (k, v)| {
            n.checked_add(k.as_str().len() + v.as_bytes().len())
        });
        if params.len() > 4096
            || headers.len() > 256
            || input.is_none_or(|n| n > MAX_INPUT_BYTES)
            || header_bytes.is_none_or(|n| n > MAX_INPUT_BYTES)
        {
            return Err(unavailable());
        }
        // A bounded input owner precedes auth/cache/SQL. Waiting does not
        // reserve an active96MiB workspace or start physical database work.
        let expires = tokio::time::Instant::now() + self.inner.limits.deadline;
        let state = self.clone();
        let (mut tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let request_permit = tokio::select! {
                _ = tx.closed() => return,
                permit = tokio::time::timeout_at(expires, state.inner.requests.clone().acquire_owned()) => permit,
            };
            let request_permit = match request_permit {
                Ok(Ok(permit)) => permit,
                _ => {
                    drop(owner);
                    let _ = tx.send(Err(unavailable()));
                    return;
                }
            };
            let workspace = tokio::select! {
                _ = tx.closed() => return,
                permit = tokio::time::timeout_at(expires, state.inner.memory.clone().acquire_many_owned(WORK_BYTES as u32)) => permit,
            };
            let workspace = match workspace {
                Ok(Ok(permit)) => permit,
                _ => {
                    drop(request_permit);
                    drop(owner);
                    let _ = tx.send(Err(unavailable()));
                    return;
                }
            };
            if tx.is_closed() || tokio::time::Instant::now() >= expires {
                return;
            }
            let is_http = matches!(mode, Mode::Http(_));
            let html = html_renderer(&headers, &params);
            let indent = json_indent(&headers);
            let result = state.prepare(peer, headers, params, &mode).await;
            let prepared = match result {
                Ok(value) => Ok(value),
                Err(error) if is_http => {
                    let error = if html
                        && !matches!(error, StoreError::NotFound | StoreError::StatusBody(406, _))
                    {
                        StoreError::Unsupported("Guide browsable error HTML is not yet implemented")
                    } else {
                        error
                    };
                    let response = match error {
                        StoreError::StatusBody(500, _) | StoreError::Database(_) => outer_error(),
                        error => error.into_response(),
                    };
                    let (mut parts, body) = response.into_parts();
                    match axum::body::to_bytes(body, 256 * 1024).await {
                        Ok(data) => {
                            let data = if parts
                                .headers
                                .get("content-type")
                                .is_some_and(|v| v == "application/json")
                            {
                                match serde_json::from_slice::<serde_json::Value>(&data) {
                                    Ok(value) => render_json(&value, indent)
                                        .unwrap_or_else(|_| data.to_vec()),
                                    Err(_) => data.to_vec(),
                                }
                            } else {
                                data.to_vec()
                            };
                            // Original CommonMiddleware assigns a complete error
                            // representation length; successful guide streams do not.
                            parts
                                .headers
                                .insert("content-length", data.len().to_string().parse().unwrap());
                            Ok(Prepared {
                                frames: vec![data.to_vec()],
                                status: parts.status,
                                headers: parts.headers,
                            })
                        }
                        Err(_) => Err(unavailable()),
                    }
                }
                Err(error) => Err(error),
            };
            let result = prepared.and_then(|prepared| finish(prepared, workspace));
            drop(request_permit);
            drop(owner);
            let _ = tx.send(result);
        });
        tokio::time::timeout_at(expires, rx)
            .await
            .map_err(|_| unavailable())?
            .map_err(|_| unavailable())?
    }
    async fn prepare(
        &self,
        peer: SocketAddr,
        headers: HeaderMap,
        params: Vec<(String, String)>,
        mode: &Mode,
    ) -> Result<Prepared, StoreError> {
        let method = match mode {
            Mode::Frames => Method::GET,
            Mode::Http(method) => method.clone(),
        };
        if matches!(mode, Mode::Http(_)) {
            negotiate(&headers, &params)?;
        }
        let selected_html = html_renderer(&headers, &params);
        let selected_indent = json_indent(&headers);
        let user = self
            .inner
            .auth
            .account_user(peer, headers, !matches!(method, Method::GET | Method::HEAD))
            .await?;
        // Original permissions_for_method maps GET/HEAD to Standard and
        // unsafe/default methods (including OPTIONS) to Admin.
        let minimum_level = if matches!(method, Method::GET | Method::HEAD) {
            1
        } else {
            10
        };
        if user.user_level < minimum_level {
            return Err(StoreError::PermissionDenied);
        }
        if method == Method::OPTIONS {
            if selected_html {
                return Err(StoreError::Unsupported(
                    "Guide browsable metadata HTML is not yet implemented",
                ));
            }
            let data = include_bytes!("../datasets/source-grid-options.json").to_vec();
            let value: serde_json::Value =
                serde_json::from_slice(&data).map_err(|_| unavailable())?;
            let data = render_json(&value, selected_indent)?;
            let mut headers = HeaderMap::new();
            headers.insert("content-type", "application/json".parse().unwrap());
            headers.insert("content-length", data.len().to_string().parse().unwrap());
            return Ok(Prepared {
                frames: vec![data],
                status: StatusCode::OK,
                headers,
            });
        }
        if !matches!(method, Method::GET | Method::HEAD) {
            return Err(StoreError::StatusBody(
                405,
                json!({"detail":format!("Method \"{method}\" not allowed.")}),
            ));
        }
        let policy = dispatcharr_store::source_epg_history_policy(
            &self.inner.auth.source_epg_settings().await?,
        )
        .map_err(|_| unavailable())?;
        let frames = stored_standard_entity(
            &self.inner.store,
            &user,
            &params,
            (self.inner.clock)(),
            policy.max_days,
        )
        .await?;
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().unwrap());
        Ok(Prepared {
            frames: if method == Method::HEAD {
                Vec::new()
            } else {
                frames
            },
            status: StatusCode::OK,
            headers,
        })
    }
}
fn finish(prepared: Prepared, mut workspace: OwnedSemaphorePermit) -> Result<Finished, StoreError> {
    if prepared.frames.len() > 2048 {
        return Err(unavailable());
    }
    let retained = prepared
        .frames
        .iter()
        .try_fold(64 * 1024usize, |total, data| {
            total.checked_add(data.capacity())?.checked_add(512)
        })
        .filter(|n| *n <= WORK_BYTES)
        .ok_or_else(unavailable)?;
    let excess = workspace.num_permits().saturating_sub(retained);
    drop(workspace.split(excess));
    let memory = Arc::new(Memory { _permit: workspace });
    let frames = prepared
        .frames
        .into_iter()
        .map(|data| {
            Bytes::from_owner(Frame {
                data,
                _memory: memory.clone(),
            })
        })
        .collect();
    Ok(Finished {
        frames,
        status: prepared.status,
        headers: prepared.headers,
    })
}
fn json_indent(headers: &HeaderMap) -> Option<usize> {
    let raw = headers.get("accept")?.to_str().ok()?;
    for part in raw.split(';').skip(1) {
        let (key, value) = part.trim().split_once('=')?;
        if key.eq_ignore_ascii_case("indent") {
            let value =
                dispatcharr_ingest_core::source_integer_literal(value.trim().trim_matches('"'))?;
            return Some(
                value
                    .parse::<i64>()
                    .map(|v| v.clamp(0, 8) as usize)
                    .unwrap_or(if value.starts_with('-') { 0 } else { 8 }),
            );
        }
    }
    None
}
fn render_json(value: &serde_json::Value, indent: Option<usize>) -> Result<Vec<u8>, StoreError> {
    match indent {
        None => serde_json::to_vec(value).map_err(|_| unavailable()),
        Some(n) => {
            let pretty = serde_json::to_string_pretty(value).map_err(|_| unavailable())?;
            Ok(pretty
                .lines()
                .map(|line| {
                    let spaces = line.len() - line.trim_start_matches(' ').len();
                    format!("{}{}", " ".repeat(spaces / 2 * n), &line[spaces..])
                })
                .collect::<Vec<_>>()
                .join("\n")
                .into_bytes())
        }
    }
}
fn html_renderer(headers: &HeaderMap, params: &[(String, String)]) -> bool {
    if let Some((_, format)) = params.iter().rev().find(|(k, _)| k == "format") {
        if format == "api" {
            return true;
        }
        if format == "json" {
            return false;
        }
    }
    let accept = headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("*/*")
        .to_ascii_lowercase();
    let media: Vec<&str> = accept
        .split(',')
        .map(|v| v.split(';').next().unwrap_or("").trim())
        .collect();
    !media
        .iter()
        .any(|v| matches!(*v, "application/json" | "application/*" | "*/*"))
        && media.iter().any(|v| matches!(*v, "text/html" | "text/*"))
}
fn negotiate(headers: &HeaderMap, params: &[(String, String)]) -> Result<(), StoreError> {
    if let Some((_, format)) = params.iter().rev().find(|(k, _)| k == "format") {
        if !matches!(format.as_str(), "" | "json" | "api") {
            return Err(StoreError::NotFound);
        }
    }
    let Some(accept) = headers.get("accept").and_then(|v| v.to_str().ok()) else {
        return Ok(());
    };
    let format = params
        .iter()
        .rev()
        .find(|(k, _)| k == "format")
        .map(|(_, v)| v.as_str())
        .filter(|v| !v.is_empty());
    let allowed = accept
        .split(',')
        .map(|v| {
            v.split(';')
                .next()
                .unwrap_or("")
                .trim()
                .to_ascii_lowercase()
        })
        .any(|v| {
            (format != Some("api")
                && matches!(v.as_str(), "application/json" | "application/*" | "*/*"))
                || (format != Some("json") && matches!(v.as_str(), "text/html" | "text/*" | "*/*"))
        });
    if allowed {
        Ok(())
    } else {
        Err(StoreError::StatusBody(
            406,
            json!({"detail":"Could not satisfy the request Accept header."}),
        ))
    }
}
fn outer_error() -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR,[("content-type","text/html; charset=utf-8")],"\n<!doctype html>\n<html lang=\"en\">\n<head>\n  <title>Server Error (500)</title>\n</head>\n<body>\n  <h1>Server Error (500)</h1><p></p>\n</body>\n</html>\n").into_response()
}
async fn handle(
    State(state): State<GridGateway>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Response {
    // Check bytes before percent-decoding/copying query/header values.
    if request.uri().path().len() > 8192
        || request.headers().len() > 256
        || request
            .uri()
            .query()
            .is_some_and(|q| q.len() > MAX_INPUT_BYTES)
        || request
            .headers()
            .iter()
            .try_fold(0usize, |n, (k, v)| {
                n.checked_add(k.as_str().len() + v.as_bytes().len())
            })
            .is_none_or(|n| n > MAX_INPUT_BYTES)
    {
        return unavailable().into_response();
    }
    let owner = match state.admit() {
        Ok(owner) => owner,
        Err(error) => return error.into_response(),
    };
    let params = url::form_urlencoded::parse(request.uri().query().unwrap_or("").as_bytes())
        .into_owned()
        .take(4097)
        .collect();
    let headers = request.headers().clone();
    let mode = Mode::Http(request.method().clone());
    drop(request);
    match state.owned(peer, headers, params, mode, owner).await {
        Ok(mut output) => {
            output
                .headers
                .insert("allow", "GET, HEAD, OPTIONS".parse().unwrap());
            output.headers.insert("vary", "Accept".parse().unwrap());
            let mut response = Response::new(Body::from_stream(futures_util::stream::iter(
                output.frames.into_iter().map(Ok::<_, Infallible>),
            )));
            *response.status_mut() = output.status;
            *response.headers_mut() = output.headers;
            response
        }
        Err(error) => error.into_response(),
    }
}
