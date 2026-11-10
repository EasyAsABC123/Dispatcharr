//! Unregistered/unwired XC gateway draft. Original Dispatcharr author credit.
//! All paths require original/native HTTP and resource gates before acceptance.
use crate::{
    counts::ConnectionCounts,
    numbers,
    project::{self, JsonBytes, LiveRows},
    reader::{self, ReadLimit},
};
use axum::{
    body::Body,
    extract::{ConnectInfo, State},
    http::{Method, Request, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use bytes::Bytes;
use dispatcharr_contracts::PlaybackRequest;
use dispatcharr_store::{AuthService, Store, StoreError};
use serde_json::json;
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

#[derive(Clone)]
pub struct XcConfig {
    pub max_requests: usize,
    pub max_rows: usize,
    pub max_body_bytes: usize,
    pub max_metadata_bytes: usize,
    pub max_response_bytes: usize,
    pub max_number_bytes: usize,
    pub max_memory_bytes: usize,
    pub request_deadline: Duration,
}
impl Default for XcConfig {
    fn default() -> Self {
        Self {
            max_requests: 32,
            max_rows: 50000,
            max_body_bytes: 64 * 1024,
            max_metadata_bytes: 8 * 1024 * 1024,
            max_response_bytes: 16 * 1024 * 1024,
            max_number_bytes: 128 * 1024 * 1024,
            max_memory_bytes: 192 * 1024 * 1024,
            request_deadline: Duration::from_secs(15),
        }
    }
}
struct Inner {
    store: Store,
    auth: AuthService,
    counts: ConnectionCounts,
    config: XcConfig,
    requests: Arc<Semaphore>,
    memory: Arc<Semaphore>,
    stopped: AtomicBool,
    epg_enabled: AtomicBool,
    workers: AtomicUsize,
    done: Notify,
    live: std::sync::OnceLock<LiveDispatcher>,
}
#[derive(Clone)]
pub struct XcGateway {
    inner: Arc<Inner>,
}
struct Worker(Arc<Inner>);
impl Drop for Worker {
    fn drop(&mut self) {
        self.0.workers.fetch_sub(1, Ordering::AcqRel);
        self.0.done.notify_waiters();
    }
}
struct Payload {
    data: Vec<u8>,
    _memory: OwnedSemaphorePermit,
}
impl AsRef<[u8]> for Payload {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}
impl XcGateway {
    pub fn new(
        store: Store,
        auth: AuthService,
        counts: ConnectionCounts,
        config: XcConfig,
    ) -> Result<Self, StoreError> {
        let base = config
            .max_metadata_bytes
            .checked_mul(2)
            .and_then(|n| n.checked_add(config.max_response_bytes.checked_mul(2)?))
            .and_then(|n| n.checked_add(config.max_body_bytes.checked_mul(2)?))
            .ok_or_else(reader::capacity)?;
        if config.max_requests == 0
            || config.max_requests > 128
            || config.max_rows == 0
            || config.max_rows > 50000
            || config.max_body_bytes == 0
            || config.max_body_bytes > 1024 * 1024
            || config.max_metadata_bytes == 0
            || config.max_metadata_bytes > 32 * 1024 * 1024
            || config.max_response_bytes == 0
            || config.max_response_bytes > 64 * 1024 * 1024
            || config.max_number_bytes > 128 * 1024 * 1024
            || config.max_number_bytes < numbers::reservation(0).map_err(|_| reader::capacity())?
            || config.max_memory_bytes > 512 * 1024 * 1024
            || config.max_memory_bytes
                < base
                    .checked_add(config.max_number_bytes)
                    .ok_or_else(reader::capacity)?
            || config.request_deadline.is_zero()
        {
            return Err(StoreError::Unsupported("invalid XC read resource budget"));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                store,
                auth,
                counts,
                requests: Arc::new(Semaphore::new(config.max_requests)),
                memory: Arc::new(Semaphore::new(config.max_memory_bytes)),
                config,
                stopped: AtomicBool::new(false),
                epg_enabled: AtomicBool::new(false),
                workers: AtomicUsize::new(0),
                done: Notify::new(),
                live: std::sync::OnceLock::new(),
            }),
        })
    }
    /// Canonical paths only. Source regex aliases and additional action families
    /// are separate gates; no app route is installed by constructing this value.
    pub fn router(&self) -> Router {
        Router::new()
            .route("/player_api.php", any(dispatch))
            .route("/panel_api.php", any(dispatch))
            .with_state(self.clone())
    }
    /// Explicit staged capability; the application keeps these actions guarded
    /// until the original/native HTTP and resource gates have passed.
    pub fn with_epg(self) -> Self {
        self.inner.epg_enabled.store(true, Ordering::Release);
        self
    }
    pub fn active_requests(&self) -> usize {
        self.inner.workers.load(Ordering::Acquire)
    }
    pub fn available_memory_bytes(&self) -> usize {
        self.inner.memory.available_permits()
    }
    pub async fn shutdown(&self) -> bool {
        self.inner.stopped.store(true, Ordering::Release);
        self.inner.requests.close();
        self.inner.memory.close();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let done = self.inner.done.notified();
                if self.active_requests() == 0 {
                    break;
                }
                done.await;
            }
        })
        .await
        .is_ok()
    }
    async fn handle(&self, request: Request<Body>) -> Result<Response, StoreError> {
        if self.inner.stopped.load(Ordering::Acquire) {
            return Err(reader::capacity());
        }
        let permit = Arc::new(
            self.inner
                .requests
                .clone()
                .try_acquire_owned()
                .map_err(|_| reader::capacity())?,
        );
        self.inner.workers.fetch_add(1, Ordering::AcqRel);
        let worker = Worker(self.inner.clone());
        let gateway = self.clone();
        // Client cancellation cannot release admission while auth/SQL/Redis work
        // is running. Redis operations additionally retain an Arc of admission
        // through the actual multiplexed reply, even after this worker deadline.
        tokio::spawn(async move {
            let _worker = worker;
            tokio::time::timeout(
                gateway.inner.config.request_deadline,
                gateway.process(request, permit),
            )
            .await
            .map_err(|_| reader::capacity())?
        })
        .await
        .map_err(|_| reader::capacity())?
    }
    async fn process(
        &self,
        request: Request<Body>,
        permit: Arc<OwnedSemaphorePermit>,
    ) -> Result<Response, StoreError> {
        let config = &self.inner.config;
        let reservation = config.max_metadata_bytes * 2
            + config.max_response_bytes * 2
            + config.max_body_bytes * 2;
        let mut memory = self
            .inner
            .memory
            .clone()
            .acquire_many_owned(reservation as u32)
            .await
            .map_err(|_| reader::capacity())?;
        let mut header_bytes = request.uri().to_string().len();
        for (name, value) in request.headers() {
            header_bytes = header_bytes
                .checked_add(name.as_str().len() + value.as_bytes().len() + 4)
                .ok_or_else(reader::capacity)?;
        }
        if header_bytes > 64 * 1024 {
            return Err(reader::capacity());
        }
        let method = request.method().clone();
        let panel = request.uri().path() == "/panel_api.php";
        let identity = identity(&request);
        let (parts, body) = request.into_parts();
        // Safe Django views do not wait for a request body before responding.
        let body = if matches!(
            method,
            Method::GET | Method::HEAD | Method::OPTIONS | Method::TRACE
        ) {
            drop(body);
            Bytes::new()
        } else {
            axum::body::to_bytes(body, config.max_body_bytes)
                .await
                .map_err(|_| reader::capacity())?
        };
        if let Some(page) =
            dispatcharr_output_http::source_csrf_failure(&method, &parts.headers, &body)
        {
            return Ok((
                StatusCode::FORBIDDEN,
                [("content-type", "text/html; charset=utf-8")],
                page,
            )
                .into_response());
        }
        drop(body);
        // Ordinary Django views: presented DRF credentials are not evaluated.
        let user = self
            .inner
            .auth
            .authorize_xc(
                &identity,
                identity.parameter("username").unwrap_or(""),
                identity.parameter("password").unwrap_or(""),
                "XC_API",
            )
            .await?;
        let action = identity.parameter("action").unwrap_or("");
        let mut out = JsonBytes::new(config.max_response_bytes)?;
        let limits = ReadLimit {
            max_rows: config.max_rows,
            max_metadata: config.max_metadata_bytes,
        };
        let stream = !panel && action == "get_live_streams";
        if panel {
            let info = project::info(
                &self.inner.store,
                &self.inner.counts,
                &identity,
                &user,
                permit.clone(),
            )
            .await?;
            out.raw(b"{\"user_info\": ")?;
            out.value(&info["user_info"])?;
            out.raw(b", \"server_info\": ")?;
            out.value(&info["server_info"])?;
            out.raw(b", \"categories\": {\"series\": [], \"movie\": [], \"live\": ")?;
            let categories = reader::categories(&self.inner.store, &user, &limits).await?;
            out.value(&serde_json::Value::Array(categories))?;
            out.raw(b"}, \"available_channels\": ")?;
            self.write_live(&identity, &user, &limits, &mut out, true)
                .await?;
            out.raw(b"}")?;
        } else if action == "get_live_categories" {
            let categories = reader::categories(&self.inner.store, &user, &limits).await?;
            out.value(&serde_json::Value::Array(categories))?;
        } else if stream {
            self.write_live(&identity, &user, &limits, &mut out, false)
                .await?;
        } else if matches!(action, "get_short_epg" | "get_simple_data_table")
            && self.inner.epg_enabled.load(Ordering::Acquire)
        {
            let limits = crate::epg::Limits {
                max_programmes: config.max_rows,
                max_category_channels: config.max_rows,
                max_metadata_bytes: config.max_metadata_bytes,
                max_response_bytes: config.max_response_bytes,
                max_number_bytes: config.max_number_bytes,
            };
            let result = crate::epg::write(
                &crate::epg::Context {
                    store: &self.inner.store,
                    auth: &self.inner.auth,
                    user: &user,
                    request: &identity,
                    short: action == "get_short_epg",
                    now: chrono::Utc::now(),
                    limits: &limits,
                    number_memory: self.inner.memory.clone(),
                },
                &mut out,
            )
            .await;
            match result {
                Ok(()) => {}
                Err(crate::epg::Error::NotFound) => return Ok(django_page(404)),
                Err(crate::epg::Error::InvalidLimit | crate::epg::Error::Backend) => {
                    return Ok(django_page(500))
                }
                Err(crate::epg::Error::Unsupported) => {
                    return Err(StoreError::StatusBody(
                        501,
                        json!({"detail":"Custom XC EPG source properties require a verified regex adapter."}),
                    ))
                }
                Err(crate::epg::Error::Capacity) => return Err(reader::capacity()),
            }
        } else if matches!(
            action,
            "get_short_epg"
                | "get_simple_data_table"
                | "get_vod_categories"
                | "get_vod_streams"
                | "get_series_categories"
                | "get_series"
                | "get_series_info"
                | "get_vod_info"
        ) {
            return Err(StoreError::StatusBody(
                501,
                json!({"detail":"This XC action has not yet completed its Source migration gates."}),
            ));
        } else {
            out.value(
                &project::info(
                    &self.inner.store,
                    &self.inner.counts,
                    &identity,
                    &user,
                    permit.clone(),
                )
                .await?,
            )?;
        }
        let data = out.finish();
        let size = data.len();
        let retained = memory.split(data.capacity()).ok_or_else(reader::capacity)?;
        drop(memory);
        let data = Bytes::from_owner(Payload {
            data,
            _memory: retained,
        });
        let mut response = Response::new(if method == Method::HEAD {
            Body::empty()
        } else {
            Body::from(data)
        });
        response
            .headers_mut()
            .insert("content-type", "application/json".parse().unwrap());
        if !stream {
            response.headers_mut().insert("content-length", size.into());
        }
        Ok(response)
    }
    async fn write_live(
        &self,
        request: &PlaybackRequest,
        user: &dispatcharr_store::User,
        limits: &ReadLimit,
        out: &mut JsonBytes,
        as_object: bool,
    ) -> Result<(), StoreError> {
        let rows = LiveRows::read(&self.inner.store, request, user, limits).await?;
        let budget = numbers::reservation(rows.len()).map_err(|_| reader::capacity())?;
        if budget > self.inner.config.max_number_bytes {
            return Err(reader::capacity());
        }
        // Never await a second build reservation while holding the first: a
        // burst of large requests must reject capacity, not deadlock each other.
        let _number_memory = self
            .inner
            .memory
            .clone()
            .try_acquire_many_owned(budget as u32)
            .map_err(|_| reader::capacity())?;
        let mut live = LiveRows::project(&self.inner.auth, request, user, rows, budget).await?;
        live.write(&self.inner.store, out, as_object).await
    }
}
fn django_page(status: u16) -> Response {
    let body = if status == 404 {
        "\n<!doctype html>\n<html lang=\"en\">\n<head>\n  <title>Not Found</title>\n</head>\n<body>\n  <h1>Not Found</h1><p>The requested resource was not found on this server.</p>\n</body>\n</html>\n"
    } else {
        "\n<!doctype html>\n<html lang=\"en\">\n<head>\n  <title>Server Error (500)</title>\n</head>\n<body>\n  <h1>Server Error (500)</h1><p></p>\n</body>\n</html>\n"
    };
    (
        StatusCode::from_u16(status).unwrap(),
        [("content-type", "text/html; charset=utf-8")],
        body,
    )
        .into_response()
}
fn identity(request: &Request<Body>) -> PlaybackRequest {
    PlaybackRequest {
        channel_id: String::new(),
        query: request
            .uri()
            .query()
            .map(|value| {
                url::form_urlencoded::parse(value.as_bytes())
                    .map(|(key, value)| (key.into_owned(), value.into_owned()))
                    .collect()
            })
            .unwrap_or_default(),
        headers: dispatcharr_contracts::source_headers(
            request
                .headers()
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_bytes())),
        ),
        peer_ip: request
            .extensions()
            .get::<ConnectInfo<SocketAddr>>()
            .map(|value| value.0.ip())
            .unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST)),
    }
}
async fn dispatch(State(gateway): State<XcGateway>, request: Request<Body>) -> Response {
    match gateway.handle(request).await {
        Ok(response) => response,
        Err(StoreError::XcDenied {
            status,
            retry_after,
        }) => {
            // JsonResponse uses default JSON spaces; the generic Store error
            // adapter has compact JSON for its existing API consumers.
            let data: &'static [u8] = match status {
                403 => b"{\"error\": \"Forbidden\"}",
                429 => b"{\"error\": \"Too many attempts\"}",
                _ => b"{\"error\": \"Unauthorized\"}",
            };
            let mut response = Response::new(Body::from(Bytes::from_static(data)));
            *response.status_mut() =
                StatusCode::from_u16(status).unwrap_or(StatusCode::UNAUTHORIZED);
            response
                .headers_mut()
                .insert("content-type", "application/json".parse().unwrap());
            if let Some(wait) = retry_after {
                response.headers_mut().insert("retry-after", wait.into());
            }
            response
        }
        Err(error) => error.into_response(),
    }
}
#[path = "live.rs"]
mod live;
#[cfg(test)]
#[path = "gateway_tests.rs"]
mod tests;
pub use live::LiveDispatcher;
