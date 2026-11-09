use axum::{
    body::Body,
    extract::{ConnectInfo, State},
    http::{HeaderMap, Method, Request, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Json, Router,
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
pub struct HdhrConfig {
    pub max_requests: usize,
    pub max_channels: usize,
    pub max_devices: usize,
    pub max_body_bytes: usize,
    pub max_metadata_bytes: usize,
    pub max_response_bytes: usize,
    pub max_memory_bytes: usize,
    pub request_deadline: Duration,
}
impl Default for HdhrConfig {
    fn default() -> Self {
        Self {
            max_requests: 32,
            max_channels: 50000,
            max_devices: 4096,
            max_body_bytes: 64 * 1024,
            max_metadata_bytes: 8 * 1024 * 1024,
            max_response_bytes: 16 * 1024 * 1024,
            max_memory_bytes: 128 * 1024 * 1024,
            request_deadline: Duration::from_secs(15),
        }
    }
}
pub(crate) struct Inner {
    pub store: Store,
    pub auth: AuthService,
    pub config: HdhrConfig,
    requests: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    stopped: AtomicBool,
    workers: AtomicUsize,
    done: Notify,
}
#[derive(Clone)]
pub struct HdhrGateway {
    pub(crate) inner: Arc<Inner>,
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
pub(crate) enum Endpoint {
    Root,
    Devices {
        lookup: Option<String>,
    },
    Discover {
        profile: Option<String>,
        output: Option<String>,
    },
    Lineup {
        profile: Option<String>,
        output: Option<String>,
    },
    Status,
    Xml,
}
impl HdhrGateway {
    pub fn new(store: Store, auth: AuthService, config: HdhrConfig) -> Result<Self, StoreError> {
        if config.max_requests == 0
            || config.max_requests > 128
            || config.max_channels == 0
            || config.max_channels > 50000
            || config.max_devices == 0
            || config.max_devices > 50000
            || config.max_body_bytes == 0
            || config.max_body_bytes > 1024 * 1024
            || config.max_metadata_bytes == 0
            || config.max_metadata_bytes > 32 * 1024 * 1024
            || config.max_response_bytes == 0
            || config.max_response_bytes > 64 * 1024 * 1024
            || config.max_memory_bytes
                < config.max_metadata_bytes * 2 + config.max_response_bytes * 2
            || config.max_memory_bytes > 512 * 1024 * 1024
            || config.request_deadline.is_zero()
        {
            return Err(StoreError::Unsupported("invalid HDHR resource budget"));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                store,
                auth,
                requests: Arc::new(Semaphore::new(config.max_requests)),
                bytes: Arc::new(Semaphore::new(config.max_memory_bytes)),
                config,
                stopped: AtomicBool::new(false),
                workers: AtomicUsize::new(0),
                done: Notify::new(),
            }),
        })
    }
    pub fn router(&self) -> Router {
        let mut router = Router::new().route("/hdhr", any(redirect));
        for prefix in ["/hdhr", "/api/hdhr"] {
            router = router
                .route(&format!("{prefix}/"), any(dispatch))
                .route(&format!("{prefix}/devices/"), any(dispatch))
                .route(&format!("{prefix}/devices.json"), any(dispatch))
                .route(&format!("{prefix}/devices.json/"), any(dispatch))
                .route(&format!("{prefix}/devices/{{lookup}}"), any(dispatch))
                .route(&format!("{prefix}/devices/{{lookup}}/"), any(dispatch));
            for route in [
                "discover.json",
                "lineup.json",
                "lineup_status.json",
                "device.xml",
            ] {
                router = router.route(&format!("{prefix}/{route}"), any(dispatch));
            }
        }
        for prefix in [
            "/hdhr/{profile}",
            "/hdhr/output_profile/{output}",
            "/hdhr/{profile}/output_profile/{output}",
        ] {
            for route in ["discover.json", "lineup.json", "lineup_status.json"] {
                router = router.route(&format!("{prefix}/{route}"), any(dispatch));
            }
        }
        router.with_state(self.clone())
    }
    fn retained(
        &self,
        status: StatusCode,
        data: Option<Vec<u8>>,
        memory: OwnedSemaphorePermit,
        head: bool,
    ) -> Result<Response, StoreError> {
        let Some(data) = data else {
            return Ok(status.into_response());
        };
        if data.len() > self.inner.config.max_response_bytes {
            return Err(unavailable());
        }
        let length = data.len();
        let mut memory = memory;
        let retained = memory.split(data.capacity()).ok_or_else(unavailable)?;
        drop(memory);
        let payload = Bytes::from_owner(Payload {
            data,
            _memory: retained,
        });
        let mut response = Response::new(if head {
            Body::empty()
        } else {
            Body::from(payload)
        });
        *response.status_mut() = status;
        response
            .headers_mut()
            .insert("content-type", "application/json".parse().unwrap());
        response
            .headers_mut()
            .insert("content-length", length.into());
        Ok(response)
    }
    pub async fn shutdown(&self) -> bool {
        self.inner.stopped.store(true, Ordering::Release);
        self.inner.requests.close();
        self.inner.bytes.close();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let done = self.inner.done.notified();
                if self.inner.workers.load(Ordering::Acquire) == 0 {
                    break;
                }
                done.await;
            }
        })
        .await
        .is_ok()
    }
    pub fn available_response_bytes(&self) -> usize {
        self.inner.bytes.available_permits()
    }
    pub fn active_requests(&self) -> usize {
        self.inner.workers.load(Ordering::Acquire)
    }
    async fn handle(&self, request: Request<Body>) -> Result<Response, StoreError> {
        if self.inner.stopped.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        let permit = self
            .inner
            .requests
            .clone()
            .try_acquire_owned()
            .map_err(|_| unavailable())?;
        self.inner.workers.fetch_add(1, Ordering::AcqRel);
        let worker = Worker(self.inner.clone());
        let gateway = self.clone();
        // The owned worker survives HTTP cancellation; its admission and build
        // reservation remain until the actual database/authentication work ends.
        tokio::spawn(async move {
            let _worker = worker;
            let _permit = permit;
            tokio::time::timeout(
                gateway.inner.config.request_deadline,
                gateway.process(request),
            )
            .await
            .map_err(|_| unavailable())?
        })
        .await
        .map_err(|_| unavailable())?
    }
    async fn process(&self, request: Request<Body>) -> Result<Response, StoreError> {
        let reservation =
            self.inner.config.max_metadata_bytes * 2 + self.inner.config.max_response_bytes * 2;
        let memory = self
            .inner
            .bytes
            .clone()
            .acquire_many_owned(reservation as u32)
            .await
            .map_err(|_| unavailable())?;
        let mut header_bytes = request.uri().to_string().len();
        for (name, value) in request.headers() {
            header_bytes = header_bytes
                .checked_add(name.as_str().len() + value.as_bytes().len() + 4)
                .ok_or_else(unavailable)?;
        }
        if header_bytes > 64 * 1024 {
            return Err(unavailable());
        }
        let identity = identity(&request)?;
        let method = request.method().clone();
        let headers = request.headers();
        let endpoint = endpoint(request.uri().path())?;
        // DRF content negotiation precedes permission checks and GET handlers.
        if identity
            .parameter("format")
            .is_some_and(|value| !value.is_empty() && value != "json")
        {
            return Ok(error(StatusCode::NOT_FOUND, "Not found."));
        }
        let django_response = matches!(method, Method::GET | Method::HEAD)
            && matches!(
                &endpoint,
                Endpoint::Discover { .. }
                    | Endpoint::Lineup { .. }
                    | Endpoint::Status
                    | Endpoint::Xml
            );
        if headers
            .get("accept")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|value| {
                !value.split(',').any(|part| {
                    let value = part.split(';').next().unwrap_or("").trim();
                    matches!(value, "application/json" | "*/*" | "application/*")
                        || (django_response && matches!(value, "text/html" | "text/*"))
                })
            })
        {
            return Ok(error(
                StatusCode::NOT_ACCEPTABLE,
                "Could not satisfy the request Accept header.",
            ));
        }
        let origin = origin(&self.inner.auth, &identity, headers);
        if let Endpoint::Devices { lookup } = endpoint {
            let headers = headers.clone();
            let (status, value) = crate::devices::handle(
                &self.inner,
                &identity,
                lookup,
                &method,
                &headers,
                request.into_body(),
            )
            .await?;
            return self.retained(status, value, memory, method == Method::HEAD);
        }
        if let Endpoint::Root = endpoint {
            self.inner.auth.authorize_ui(&identity, true).await?;
            if method != Method::GET && method != Method::HEAD && method != Method::OPTIONS {
                return Ok(error(
                    StatusCode::METHOD_NOT_ALLOWED,
                    &format!("Method \"{method}\" not allowed."),
                ));
            }
            let value = if method == Method::OPTIONS {
                json!({"name":"Api Root","description":"The default basic root view for DefaultRouter","renders":["application/json","text/html"],"parses":["application/json","application/x-www-form-urlencoded","multipart/form-data"]})
            } else {
                json!({"devices":format!("{origin}{}devices/",request.uri().path())})
            };
            return self.retained(
                StatusCode::OK,
                Some(serde_json::to_vec(&value).unwrap()),
                memory,
                method == Method::HEAD,
            );
        }
        self.inner.auth.authenticate(&identity).await?;
        let mut response = if method == Method::OPTIONS {
            Json(json!({"name":match endpoint{Endpoint::Discover{..}=>"Discover Api",Endpoint::Lineup{..}=>"Lineup Api",Endpoint::Status=>"Lineup Status Api",Endpoint::Xml=>"Hdhr Device Xml Api",_=>unreachable!()},"description":match endpoint{Endpoint::Discover{..}=>"Returns device discovery information",Endpoint::Lineup{..}=>"Returns available channel lineup",Endpoint::Status=>"Returns the current status of the HDHR lineup",Endpoint::Xml=>"Returns HDHomeRun device configuration in XML",_=>unreachable!()},"renders":["application/json","text/html"],"parses":["application/json","application/x-www-form-urlencoded","multipart/form-data"]})).into_response()
        } else if method != Method::GET && method != Method::HEAD {
            error(
                StatusCode::METHOD_NOT_ALLOWED,
                &format!("Method \"{method}\" not allowed."),
            )
        } else if !self
            .inner
            .auth
            .source_network_allowed(self.inner.auth.client_ip(&identity), "M3U_EPG", None)
            .await?
        {
            (StatusCode::FORBIDDEN, Json(json!({"error":"Forbidden"}))).into_response()
        } else {
            let (content_type, data) = crate::read::read(&self.inner, endpoint, &origin).await?;
            if data.len() > self.inner.config.max_response_bytes {
                return Err(unavailable());
            }
            let mut memory = memory;
            let retained = memory.split(data.capacity()).ok_or_else(unavailable)?;
            drop(memory);
            let length = data.len();
            let payload = Bytes::from_owner(Payload {
                data,
                _memory: retained,
            });
            let mut response = Response::new(if method == Method::HEAD {
                Body::empty()
            } else {
                Body::from(payload)
            });
            response
                .headers_mut()
                .insert("content-type", content_type.parse().unwrap());
            response
                .headers_mut()
                .insert("content-length", length.into());
            response
        };
        response
            .headers_mut()
            .insert("allow", "GET, HEAD, OPTIONS".parse().unwrap());
        Ok(response)
    }
}
async fn redirect() -> Response {
    let mut result = StatusCode::MOVED_PERMANENTLY.into_response();
    result
        .headers_mut()
        .insert("location", "/hdhr/".parse().unwrap());
    result
        .headers_mut()
        .insert("content-type", "text/html; charset=utf-8".parse().unwrap());
    result
        .headers_mut()
        .insert("vary", "origin".parse().unwrap());
    result
}
async fn dispatch(State(gateway): State<HdhrGateway>, request: Request<Body>) -> Response {
    let path = request.uri().path();
    let allow = match endpoint(path) {
        Ok(Endpoint::Devices { lookup: None }) => "GET, POST, HEAD, OPTIONS",
        Ok(Endpoint::Devices { lookup: Some(_) }) => "GET, PUT, PATCH, DELETE, HEAD, OPTIONS",
        _ => "GET, HEAD, OPTIONS",
    };
    let mut response = gateway
        .handle(request)
        .await
        .unwrap_or_else(IntoResponse::into_response);
    response
        .headers_mut()
        .insert("allow", allow.parse().unwrap());
    response
        .headers_mut()
        .insert("vary", "Accept, origin".parse().unwrap());
    response
}
pub(crate) fn unavailable() -> StoreError {
    StoreError::StatusBody(
        503,
        json!({"detail":"HDHR resource capacity is unavailable."}),
    )
}
fn error(status: StatusCode, detail: &str) -> Response {
    (status, Json(json!({"detail":detail}))).into_response()
}
fn identity(request: &Request<Body>) -> Result<PlaybackRequest, StoreError> {
    for key in ["authorization", "x-api-key"] {
        let values: Vec<_> = request.headers().get_all(key).iter().collect();
        if values.len() > 1 || values.iter().any(|value| value.to_str().is_err()) {
            return Err(StoreError::Unauthorized(
                "Invalid authentication credentials.",
            ));
        }
    }
    Ok(PlaybackRequest {
        channel_id: String::new(),
        query: request
            .uri()
            .query()
            .map(|value| {
                url::form_urlencoded::parse(value.as_bytes())
                    .into_owned()
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
            .map_or(IpAddr::V4(Ipv4Addr::LOCALHOST), |peer| peer.0.ip()),
    })
}
fn endpoint(path: &str) -> Result<Endpoint, StoreError> {
    let api_namespace = path.starts_with("/api/hdhr/");
    let path = path
        .strip_prefix("/api/hdhr/")
        .or_else(|| path.strip_prefix("/hdhr/"))
        .ok_or(StoreError::NotFound)?;
    if path.is_empty() {
        return Ok(Endpoint::Root);
    }
    if matches!(path, "devices/" | "devices.json" | "devices.json/") {
        return Ok(Endpoint::Devices { lookup: None });
    }
    if let Some(value) = path.strip_prefix("devices/") {
        let value = value.trim_end_matches('/');
        // Original profile routes precede DefaultRouter device suffix routes.
        let profile_route = !api_namespace
            && (value.contains('/')
                || matches!(
                    value,
                    "discover.json" | "lineup.json" | "lineup_status.json"
                ));
        if !profile_route {
            let value = value.strip_suffix(".json").unwrap_or(value);
            return Ok(Endpoint::Devices {
                lookup: Some(normalize_id(value).unwrap_or_else(|_| value.to_string())),
            });
        }
    }
    let parts: Vec<_> = path.split('/').collect();
    let route = *parts.last().ok_or(StoreError::NotFound)?;
    let decode = |value: &str| {
        url::form_urlencoded::parse(format!("v={}", value.replace('+', "%2B")).as_bytes())
            .next()
            .map(|(_, v)| v.into_owned())
            .unwrap_or_default()
    };
    let (profile, output) = match &parts[..parts.len() - 1] {
        [] => (None, None),
        [profile] => (Some(decode(profile)), None),
        ["output_profile", output] => (None, Some(normalize_id(output)?)),
        [profile, "output_profile", output] => (Some(decode(profile)), Some(normalize_id(output)?)),
        _ => return Err(StoreError::NotFound),
    };
    match route {
        "discover.json" => Ok(Endpoint::Discover { profile, output }),
        "lineup.json" => Ok(Endpoint::Lineup { profile, output }),
        "lineup_status.json" => Ok(Endpoint::Status),
        "device.xml" => Ok(Endpoint::Xml),
        _ => Err(StoreError::NotFound),
    }
}
fn normalize_id(value: &str) -> Result<String, StoreError> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(StoreError::NotFound);
    }
    let value = value.trim_start_matches('0');
    Ok(if value.is_empty() { "0" } else { value }.to_string())
}
// Port of core.utils._resolve_host_port_scheme; preserves its first-colon split.
fn origin(auth: &AuthService, identity: &PlaybackRequest, _headers: &HeaderMap) -> String {
    let header = |key| identity.header(key);
    let trusted = auth.is_trusted_proxy(identity.peer_ip);
    let scheme = if trusted {
        header("x-forwarded-proto")
            .and_then(|value| value.split(',').next())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("http")
    } else {
        "http"
    };
    let standard = if scheme == "https" { "443" } else { "80" };
    let combine = |host: &str, port: &str| {
        if port == standard {
            format!("{scheme}://{host}")
        } else {
            format!("{scheme}://{host}:{port}")
        }
    };
    if trusted {
        if let Some(host) = header("x-forwarded-host") {
            if let Some((host, port)) = host.split_once(':') {
                return combine(host, port);
            }
            if let Some(port) = header("x-forwarded-port") {
                return combine(host, port);
            }
            if header("x-forwarded-proto").is_some() {
                return format!("{scheme}://{host}");
            }
        }
    }
    let host = header("host").unwrap_or("localhost");
    if let Some((host, port)) = host.split_once(':') {
        return combine(host, port);
    }
    if trusted {
        if let Some(port) = header("x-forwarded-port") {
            return combine(host, port);
        }
    }
    format!("{scheme}://{host}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    async fn fixture(config: HdhrConfig) -> HdhrGateway {
        let store = Store::connect(
            &std::env::var("DATABASE_TEST_URL").expect("real PostgreSQL fixture"),
            5,
        )
        .await
        .unwrap();
        let auth = AuthService::new(
            store.clone(),
            dispatcharr_store::AuthConfig::new(std::env::var("SECRET_KEY").unwrap()).unwrap(),
        );
        HdhrGateway::new(store, auth, config).unwrap()
    }
    fn request(path: &str) -> Request<Body> {
        Request::builder()
            .uri(path)
            .header("host", "hdhr.fixture.invalid:8088")
            .body(Body::empty())
            .unwrap()
    }
    #[tokio::test]
    #[ignore = "requires disposable original PostgreSQL schema"]
    async fn held_final_frame_retains_budget_after_shutdown() {
        let config = HdhrConfig::default();
        let budget = config.max_memory_bytes;
        let gateway = fixture(config).await;
        let response = gateway.handle(request("/hdhr/device.xml")).await.unwrap();
        let mut body = response.into_body();
        let frame = body.frame().await.unwrap().unwrap().into_data().unwrap();
        let length = frame.len();
        let clone = frame.clone();
        drop(frame);
        drop(body);
        let charged = budget - gateway.available_response_bytes();
        assert!(charged >= length);
        gateway.shutdown().await;
        assert_eq!(gateway.active_requests(), 0);
        assert_eq!(gateway.available_response_bytes(), budget - charged);
        drop(clone);
        assert_eq!(gateway.available_response_bytes(), budget);
    }
    #[tokio::test]
    #[ignore = "requires disposable original PostgreSQL schema"]
    async fn amplified_lineup_is_capped_and_allocation_capacity_is_leased() {
        let config = HdhrConfig {
            max_response_bytes: 4096,
            ..HdhrConfig::default()
        };
        let budget = config.max_memory_bytes;
        let gateway = fixture(config).await;
        let request = Request::builder()
            .uri("/hdhr/lineup.json")
            .header("host", "a".repeat(2048))
            .body(Body::empty())
            .unwrap();
        assert!(gateway.handle(request).await.is_err());
        assert_eq!(gateway.available_response_bytes(), budget);
        assert_eq!(gateway.active_requests(), 0);
        let mut data = Vec::with_capacity(4096);
        data.extend_from_slice(b"{}");
        let memory = gateway
            .inner
            .bytes
            .clone()
            .acquire_many_owned(8192)
            .await
            .unwrap();
        let response = gateway
            .retained(StatusCode::OK, Some(data), memory, false)
            .unwrap();
        assert_eq!(gateway.available_response_bytes(), budget - 4096);
        drop(response);
        assert_eq!(gateway.available_response_bytes(), budget);
        assert!(gateway.shutdown().await);
    }
    #[tokio::test]
    #[ignore = "requires disposable original PostgreSQL schema"]
    async fn cancelled_caller_holds_admission_until_actual_query_completes() {
        let config = HdhrConfig {
            max_requests: 1,
            ..HdhrConfig::default()
        };
        let budget = config.max_memory_bytes;
        let gateway = fixture(config).await;
        let mut transaction = gateway.inner.store.pool().begin().await.unwrap();
        sqlx::query("LOCK TABLE hdhr_hdhrdevice IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *transaction)
            .await
            .unwrap();
        let cloned = gateway.clone();
        let caller =
            tokio::spawn(async move { cloned.handle(request("/hdhr/discover.json")).await });
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if gateway.active_requests() == 1 && gateway.available_response_bytes() < budget {
                    break;
                }
                tokio::task::yield_now().await
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        caller.abort();
        let _ = caller.await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(gateway.active_requests(), 1);
        assert!(gateway.available_response_bytes() < budget);
        assert!(gateway.handle(request("/hdhr/device.xml")).await.is_err());
        transaction.commit().await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if gateway.active_requests() == 0 {
                    break;
                }
                tokio::task::yield_now().await
            }
        })
        .await
        .unwrap();
        assert_eq!(gateway.available_response_bytes(), budget);
        gateway.shutdown().await;
    }
}
