use axum::{
    body::Body,
    extract::{ConnectInfo, State},
    http::{header, HeaderMap, HeaderValue, Method, Request, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::any,
    Json, Router,
};
use bytes::Bytes;
use dispatcharr_contracts::PlaybackRequest;
use dispatcharr_store::{source_browsable_csrf, AuthService, Store, StoreError};
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
        let browser_options = lineup_status_browser_options(&method, request.uri(), headers);
        let rejection = if browser_options {
            None
        } else {
            negotiation_status(
                &method,
                &endpoint,
                identity.parameter("format"),
                headers.get("accept").and_then(|value| value.to_str().ok()),
            )
        };
        if let Some(status) = rejection {
            let detail = if status == StatusCode::NOT_FOUND {
                "Not found."
            } else {
                "Could not satisfy the request Accept header."
            };
            return Ok(error(status, detail));
        }
        let origin = dispatcharr_contracts::source_output_origin(
            &identity,
            self.inner.auth.is_trusted_proxy(identity.peer_ip),
        );
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
        if let Err(error) = self.inner.auth.authenticate(&identity).await {
            let Some(shell) = lineup_status_auth_shell(
                &method,
                request.uri(),
                identity.parameter("format"),
                headers,
            ) else {
                return Err(error);
            };
            let Some(content) = lineup_status_auth_content(&error) else {
                return Err(error);
            };
            let (body, set_cookie) = lineup_status_auth_html(
                shell,
                content,
                headers,
                self.inner.config.max_response_bytes,
            )?;
            let mut response =
                self.retained(StatusCode::UNAUTHORIZED, Some(body), memory, false)?;
            set_lineup_status_auth_headers(response.headers_mut(), set_cookie);
            return Ok(response);
        }
        if browser_options {
            let shell = if request.uri().query().is_none() {
                LINEUP_STATUS_ORDINARY_BROWSER_OPTIONS_SHELL
            } else {
                LINEUP_STATUS_BROWSER_OPTIONS_SHELL
            };
            let (body, cookie) =
                lineup_status_browser_options_html(shell, self.inner.config.max_response_bytes)?;
            let mut response = self.retained(StatusCode::OK, Some(body), memory, false)?;
            set_lineup_status_browsable_headers(response.headers_mut(), cookie);
            return Ok(response);
        }
        if method == Method::OPTIONS && request.uri().path() == LINEUP_STATUS_PATH {
            return self.retained(
                StatusCode::OK,
                Some(LINEUP_STATUS_OPTIONS.as_bytes().to_vec()),
                memory,
                false,
            );
        }
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
            network_denied()
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
const LINEUP_STATUS_PATH: &str = "/hdhr/lineup_status.json";
const LINEUP_STATUS_OPTIONS: &str = concat!(
    r#"{"name":"Lineup Status Api","description":"Returns the current status of the HDHR lineup","#,
    r#""renders":["application/json","text/html"],"#,
    r#""parses":["application/json","application/x-www-form-urlencoded","multipart/form-data"]}"#,
);

const BROWSABLE_AUTH_VARY: HeaderValue = HeaderValue::from_static("Accept, origin, Cookie");
const LINEUP_STATUS_AUTH_SHELL: &str = include!("../datasets/lineup-status-auth.rs");
const LINEUP_STATUS_ORDINARY_BROWSER_AUTH_SHELL: &str =
    include!("../datasets/lineup-status-ordinary-browser-auth.rs");
const LINEUP_STATUS_BROWSER_OPTIONS_SHELL: &str =
    include!("../datasets/lineup-status-browser-options.rs");
const LINEUP_STATUS_ORDINARY_BROWSER_OPTIONS_SHELL: &str =
    include!("../datasets/lineup-status-ordinary-browser-options.rs");
const ORDINARY_BROWSER_ACCEPT: &str =
    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";

fn lineup_status_browser_options(method: &Method, uri: &Uri, headers: &HeaderMap) -> bool {
    if *method != Method::OPTIONS
        || uri.path() != LINEUP_STATUS_PATH
        || !matches!(uri.query(), None | Some("format=api"))
        || headers.contains_key(header::AUTHORIZATION)
        || headers.contains_key("x-api-key")
        || headers.contains_key(header::COOKIE)
    {
        return false;
    }
    let mut accepts = headers.get_all(header::ACCEPT).iter();
    accepts
        .next()
        .is_some_and(|value| value == ORDINARY_BROWSER_ACCEPT)
        && accepts.next().is_none()
}

fn lineup_status_browser_options_html(
    shell: &str,
    maximum: usize,
) -> Result<(Vec<u8>, HeaderValue), StoreError> {
    let (before, after) = shell
        .split_once("@@HDHR_CSRF_SCRIPT@@")
        .expect("captured HDHR OPTIONS CSRF slot");
    let (_, masked, set_cookie) = source_browsable_csrf(None);
    let length = before.len() + masked.len() + after.len();
    if length > maximum {
        return Err(unavailable());
    }
    let mut body = Vec::with_capacity(length);
    for part in [before, &masked, after] {
        body.extend_from_slice(part.as_bytes());
    }
    let cookie = HeaderValue::from_str(&set_cookie).map_err(|_| unavailable())?;
    Ok((body, cookie))
}

fn lineup_status_auth_shell(
    method: &Method,
    uri: &Uri,
    format: Option<&str>,
    headers: &HeaderMap,
) -> Option<&'static str> {
    if *method != Method::GET || uri.path() != LINEUP_STATUS_PATH {
        return None;
    }
    if format == Some("api") {
        return Some(LINEUP_STATUS_AUTH_SHELL);
    }
    let mut accepts = headers.get_all(header::ACCEPT).iter();
    (uri.query().is_none()
        && accepts
            .next()
            .is_some_and(|accept| accept == ORDINARY_BROWSER_ACCEPT)
        && accepts.next().is_none())
    .then_some(LINEUP_STATUS_ORDINARY_BROWSER_AUTH_SHELL)
}

fn set_lineup_status_auth_headers(headers: &mut HeaderMap, set_cookie: HeaderValue) {
    set_lineup_status_browsable_headers(headers, set_cookie);
    headers.insert(
        header::WWW_AUTHENTICATE,
        HeaderValue::from_static("Bearer realm=\"api\""),
    );
}

fn set_lineup_status_browsable_headers(headers: &mut HeaderMap, set_cookie: HeaderValue) {
    for (name, value) in [
        (
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/html; charset=utf-8"),
        ),
        (header::VARY, BROWSABLE_AUTH_VARY),
        (header::SET_COOKIE, set_cookie),
    ] {
        headers.insert(name, value);
    }
}

fn lineup_status_auth_content(error: &StoreError) -> Option<&'static str> {
    let value = match error {
        StoreError::Unauthorized("User inactive") => return Some(INACTIVE_API_KEY_HTML),
        StoreError::AuthenticationBody(value) => value,
        _ => return None,
    };
    if value == &json!({"detail":"User is inactive","code":"user_inactive"}) {
        return Some(INACTIVE_BEARER_HTML);
    }
    (value == &json!({
        "detail":"Given token not valid for any token type",
        "code":"token_not_valid",
        "messages":[{"token_class":"AccessToken","token_type":"access","message":"Token is expired"}]
    }))
        .then_some(EXPIRED_BEARER_HTML)
}

// Keep the Source document order and escaping for these observed error shapes.
const INACTIVE_API_KEY_HTML: &str = r#"{
    &quot;detail&quot;: &quot;User inactive&quot;
}"#;
const INACTIVE_BEARER_HTML: &str = r#"{
    &quot;detail&quot;: &quot;User is inactive&quot;,
    &quot;code&quot;: &quot;user_inactive&quot;
}"#;
const EXPIRED_BEARER_HTML: &str = r#"{
    &quot;detail&quot;: &quot;Given token not valid for any token type&quot;,
    &quot;code&quot;: &quot;token_not_valid&quot;,
    &quot;messages&quot;: [
        {
            &quot;token_class&quot;: &quot;AccessToken&quot;,
            &quot;token_type&quot;: &quot;access&quot;,
            &quot;message&quot;: &quot;Token is expired&quot;
        }
    ]
}"#;

fn lineup_status_auth_html(
    shell: &str,
    content: &str,
    headers: &HeaderMap,
    maximum: usize,
) -> Result<(Vec<u8>, HeaderValue), StoreError> {
    let (before, after) = shell
        .split_once("@@HDHR_ERROR_CONTENT@@")
        .expect("captured HDHR error slot");
    let (middle, end) = after
        .split_once("@@HDHR_CSRF_SCRIPT@@")
        .expect("captured HDHR CSRF slot");
    let cookie = headers.get_all(header::COOKIE).iter().find_map(|raw| {
        raw.to_str().ok()?.split(';').find_map(|part| {
            let (name, value) = part.trim().split_once('=')?;
            (name == "csrftoken").then_some(value)
        })
    });
    let (_, masked, set_cookie) = source_browsable_csrf(cookie);
    let pieces = [before, content, middle, &masked, end];
    let length = pieces.iter().map(|piece| piece.len()).sum();
    if length > maximum {
        return Err(unavailable());
    }
    let mut body = Vec::with_capacity(length);
    for piece in pieces {
        body.extend_from_slice(piece.as_bytes());
    }
    let cookie = HeaderValue::from_str(&set_cookie).map_err(|_| unavailable())?;
    Ok((body, cookie))
}

// The four public Django GET handlers return their own JSON/XML entity after
// DRF selects a renderer. The format alias selects the browsable renderer;
// it does not change those concrete HttpResponse bodies or network checks.
fn negotiation_status(
    method: &Method,
    endpoint: &Endpoint,
    format: Option<&str>,
    accept: Option<&str>,
) -> Option<StatusCode> {
    let django_response = matches!(*method, Method::GET | Method::HEAD)
        && matches!(
            endpoint,
            Endpoint::Discover { .. } | Endpoint::Lineup { .. } | Endpoint::Status | Endpoint::Xml
        );
    // The router registers these Source data views in both namespaces. Every
    // GET and inherited HEAD negotiates first, then returns its own entity.
    let browser_format = format == Some("api") && django_response;
    if !browser_format && format.is_some_and(|value| !value.is_empty() && value != "json") {
        return Some(StatusCode::NOT_FOUND);
    }
    // An explicit format restricts the eligible renderer before Accept
    // matching. With no selector, public data views can use either renderer.
    let accepts_html = browser_format || (django_response && format != Some("json"));
    let accepts_json = !browser_format;
    let rejected = accept.is_some_and(|value| {
        !value.split(',').any(|part| {
            let value = part.split(';').next().unwrap_or("").trim();
            let matches =
                |names: &[&str]| names.iter().any(|kind| value.eq_ignore_ascii_case(kind));
            value == "*/*"
                || (accepts_json && matches(&["application/json", "application/*"]))
                || (accepts_html && matches(&["text/html", "text/*"]))
        })
    });
    rejected.then_some(StatusCode::NOT_ACCEPTABLE)
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
    set_dispatch_headers(response.headers_mut(), allow);
    response
}

fn set_dispatch_headers(headers: &mut HeaderMap, allow: &'static str) {
    headers.insert(header::ALLOW, HeaderValue::from_static(allow));
    if headers.get(header::VARY) != Some(&BROWSABLE_AUTH_VARY) {
        headers.insert(header::VARY, HeaderValue::from_static("Accept, origin"));
    }
}

pub(crate) fn unavailable() -> StoreError {
    StoreError::StatusBody(
        503,
        json!({"detail":"HDHR resource capacity is unavailable."}),
    )
}
fn network_denied() -> Response {
    // Django's JsonResponse includes the space after the colon.
    let body = r#"{"error": "Forbidden"}"#;
    (
        StatusCode::FORBIDDEN,
        [
            ("content-type", "application/json"),
            ("content-length", "22"),
        ],
        body,
    )
        .into_response()
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
        // The router selects a path segment, not a query parameter. Protect
        // literal path '+' and '&' before decoding percent escapes.
        let escaped = value.replace('+', "%2B").replace('&', "%26");
        url::form_urlencoded::parse(format!("v={escaped}").as_bytes())
            .next()
            .map(|(_, v)| v.into_owned())
            .unwrap_or_default()
    };
    let (profile, output) = match &parts[..parts.len() - 1] {
        [] => (None, None),
        [profile] => (Some(decode(profile)), None),
        // Django's int converter sees the decoded path segment. Keep the
        // existing ASCII-digit check and canonicalize leading zeros afterward.
        ["output_profile", output] => (None, Some(normalize_id(&decode(output))?)),
        [profile, "output_profile", output] => {
            (Some(decode(profile)), Some(normalize_id(&decode(output))?))
        }
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
#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    #[test]
    fn selects_only_the_observed_anonymous_fixed_browser_options() {
        let uri: Uri = "/hdhr/lineup_status.json?format=api".parse().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            header::ACCEPT,
            HeaderValue::from_static(ORDINARY_BROWSER_ACCEPT),
        );
        assert!(lineup_status_browser_options(
            &Method::OPTIONS,
            &uri,
            &headers
        ));
        let ordinary: Uri = "/hdhr/lineup_status.json".parse().unwrap();
        assert!(lineup_status_browser_options(
            &Method::OPTIONS,
            &ordinary,
            &headers
        ));
        for method in [Method::GET, Method::HEAD] {
            assert!(!lineup_status_browser_options(&method, &uri, &headers));
            assert!(!lineup_status_browser_options(&method, &ordinary, &headers));
        }
        for path in [
            "/hdhr/lineup_status.json?",
            "/hdhr/lineup_status.json?format=json",
            "/hdhr/lineup_status.json?format=api&other=1",
            "/api/hdhr/lineup_status.json?format=api",
        ] {
            assert!(!lineup_status_browser_options(
                &Method::OPTIONS,
                &path.parse().unwrap(),
                &headers
            ));
        }
        for header_name in [
            header::AUTHORIZATION,
            header::COOKIE,
            header::HeaderName::from_static("x-api-key"),
        ] {
            headers.insert(header_name.clone(), HeaderValue::from_static("fixture"));
            assert!(!lineup_status_browser_options(
                &Method::OPTIONS,
                &uri,
                &headers
            ));
            assert!(!lineup_status_browser_options(
                &Method::OPTIONS,
                &ordinary,
                &headers
            ));
            headers.remove(header_name);
        }
        headers.append(header::ACCEPT, HeaderValue::from_static("application/json"));
        assert!(!lineup_status_browser_options(
            &Method::OPTIONS,
            &uri,
            &headers
        ));
        headers.insert(header::ACCEPT, HeaderValue::from_static("application/json"));
        assert!(!lineup_status_browser_options(
            &Method::OPTIONS,
            &uri,
            &headers
        ));
    }

    #[test]
    fn fixed_browser_options_use_the_stock_shell_with_fresh_csrf() {
        let (body, cookie) =
            lineup_status_browser_options_html(LINEUP_STATUS_BROWSER_OPTIONS_SHELL, 10642).unwrap();
        let (other_body, other_cookie) =
            lineup_status_browser_options_html(LINEUP_STATUS_BROWSER_OPTIONS_SHELL, 10642).unwrap();
        assert_eq!(body.len(), 10642);
        assert_ne!(body, other_body);
        assert_ne!(cookie, other_cookie);
        assert!(
            lineup_status_browser_options_html(LINEUP_STATUS_BROWSER_OPTIONS_SHELL, 10641).is_err()
        );
        let text = std::str::from_utf8(&body).unwrap();
        assert!(text.contains("HTTP 200 OK"));
        assert!(text.contains("&quot;Lineup Status Api&quot;"));
        assert!(!text.contains("@@"));
        let (_, inline) = text.split_once("\"csrfToken\": \"").unwrap();
        let (masked, _) = inline.split_once('"').unwrap();
        assert_eq!(masked.len(), 64);
        assert!(masked.bytes().all(|byte| byte.is_ascii_alphanumeric()));

        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        headers.insert(header::CONTENT_LENGTH, HeaderValue::from_static("10642"));
        set_lineup_status_browsable_headers(&mut headers, cookie);
        set_dispatch_headers(&mut headers, "GET, HEAD, OPTIONS");
        assert_eq!(headers.get_all(header::CONTENT_TYPE).iter().count(), 1);
        assert_eq!(headers[header::CONTENT_TYPE], "text/html; charset=utf-8");
        assert_eq!(headers[header::CONTENT_LENGTH], "10642");
        assert_eq!(headers[header::VARY], BROWSABLE_AUTH_VARY);
        assert_eq!(headers.get_all(header::SET_COOKIE).iter().count(), 1);
        assert!(!headers.contains_key(header::WWW_AUTHENTICATE));
    }

    #[test]
    fn fixed_browser_options_uses_the_exact_no_query_shell() {
        let (body, _) =
            lineup_status_browser_options_html(LINEUP_STATUS_ORDINARY_BROWSER_OPTIONS_SHELL, 10587)
                .unwrap();
        assert_eq!(body.len(), 10587);
        assert!(!std::str::from_utf8(&body).unwrap().contains("@@"));
        assert!(lineup_status_browser_options_html(
            LINEUP_STATUS_ORDINARY_BROWSER_OPTIONS_SHELL,
            10586,
        )
        .is_err());
    }

    #[test]
    fn selects_the_captured_ordinary_browser_auth_shell_only_for_fixed_get() {
        let uri: Uri = "/hdhr/lineup_status.json".parse().unwrap();
        let forced: Uri = "/hdhr/lineup_status.json?format=api".parse().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            header::ACCEPT,
            HeaderValue::from_static(ORDINARY_BROWSER_ACCEPT),
        );
        assert_eq!(
            lineup_status_auth_shell(&Method::GET, &uri, None, &headers),
            Some(LINEUP_STATUS_ORDINARY_BROWSER_AUTH_SHELL)
        );
        assert_eq!(
            lineup_status_auth_shell(&Method::GET, &forced, Some("api"), &headers),
            Some(LINEUP_STATUS_AUTH_SHELL)
        );
        for request_uri in [
            "/hdhr/lineup_status.json?format=json",
            "/hdhr/lineup_status.json?ordinary=1",
            "/hdhr/lineup_status.json?",
            "/hdhr/profile/lineup_status.json",
        ] {
            assert!(lineup_status_auth_shell(
                &Method::GET,
                &request_uri.parse().unwrap(),
                None,
                &headers,
            )
            .is_none());
        }
        assert!(lineup_status_auth_shell(&Method::HEAD, &uri, None, &headers).is_none());
        assert!(lineup_status_auth_shell(&Method::OPTIONS, &uri, None, &headers).is_none());
        headers.append(header::ACCEPT, HeaderValue::from_static("application/json"));
        assert!(lineup_status_auth_shell(&Method::GET, &uri, None, &headers).is_none());
        headers.insert(header::ACCEPT, HeaderValue::from_static("application/json"));
        assert!(lineup_status_auth_shell(&Method::GET, &uri, None, &headers).is_none());
        headers.clear();
        assert!(lineup_status_auth_shell(&Method::GET, &uri, None, &headers).is_none());
    }

    #[test]
    fn fixed_status_browsable_auth_preserves_captured_shapes_and_response_limit() {
        let inactive_bearer = StoreError::AuthenticationBody(
            json!({"detail":"User is inactive","code":"user_inactive"}),
        );
        let expired = StoreError::AuthenticationBody(json!({
            "detail":"Given token not valid for any token type",
            "code":"token_not_valid",
            "messages":[{"token_class":"AccessToken","token_type":"access","message":"Token is expired"}]
        }));
        let mut headers = HeaderMap::new();
        let secret = "a".repeat(32);
        headers.insert(
            header::COOKIE,
            format!("unrelated=ordinary; csrftoken={secret}")
                .parse()
                .unwrap(),
        );
        for (error, length) in [
            (StoreError::Unauthorized("User inactive"), 10317),
            (inactive_bearer, 10369),
            (expired, 10630),
        ] {
            let content = lineup_status_auth_content(&error).unwrap();
            let (body, cookie) =
                lineup_status_auth_html(LINEUP_STATUS_AUTH_SHELL, content, &headers, length)
                    .unwrap();
            assert_eq!(body.len(), length);
            let body = std::str::from_utf8(&body).unwrap();
            assert!(body.contains(content));
            let (_, script) = body.split_once("\"csrfToken\": \"").unwrap();
            let (masked, _) = script.split_once('"').unwrap();
            assert_eq!(masked.len(), 64);
            assert!(masked.bytes().all(|byte| byte.is_ascii_alphanumeric()));
            assert!(cookie
                .to_str()
                .unwrap()
                .starts_with(&format!("csrftoken={secret};")));
            assert!(lineup_status_auth_html(
                LINEUP_STATUS_AUTH_SHELL,
                content,
                &headers,
                length - 1
            )
            .is_err());
        }
        assert!(lineup_status_auth_content(&StoreError::Unauthorized("Invalid API key")).is_none());
        assert!(lineup_status_auth_content(&StoreError::RateLimited(1)).is_none());
    }

    #[test]
    fn browsable_auth_replaces_retained_json_type_without_duplicate_headers() {
        let mut response = StoreError::Unauthorized("User inactive").into_response();
        let headers = response.headers_mut();
        // The body-retention helper initializes Content-Type and Content-Length.
        headers.insert(header::CONTENT_LENGTH, HeaderValue::from_static("10317"));
        let (_, _, cookie) = source_browsable_csrf(None);
        let cookie = cookie.parse::<HeaderValue>().unwrap();
        set_lineup_status_auth_headers(headers, cookie.clone());
        set_dispatch_headers(headers, "GET, HEAD, OPTIONS");

        for (name, expected) in [
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("text/html; charset=utf-8"),
            ),
            (header::CONTENT_LENGTH, HeaderValue::from_static("10317")),
            (header::VARY, BROWSABLE_AUTH_VARY),
            (
                header::WWW_AUTHENTICATE,
                HeaderValue::from_static("Bearer realm=\"api\""),
            ),
            (
                header::ALLOW,
                HeaderValue::from_static("GET, HEAD, OPTIONS"),
            ),
            (header::SET_COOKIE, cookie),
        ] {
            assert_eq!(
                headers.get_all(name).iter().collect::<Vec<_>>(),
                [&expected]
            );
        }

        let mut json = StoreError::Unauthorized("Invalid API key").into_response();
        set_dispatch_headers(json.headers_mut(), "GET, HEAD, OPTIONS");
        assert_eq!(json.headers()[header::CONTENT_TYPE], "application/json");
        assert_eq!(json.headers()[header::VARY], "Accept, origin");
        assert!(json.headers().get(header::SET_COOKIE).is_none());

        let mut success = Json(json!({"ScanInProgress": 0})).into_response();
        set_dispatch_headers(success.headers_mut(), "GET, HEAD, OPTIONS");
        assert_eq!(success.status(), StatusCode::OK);
        assert_eq!(success.headers()[header::CONTENT_TYPE], "application/json");
        assert_eq!(success.headers()[header::VARY], "Accept, origin");
        assert!(success.headers().get(header::WWW_AUTHENTICATE).is_none());
    }

    #[tokio::test]
    async fn network_denial_uses_the_exact_django_json_bytes() {
        let response = network_denied();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(response.headers()["content-type"], "application/json");
        assert_eq!(response.headers()["content-length"], "22");
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body.as_ref(), br#"{"error": "Forbidden"}"#);
        assert_eq!(body.len(), 22);
    }

    #[test]
    fn browser_format_keeps_the_public_django_response() {
        let browser = Some(ORDINARY_BROWSER_ACCEPT);
        for path in [
            "/hdhr/discover.json",
            "/hdhr/lineup.json",
            "/hdhr/lineup_status.json",
            "/hdhr/device.xml",
            "/api/hdhr/discover.json",
            "/api/hdhr/lineup.json",
            "/api/hdhr/lineup_status.json",
            "/api/hdhr/device.xml",
            "/hdhr/House/lineup.json",
            "/hdhr/output_profile/2/discover.json",
            "/hdhr/House/output_profile/2/lineup_status.json",
        ] {
            let route = endpoint(path).unwrap();
            for method in [Method::GET, Method::HEAD] {
                for format in [None, Some("api")] {
                    assert_eq!(
                        negotiation_status(&method, &route, format, browser),
                        None,
                        "{method} {path}"
                    );
                }
                assert_eq!(
                    negotiation_status(&method, &route, Some("api"), Some("application/json")),
                    Some(StatusCode::NOT_ACCEPTABLE),
                    "{method} {path}"
                );
                assert_eq!(
                    negotiation_status(&method, &route, Some("json"), Some("application/json")),
                    None,
                    "{method} {path}"
                );
            }
        }
    }

    #[test]
    fn explicit_json_excludes_browser_only_accept_and_matches_case_insensitive_media_types() {
        for path in [
            "/hdhr/discover.json",
            "/api/hdhr/device.xml",
            "/hdhr/House/lineup.json",
            "/hdhr/House/output_profile/2/lineup_status.json",
        ] {
            let route = endpoint(path).unwrap();
            for method in [Method::GET, Method::HEAD] {
                for (format, accept, expected) in [
                    (
                        Some("json"),
                        Some("text/html"),
                        Some(StatusCode::NOT_ACCEPTABLE),
                    ),
                    (
                        Some("json"),
                        Some("TEXT/*"),
                        Some(StatusCode::NOT_ACCEPTABLE),
                    ),
                    (
                        Some("json"),
                        Some("text/html, APPLICATION/JSON;q=0.5"),
                        None,
                    ),
                    (Some("json"), Some("APPLICATION/*"), None),
                    (Some("json"), Some("*/*"), None),
                    (Some("json"), None, None),
                    (None, Some("TEXT/HTML"), None),
                    (Some(""), Some("text/html"), None),
                    (Some("api"), Some("Text/HTML; charset=UTF-8"), None),
                    (
                        Some("api"),
                        Some("application/json"),
                        Some(StatusCode::NOT_ACCEPTABLE),
                    ),
                    (Some("xml"), Some("*/*"), Some(StatusCode::NOT_FOUND)),
                ] {
                    assert_eq!(
                        negotiation_status(&method, &route, format, accept),
                        expected,
                        "{method} {path} {format:?} {accept:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn lineup_status_options_preserves_the_source_wire_order() {
        let fields: Vec<_> = LINEUP_STATUS_OPTIONS
            .match_indices(r#"":"#)
            .map(|(offset, _)| {
                let prefix = &LINEUP_STATUS_OPTIONS[..offset];
                prefix.rsplit('"').next().unwrap()
            })
            .collect();
        assert_eq!(fields, ["name", "description", "renders", "parses"]);
        assert_eq!(LINEUP_STATUS_OPTIONS.len(), 221);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(LINEUP_STATUS_OPTIONS).unwrap(),
            serde_json::json!({
                "name": "Lineup Status Api",
                "description": "Returns the current status of the HDHR lineup",
                "renders": ["application/json", "text/html"],
                "parses": ["application/json", "application/x-www-form-urlencoded", "multipart/form-data"]
            })
        );
    }

    #[test]
    fn browser_format_preserves_other_route_and_method_admission() {
        for (method, path) in [
            (Method::GET, "/api/hdhr/devices/"),
            (Method::HEAD, "/hdhr/devices/"),
            (Method::GET, "/hdhr/"),
            (Method::OPTIONS, "/hdhr/lineup_status.json"),
            (Method::POST, "/hdhr/House/lineup.json"),
        ] {
            let route = endpoint(path).unwrap();
            assert_eq!(
                negotiation_status(&method, &route, Some("api"), Some("text/html")),
                Some(StatusCode::NOT_FOUND)
            );
        }
    }

    #[test]
    fn hdhr_channel_profile_path_keeps_literal_query_delimiters() {
        for (path, expected) in [
            ("/hdhr/Rock&Roll/discover.json", "Rock&Roll"),
            ("/hdhr/Rock%26Roll/discover.json", "Rock&Roll"),
            ("/hdhr/Sport+News&Music/lineup.json", "Sport+News&Music"),
            (
                "/hdhr/P%C3%BAblico+2&4/output_profile/003/discover.json",
                "Público+2&4",
            ),
        ] {
            let actual = match endpoint(path).unwrap() {
                Endpoint::Discover { profile, .. } | Endpoint::Lineup { profile, .. } => profile,
                _ => panic!("unexpected HDHR route"),
            };
            assert_eq!(actual.as_deref(), Some(expected), "{path}");
        }
    }

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
    #[test]
    fn escaped_hdhr_output_ids_use_the_same_route_and_canonical_digit_segment() {
        for path in [
            "/hdhr/output_profile/00042/discover.json",
            "/hdhr/output_profile/%30%300%34%32/discover.json",
        ] {
            let Endpoint::Discover { profile, output } = endpoint(path).unwrap() else {
                panic!("expected ordinary discovery");
            };
            assert_eq!(profile, None);
            assert_eq!(output.as_deref(), Some("42"));
        }
        let path = "/hdhr/Kids%20&%20News/output_profile/%3402/lineup.json";
        let Endpoint::Lineup { profile, output } = endpoint(path).unwrap() else {
            panic!("expected named lineup");
        };
        assert_eq!(profile.as_deref(), Some("Kids & News"));
        assert_eq!(output.as_deref(), Some("402"));
        assert!(matches!(
            endpoint("/hdhr/output_profile/%30%32/lineup_status.json"),
            Ok(Endpoint::Status)
        ));
        for output in ["%2531", "1%2F2", "%2B1", "%FF", "12a"] {
            let path = format!("/hdhr/output_profile/{output}/discover.json");
            assert!(matches!(endpoint(&path), Err(StoreError::NotFound)));
        }
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
