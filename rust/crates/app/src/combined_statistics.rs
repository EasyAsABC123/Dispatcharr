//! Original combined live/VOD/catch-up admin statistics, with owned bounded I/O.
//! Original Dispatcharr authors retain full application and algorithm credit.
use super::{combined_statistics_html as html, combined_statistics_wire as wire};
use axum::{
    body::{to_bytes, Body, Bytes},
    extract::{ConnectInfo, State},
    http::{HeaderMap, HeaderValue, Method, Request, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use dispatcharr_output_store::transport::{RedisTransport, TransportConfig};
use dispatcharr_store::{AuthService, StoreError};
use dispatcharr_stream_http::StreamEngine;
use dispatcharr_timeshift_http::CatchupManagement;
use serde_json::{json, Value};
use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};

const MIB: usize = 1024 * 1024;
#[derive(Clone, Copy)]
pub struct Config {
    pub workers: usize,
    pub memory_bytes: usize,
    pub work_bytes: usize,
    pub response_bytes: usize,
    pub input_bytes: usize,
    pub observer_deadline: Duration,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            workers: 2,
            memory_bytes: 192 * MIB,
            work_bytes: 64 * MIB,
            response_bytes: 24 * MIB,
            input_bytes: 64 * 1024,
            observer_deadline: Duration::from_secs(5),
        }
    }
}
struct Inner {
    auth: AuthService,
    store: dispatcharr_store::Store,
    live: StreamEngine,
    catchup: Option<CatchupManagement>,
    transport: Option<RedisTransport>,
    slots: Arc<Semaphore>,
    memory: Arc<Semaphore>,
    config: Config,
    reservation: usize,
}
#[derive(Clone)]
pub struct CombinedStatistics {
    inner: Arc<Inner>,
}
struct Ownership {
    _slot: OwnedSemaphorePermit,
    _memory: OwnedSemaphorePermit,
}
struct Frame {
    data: Vec<u8>,
    _owner: Arc<Ownership>,
}
impl AsRef<[u8]> for Frame {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}
fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}
fn config_reservation(config: Config) -> Result<usize, ()> {
    let reservation = config
        .work_bytes
        .checked_add(config.response_bytes)
        .and_then(|n| n.checked_add(config.input_bytes))
        .ok_or(())?;
    if config.workers == 0
        || config.workers > 16
        || config.input_bytes == 0
        || config.input_bytes > 64 * 1024
        || config.work_bytes < 64 * MIB
        || config.response_bytes == 0
        || config.response_bytes > 24 * MIB
        || config.memory_bytes > u32::MAX as usize
        || config.observer_deadline.is_zero()
        || reservation
            .checked_mul(config.workers)
            .is_none_or(|n| n > config.memory_bytes)
    {
        return Err(());
    }
    Ok(reservation)
}
impl CombinedStatistics {
    pub async fn connect(
        auth: AuthService,
        store: dispatcharr_store::Store,
        live: StreamEngine,
        catchup: Option<CatchupManagement>,
        redis_url: Option<&str>,
        config: Config,
    ) -> Result<Self, StoreError> {
        let reservation = config_reservation(config)
            .map_err(|_| StoreError::Unsupported("Invalid combined statistics limits"))?;
        let transport = match redis_url {
            Some(url) => Some(
                RedisTransport::connect(
                    url,
                    TransportConfig {
                        max_operations: config.workers,
                        max_queued_operations: config.workers,
                        command_timeout: Duration::from_secs(3),
                    },
                )
                .await
                .map_err(|_| StoreError::Unsupported("Combined statistics Redis unavailable"))?,
            ),
            None => None,
        };
        Ok(Self {
            inner: Arc::new(Inner {
                auth,
                store,
                live,
                catchup,
                transport,
                config,
                reservation,
                slots: Arc::new(Semaphore::new(config.workers)),
                memory: Arc::new(Semaphore::new(config.memory_bytes)),
            }),
        })
    }
    pub fn router(&self) -> Router {
        Router::new()
            .route("/proxy/stats/", any(dispatch))
            .with_state(self.clone())
    }
    pub fn close_admission(&self) {
        self.inner.slots.close();
    }
    pub async fn shutdown(&self, budget: Duration) -> bool {
        self.close_admission();
        tokio::time::timeout(budget, async {
            while self.inner.slots.available_permits() != self.inner.config.workers
                || self.inner.memory.available_permits() != self.inner.config.memory_bytes
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .is_ok()
    }
    fn acquire(&self) -> Option<Arc<Ownership>> {
        let slot = self.inner.slots.clone().try_acquire_owned().ok()?;
        let memory = self
            .inner
            .memory
            .clone()
            .try_acquire_many_owned(u32::try_from(self.inner.reservation).ok()?)
            .ok()?;
        Some(Arc::new(Ownership {
            _slot: slot,
            _memory: memory,
        }))
    }
    async fn snapshot(&self, owner: Arc<Ownership>) -> Result<Value, ()> {
        let (Some(transport), Some(catchup)) = (&self.inner.transport, &self.inner.catchup) else {
            return Err(());
        };
        let guard: Arc<dyn Send + Sync> = owner.clone();
        // Preserve Source's sequential observations and separate per-family clocks.
        let live = self
            .inner
            .live
            .global_live_statistics_retained(guard)
            .await
            .unwrap_or_else(|_| json!({"channels":[],"count":0}));
        let vod = dispatcharr_vod_http::source_statistics_retained(
            &self.inner.store,
            transport,
            now(),
            owner.clone(),
        )
        .await;
        let catchup = catchup.statistics_retained(owner).await;
        Ok(json!({"live":live,"vod":vod,"catchup":catchup,"timestamp":now()}))
    }
    async fn process(
        &self,
        peer: SocketAddr,
        method: Method,
        uri: Uri,
        headers: HeaderMap,
        owner: Arc<Ownership>,
    ) -> Response {
        let negotiated = match negotiate(&uri, &headers) {
            Ok(value) => value,
            Err((status, body)) => {
                return finish(
                    status,
                    &body,
                    &method,
                    None,
                    None,
                    &uri,
                    &headers,
                    &self.inner.auth,
                    peer,
                    owner,
                    self.inner.config.response_bytes,
                )
                .await;
            }
        };
        let guard: Arc<dyn Send + Sync> = owner.clone();
        let user = self
            .inner
            .auth
            .clone()
            .with_retained_scope(guard)
            .account_user(peer, headers.clone(), false)
            .await;
        let user = match user {
            Ok(user) => user,
            Err(StoreError::UiPermissionFault) => {
                let (parts, body) = StoreError::UiPermissionFault.into_response().into_parts();
                let data = match to_bytes(body, 1024).await {
                    Ok(data) => data.to_vec(),
                    Err(_) => return capacity_owned(owner),
                };
                let data = Bytes::from_owner(Frame {
                    data,
                    _owner: owner,
                });
                return security(Response::from_parts(parts, Body::from(data)));
            }
            Err(error) => {
                let response = error.into_response();
                let status = response.status();
                let authenticate = response.headers().get("www-authenticate").cloned();
                let body = match to_bytes(response.into_body(), 64 * 1024)
                    .await
                    .ok()
                    .and_then(|raw| serde_json::from_slice::<Value>(&raw).ok())
                {
                    Some(body) => body,
                    None => return capacity_owned(owner),
                };
                let mut response = finish(
                    status,
                    &body,
                    &method,
                    None,
                    Some(negotiated),
                    &uri,
                    &headers,
                    &self.inner.auth,
                    peer,
                    owner,
                    self.inner.config.response_bytes,
                )
                .await;
                if let Some(value) = authenticate {
                    response.headers_mut().insert("www-authenticate", value);
                }
                return response;
            }
        };
        let identity = Some((user.username.as_str(), user.user_level));
        if user.user_level < 10 {
            return finish(
                StatusCode::FORBIDDEN,
                &json!({"detail":"You do not have permission to perform this action."}),
                &method,
                identity,
                Some(negotiated),
                &uri,
                &headers,
                &self.inner.auth,
                peer,
                owner,
                self.inner.config.response_bytes,
            )
            .await;
        }
        if method == Method::OPTIONS {
            return finish(StatusCode::OK,&json!({"name":"Combined Stats","description":"Return live, VOD, and catch-up stats in one response.",
                "renders":["application/json","text/html"],"parses":["application/json","application/x-www-form-urlencoded","multipart/form-data"]}),
                &method,identity,Some(negotiated),&uri,&headers,&self.inner.auth,peer,owner,self.inner.config.response_bytes).await;
        }
        if method != Method::GET {
            return finish(
                StatusCode::METHOD_NOT_ALLOWED,
                &json!({"detail":format!("Method \"{method}\" not allowed.")}),
                &method,
                identity,
                Some(negotiated),
                &uri,
                &headers,
                &self.inner.auth,
                peer,
                owner,
                self.inner.config.response_bytes,
            )
            .await;
        }
        drop(user);
        let (status, value) = match self.snapshot(owner.clone()).await {
            Ok(value) => (StatusCode::OK, value),
            Err(()) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({"error":"Redis not available"}),
            ),
        };
        // A real Django JsonResponse bypasses DRF HTML/indentation for GET.
        let data = match wire::json(
            &value,
            wire::Format::Django,
            self.inner.config.response_bytes,
        ) {
            Ok(data) => data,
            Err(()) => return capacity_owned(owner),
        };
        reply(status, data, "application/json", None, owner)
    }
}
#[derive(Clone, Copy)]
struct Negotiated {
    html: bool,
    indent: Option<usize>,
}
fn negotiate(uri: &Uri, headers: &HeaderMap) -> Result<Negotiated, (StatusCode, Value)> {
    let format = url::form_urlencoded::parse(uri.query().unwrap_or("").as_bytes())
        .filter(|(k, _)| k == "format")
        .last()
        .map(|(_, v)| v.into_owned());
    if format
        .as_deref()
        .is_some_and(|v| !matches!(v, "json" | "api"))
    {
        return Err((StatusCode::NOT_FOUND, json!({"detail":"Not found."})));
    }
    let accept = headers
        .get("accept")
        .map(|value| dispatcharr_contracts::source_header_value(value.as_bytes()))
        .unwrap_or_else(|| "*/*".into());
    let types: Vec<_> = accept
        .split(',')
        .map(|v| v.trim().split(';').next().unwrap_or(""))
        .collect();
    let json = types
        .iter()
        .any(|v| matches!(*v, "*/*" | "application/*" | "application/json"));
    let html = types
        .iter()
        .any(|v| matches!(*v, "*/*" | "text/*" | "text/html"));
    let selected = match format.as_deref() {
        Some("json") if json => false,
        Some("api") if html => true,
        None if json => false,
        None if html => true,
        _ => {
            return Err((
                StatusCode::NOT_ACCEPTABLE,
                json!({"detail":"Could not satisfy the request Accept header."}),
            ))
        }
    };
    let indent = accept.split(';').skip(1).find_map(|part| {
        let (k, v) = part.trim().split_once('=')?;
        if !k.eq_ignore_ascii_case("indent") {
            return None;
        }
        v.trim()
            .trim_matches('"')
            .parse::<i64>()
            .ok()
            .map(|n| n.clamp(0, 8) as usize)
            .filter(|n| *n > 0)
    });
    Ok(Negotiated {
        html: selected,
        indent,
    })
}
fn security(mut response: Response) -> Response {
    for (name, value) in [
        ("x-content-type-options", "nosniff"),
        ("x-frame-options", "DENY"),
        ("referrer-policy", "same-origin"),
        ("cross-origin-opener-policy", "same-origin"),
    ] {
        response
            .headers_mut()
            .insert(name, HeaderValue::from_static(value));
    }
    response
}
const CAPACITY_BODY: &str = "{\"detail\":\"Combined statistics capacity unavailable.\"}";
fn capacity() -> Response {
    security(
        (
            StatusCode::SERVICE_UNAVAILABLE,
            [
                ("content-type", "application/json"),
                ("allow", "GET, OPTIONS"),
                ("vary", "Accept, origin"),
            ],
            CAPACITY_BODY,
        )
            .into_response(),
    )
}
fn capacity_owned(owner: Arc<Ownership>) -> Response {
    reply(
        StatusCode::SERVICE_UNAVAILABLE,
        CAPACITY_BODY.as_bytes().to_vec(),
        "application/json",
        None,
        owner,
    )
}
fn reply(
    status: StatusCode,
    data: Vec<u8>,
    content_type: &'static str,
    set_cookie: Option<String>,
    owner: Arc<Ownership>,
) -> Response {
    let length = data.len();
    let bytes = Bytes::from_owner(Frame {
        data,
        _owner: owner,
    });
    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = status;
    for (name, value) in [
        ("content-type", content_type),
        ("allow", "GET, OPTIONS"),
        ("vary", "Accept, origin"),
    ] {
        response
            .headers_mut()
            .insert(name, HeaderValue::from_static(value));
    }
    response.headers_mut().insert(
        "content-length",
        HeaderValue::from_str(&length.to_string()).expect("bounded length"),
    );
    if let Some(cookie) = set_cookie {
        if let Ok(value) = HeaderValue::from_str(&cookie) {
            response.headers_mut().insert("set-cookie", value);
            response
                .headers_mut()
                .insert("vary", HeaderValue::from_static("Accept, origin, Cookie"));
        }
    }
    security(response)
}
#[allow(clippy::too_many_arguments)]
async fn finish(
    status: StatusCode,
    value: &Value,
    method: &Method,
    identity: Option<(&str, i32)>,
    negotiated: Option<Negotiated>,
    uri: &Uri,
    headers: &HeaderMap,
    auth: &AuthService,
    peer: SocketAddr,
    owner: Arc<Ownership>,
    maximum: usize,
) -> Response {
    let selected = negotiated.unwrap_or(Negotiated {
        html: false,
        indent: None,
    });
    let result = if selected.html {
        let cookie = headers
            .get("cookie")
            .and_then(|v| v.to_str().ok())
            .and_then(|raw| {
                raw.split(';').find_map(|v| {
                    let (k, v) = v.trim().split_once('=')?;
                    (k == "csrftoken").then_some(v)
                })
            });
        let request = dispatcharr_contracts::PlaybackRequest {
            channel_id: String::new(),
            query: Vec::new(),
            peer_ip: peer.ip(),
            headers: headers
                .iter()
                .map(|(name, value)| {
                    (
                        name.as_str().to_owned(),
                        dispatcharr_contracts::source_header_value(value.as_bytes()),
                    )
                })
                .collect(),
        };
        let origin = dispatcharr_output_http::source_output_origin(auth, &request);
        html::render(
            html::Context {
                status: status.as_u16(),
                method: method.as_str(),
                uri: &uri.to_string(),
                identity,
                cookie,
                origin: &origin,
            },
            value,
        )
        .map(|rendered| {
            (
                rendered.data,
                "text/html; charset=utf-8",
                Some(rendered.set_cookie),
            )
        })
    } else {
        wire::json(value, wire::Format::Drf(selected.indent), maximum)
            .map(|data| (data, "application/json", None))
    };
    let mut response = match result {
        Ok((data, kind, cookie)) => reply(status, data, kind, cookie, owner),
        Err(()) => {
            return reply(
                StatusCode::NOT_IMPLEMENTED,
                b"{\"error\":\"Combined statistics HTML context exceeds captured scope\"}".to_vec(),
                "application/json",
                None,
                owner,
            )
        }
    };
    if method == Method::HEAD {
        *response.body_mut() = Body::empty();
    }
    response
}
async fn dispatch(State(service): State<CombinedStatistics>, request: Request<Body>) -> Response {
    let Some(peer) = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|v| v.0)
    else {
        return capacity();
    };
    let (parts, body) = request.into_parts();
    drop(body);
    let input = parts.headers.iter().fold(
        parts.uri.to_string().len().checked_add(1024),
        |n, (key, value)| {
            n.and_then(|n| n.checked_add(key.as_str().len()))
                .and_then(|n| n.checked_add(value.as_bytes().len()))
        },
    );
    if input.is_none_or(|n| n > service.inner.config.input_bytes) {
        return capacity();
    }
    let Some(owner) = service.acquire() else {
        return capacity();
    };
    let (tx, rx) = oneshot::channel();
    let task = service.clone();
    tokio::spawn(async move {
        let response = task
            .process(peer, parts.method, parts.uri, parts.headers, owner)
            .await;
        let _ = tx.send(response);
    });
    match tokio::time::timeout(service.inner.config.observer_deadline, rx).await {
        Ok(Ok(response)) => response,
        _ => capacity(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn limits_reject_overcommit_and_preserve_default_reservation() {
        let config = Config::default();
        let reservation = config_reservation(config).unwrap();
        assert_eq!(reservation, 88 * MIB + 64 * 1024);
        assert!(reservation * config.workers <= config.memory_bytes);
        assert!(config_reservation(Config {
            memory_bytes: reservation,
            ..config
        })
        .is_err());
        assert!(config_reservation(Config {
            workers: 0,
            ..config
        })
        .is_err());
        assert!(config_reservation(Config {
            input_bytes: 64 * 1024 + 1,
            ..config
        })
        .is_err());
        assert!(config_reservation(Config {
            response_bytes: 24 * MIB + 1,
            ..config
        })
        .is_err());
    }
    #[test]
    fn negotiation_is_before_auth_and_separates_html_from_success_json() {
        let uri = "/proxy/stats/".parse().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("accept", HeaderValue::from_static("text/html"));
        assert!(negotiate(&uri, &headers).unwrap().html);
        headers.insert(
            "accept",
            HeaderValue::from_static("application/json; indent=4"),
        );
        assert_eq!(negotiate(&uri, &headers).unwrap().indent, Some(4));
        assert_eq!(
            negotiate(&"/proxy/stats/?format=invalid".parse().unwrap(), &headers)
                .err()
                .unwrap()
                .0,
            StatusCode::NOT_FOUND
        );
        headers.insert("accept", HeaderValue::from_static("image/png"));
        assert_eq!(
            negotiate(&uri, &headers).err().unwrap().0,
            StatusCode::NOT_ACCEPTABLE
        );
    }
    #[tokio::test]
    async fn held_http_bytes_and_physical_clone_share_admission_until_both_drop() {
        let config = Config::default();
        let reservation = config_reservation(config).unwrap();
        let slots = Arc::new(Semaphore::new(1));
        let memory = Arc::new(Semaphore::new(config.memory_bytes));
        let owner = Arc::new(Ownership {
            _slot: slots.clone().acquire_owned().await.unwrap(),
            _memory: memory
                .clone()
                .acquire_many_owned(reservation as u32)
                .await
                .unwrap(),
        });
        let physical = owner.clone();
        let response = reply(
            StatusCode::OK,
            vec![1; 1024],
            "application/json",
            None,
            owner,
        );
        assert_eq!(slots.available_permits(), 0);
        let payload = to_bytes(response.into_body(), 2048).await.unwrap();
        assert_eq!(slots.available_permits(), 0);
        assert_eq!(
            memory.available_permits(),
            config.memory_bytes - reservation
        );
        drop(payload);
        assert_eq!(slots.available_permits(), 0);
        drop(physical);
        assert_eq!(slots.available_permits(), 1);
        assert_eq!(memory.available_permits(), config.memory_bytes);
    }
}

#[cfg(test)]
#[path = "combined_statistics_peer.rs"]
mod physical_peer;
