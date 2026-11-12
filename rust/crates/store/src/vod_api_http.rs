//! Bounded ordinary VOD HTTP reads derived from original Dispatcharr.
//! Original authors retain all application credit. Packaged/app gates are separate.
use super::vod_api_owned_read::HttpScope;
use super::{
    vod_api_owned_read::ReadError,
    vod_api_read_component::{HttpRead, ReadRequest, SqlReads},
    vod_api_sql::Resource,
};
use crate::StoreError;
use axum::{
    body::{to_bytes, Body},
    extract::ConnectInfo,
    http::{HeaderMap, HeaderValue, Method, Uri},
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use serde_json::{json, Value};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::Semaphore;
struct HttpFrame {
    data: bytes::Bytes,
    _slot: HttpScope,
}
impl AsRef<[u8]> for HttpFrame {
    fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
    }
}

/// Bounded queued input uses only the memory left after all configured
/// active read reservations. A waiting request has no auth/cache/SQL task.
#[derive(Clone)]
pub(super) struct HttpGate {
    pub(super) slots: Arc<Semaphore>,
    pub(super) waiters: Arc<Semaphore>,
    pub(super) input: Arc<Semaphore>,
    maximum_input: usize,
    deadline: Duration,
    runtime: Option<super::vod_api_owned_read::ReadRuntime>,
}
impl HttpGate {
    pub(super) fn new(limits: super::vod_api_owned_read::ReadLimits) -> Result<Self, StoreError> {
        let active = limits
            .work_bytes
            .checked_add(limits.response_bytes)
            .and_then(|v| v.checked_mul(limits.workers))
            .ok_or(StoreError::Unsupported("Invalid VOD HTTP read budget"))?;
        let queued = limits
            .memory_bytes
            .checked_sub(active)
            .filter(|bytes| *bytes >= limits.input_bytes)
            .ok_or(StoreError::Unsupported(
                "VOD HTTP memory must cover active and queued input",
            ))?
            .min(16 * 1024 * 1024);
        Ok(Self {
            slots: Arc::new(Semaphore::new(limits.workers)),
            waiters: Arc::new(Semaphore::new(256)),
            input: Arc::new(Semaphore::new(queued)),
            maximum_input: limits.input_bytes,
            deadline: limits.logical_deadline,
            runtime: None,
        })
    }
    pub(super) fn with_runtime(mut self, runtime: super::vod_api_owned_read::ReadRuntime) -> Self {
        if runtime.is_fair() {
            self.runtime = Some(runtime)
        }
        self
    }
    pub(super) async fn acquire(&self, bytes: usize) -> Option<(HttpScope, tokio::time::Instant)> {
        let deadline = tokio::time::Instant::now() + self.deadline;
        if let Some(runtime) = &self.runtime {
            return runtime
                .acquire_http(bytes, deadline)
                .await
                .ok()
                .map(|scope| (scope, deadline));
        }
        if bytes > self.maximum_input || self.slots.is_closed() {
            return None;
        }
        if let Ok(slot) = self.slots.clone().try_acquire_owned() {
            return Some((HttpScope::Legacy(Arc::new(slot)), deadline));
        }
        let _waiting = self.waiters.clone().try_acquire_owned().ok()?;
        let _input = self
            .input
            .clone()
            .try_acquire_many_owned(u32::try_from(bytes).ok()?)
            .ok()?;
        let slot = tokio::time::timeout_at(deadline, self.slots.clone().acquire_owned())
            .await
            .ok()?
            .ok()?;
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        Some((HttpScope::Legacy(Arc::new(slot)), deadline))
    }
}
/// Ordinary Movie/Series/Episode list/detail reads. Categories, unified reads,
/// provider refresh/actions and artwork are separate capabilities, not aliases.
#[derive(Clone)]
pub struct VodReads {
    pub(super) reader: SqlReads,
    auth: crate::AuthService,
    gate: HttpGate,
    image_routes: Arc<std::sync::atomic::AtomicBool>,
}
impl VodReads {
    pub fn new(
        auth: crate::AuthService,
        limits: super::vod_api_owned_read::ReadLimits,
    ) -> Result<Self, StoreError> {
        let reader = SqlReads::from_auth(auth.clone(), limits)
            .map_err(|_| StoreError::Unsupported("Invalid VOD read resource limits"))?;
        let gate = HttpGate::new(limits)?.with_runtime(reader.shared_runtime());
        Ok(Self {
            reader,
            auth,
            gate,
            image_routes: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }
    pub fn router(&self) -> Router {
        router_with_gate(self.reader.clone(), self.auth.clone(), self.gate.clone())
    }
    #[cfg(test)]
    pub(super) fn shared_gate(&self) -> HttpGate {
        self.gate.clone()
    }
    /// Private fixture scalar observer; no production route or reader access.
    #[cfg(test)]
    pub(super) fn scheduler_state(&self) -> serde_json::Value {
        self.reader.shared_runtime().scheduler_state()
    }
    /// Explicit category extension sharing this reader's active workers,
    /// byte budget, bounded waiting and shutdown. No second runtime is created.
    /// Advanced providers/unified/refresh endpoints are separate capabilities.
    pub fn category_router(&self) -> Router {
        super::vod_api_category_http::CategoryHttp::shared(
            self.auth.clone(),
            self.reader.shared_runtime(),
            self.gate.clone(),
        )
        .router()
    }
    /// Explicit unified JSON list/detail extension. Shares this SAME runtime,
    /// byte reservations, queued input, physical auth/SQL and shutdown; there
    /// is no second reader budget. Browsable HTML remains explicit501.
    pub fn unified_router(&self) -> Router {
        super::vod_api_unified_http::UnifiedHttp::shared(
            self.auth.clone(),
            self.reader.shared_runtime(),
            self.gate.clone(),
        )
        .router()
    }
    /// Source-derived image/cache router sharing this exact read runtime.
    /// Registers opt-in physical-image shutdown; app/browser gates are separate.
    pub fn vod_image_router(&self) -> Router {
        self.image_routes
            .store(true, std::sync::atomic::Ordering::Release);
        super::vod_api_images_http::ImageHttp::shared(
            self.auth.clone(),
            self.reader.shared_runtime(),
            self.gate.clone(),
        )
        .router()
    }
    /// Provider/episode actions share this exact runtime and admission queue.
    /// App attachment and packaged browser acceptance are separate gates.
    pub fn provider_router(&self) -> Router {
        super::vod_api_actions_http::ActionHttp::shared(
            self.auth.clone(),
            self.reader.shared_runtime(),
            self.gate.clone(),
        )
        .router()
    }
    /// Stops admission immediately; held body frames retain their own leases.
    pub async fn shutdown(&self, budget: std::time::Duration) -> bool {
        self.gate.slots.close();
        let (reads, images) = tokio::join!(self.reader.shutdown(budget), async {
            if self.image_routes.load(std::sync::atomic::Ordering::Acquire) {
                self.auth.shutdown_images(budget).await
            } else {
                true
            }
        });
        reads && images
    }
}
pub(crate) fn options(resource: Resource, detail: bool) -> Value {
    let (name, description) = match resource {
        Resource::Movie => ("Movie", "ViewSet for Movie content"),
        Resource::Series => ("Series", "ViewSet for Series management"),
        Resource::Episode => ("Episode", "ViewSet for Episode content"),
    };
    json!({"name":format!("{name} {}",if detail {"Instance"} else {"List"}),"description":description,
        "renders":["application/json","text/html"],"parses":["application/json","application/x-www-form-urlencoded","multipart/form-data"]})
}
pub(crate) fn negotiate(headers: &HeaderMap, uri: &Uri) -> Result<(), StoreError> {
    let format = url::form_urlencoded::parse(uri.query().unwrap_or("").as_bytes())
        .filter(|(key, _)| key == "format")
        .last()
        .map(|(_, value)| value.into_owned());
    if format
        .as_deref()
        .is_some_and(|format| !["json", "api"].contains(&format))
    {
        return Err(StoreError::NotFound);
    }
    if format.as_deref() == Some("api") {
        // Source filters renderers by format BEFORE matching Accept. The
        // existing category adapter already gates this JSON406 distinction.
        let accept = headers
            .get("accept")
            .map(|value| dispatcharr_contracts::source_header_value(value.as_bytes()))
            .unwrap_or_else(|| "*/*".into());
        if !accept.split(',').any(|value| {
            matches!(
                value.trim().split(';').next(),
                Some("*/*" | "text/*" | "text/html")
            )
        }) {
            return Err(StoreError::StatusBody(
                406,
                json!({"detail":"Could not satisfy the request Accept header."}),
            ));
        }
        return Err(StoreError::Unsupported(
            "VOD browsable HTML is not implemented",
        ));
    }
    let accept = headers
        .get("accept")
        .map(|v| dispatcharr_contracts::source_header_value(v.as_bytes()))
        .unwrap_or_else(|| "*/*".into());
    if !accept.split(',').any(|part| {
        matches!(
            part.trim().split(';').next(),
            Some("*/*" | "application/*" | "application/json")
        )
    }) {
        if accept
            .split(',')
            .any(|part| part.trim().split(';').next() == Some("text/html"))
        {
            return Err(StoreError::Unsupported(
                "VOD browsable HTML is not implemented",
            ));
        }
        return Err(StoreError::StatusBody(
            406,
            json!({"detail":"Could not satisfy the request Accept header."}),
        ));
    }
    Ok(())
}
/// Captured original JSONRenderer integer/clamp policy. Formatting below
/// scans already valid JSON and writes into the reserved bounded output. It
/// does not construct a second JSON value tree or retain per-node metadata.
pub(crate) fn json_indent(headers: &HeaderMap) -> Option<usize> {
    let accept = headers.get("accept")?;
    let raw = dispatcharr_contracts::source_header_value(accept.as_bytes());
    for part in raw.split(';').skip(1) {
        let (key, value) = part.trim().split_once('=')?;
        if key.eq_ignore_ascii_case("indent") {
            let number =
                dispatcharr_ingest_core::source_integer_literal(value.trim().trim_matches('"'))?;
            let selected = number
                .parse::<i64>()
                .map(|n| n.clamp(0, 8) as usize)
                .unwrap_or(if number.starts_with('-') { 0 } else { 8 });
            return (selected != 0).then_some(selected);
        }
    }
    None
}
pub(crate) fn pretty_document(
    input: &[u8],
    indent: usize,
    mut output: super::vod_api_owned_read::Document,
) -> Result<Vec<u8>, ReadError> {
    use std::io::Write;
    let mut depth = 0usize;
    let mut quoted = false;
    let mut escape = false;
    let write = |out: &mut super::vod_api_owned_read::Document, bytes: &[u8]| {
        out.write_all(bytes).map_err(|_| ReadError::Capacity)
    };
    let newline =
        |out: &mut super::vod_api_owned_read::Document, level: usize| -> Result<(), ReadError> {
            write(out, b"\n")?;
            for _ in 0..level {
                write(out, &b"        "[..indent])?;
            }
            Ok(())
        };
    for (index, &byte) in input.iter().enumerate() {
        if quoted {
            write(&mut output, &[byte])?;
            if escape {
                escape = false;
            } else if byte == b'\\' {
                escape = true;
            } else if byte == b'"' {
                quoted = false;
            }
            continue;
        }
        match byte {
            b'"' => {
                quoted = true;
                write(&mut output, &[byte])?;
            }
            b'{' | b'[' => {
                write(&mut output, &[byte])?;
                depth = depth.checked_add(1).ok_or(ReadError::Capacity)?;
                if !matches!(input.get(index + 1), Some(b'}' | b']')) {
                    newline(&mut output, depth)?;
                }
            }
            b'}' | b']' => {
                depth = depth.checked_sub(1).ok_or(ReadError::Capacity)?;
                if !matches!(
                    index.checked_sub(1).and_then(|i| input.get(i)),
                    Some(b'{' | b'[')
                ) {
                    newline(&mut output, depth)?;
                }
                write(&mut output, &[byte])?;
            }
            b',' => {
                write(&mut output, b",")?;
                newline(&mut output, depth)?;
            }
            b':' => write(&mut output, b": ")?,
            _ => write(&mut output, &[byte])?,
        }
    }
    if depth != 0 || quoted {
        return Err(ReadError::WorkerFailed);
    }
    Ok(output.finish())
}
fn resource_name(resource: Resource) -> &'static str {
    match resource {
        Resource::Movie => "movies",
        Resource::Series => "series",
        Resource::Episode => "episodes",
    }
}
pub(crate) fn router(reader: SqlReads, auth: crate::AuthService) -> Router {
    let gate = HttpGate::new(super::vod_api_owned_read::ReadLimits::default())
        .expect("default HTTP budget");
    router_with_gate(reader, auth, gate)
}
pub(crate) fn router_with_gate(
    reader: SqlReads,
    auth: crate::AuthService,
    gate: HttpGate,
) -> Router {
    let mut router = Router::new();
    for resource in [Resource::Movie, Resource::Series, Resource::Episode] {
        let root = format!("/api/vod/{}/", resource_name(resource));
        for (path, detail) in [(root.clone(), false), (format!("{root}{{id}}/"), true)] {
            let reader = reader.clone();
            let auth = auth.clone();
            let gate = gate.clone();
            router =
                router.route(
                    &path,
                    any(
                        move |ConnectInfo(peer): ConnectInfo<SocketAddr>,
                              request: axum::extract::Request| {
                            let reader = reader.clone();
                            let auth = auth.clone();
                            let gate = gate.clone();
                            async move {
                                respond(reader, auth, gate, peer, resource, detail, request).await
                            }
                        },
                    ),
                );
        }
    }
    router
}
pub(super) async fn respond(
    reader: SqlReads,
    auth: crate::AuthService,
    gate: HttpGate,
    peer: SocketAddr,
    resource: Resource,
    detail: bool,
    request: axum::extract::Request,
) -> Response {
    let (parts, body) = request.into_parts();
    drop(body);
    let input = parts
        .headers
        .iter()
        .fold(
            parts
                .uri
                .path_and_query()
                .map_or(0, |value| value.as_str().len())
                .checked_add(1024),
            |total, (key, value)| {
                total
                    .and_then(|n| n.checked_add(key.as_str().len()))
                    .and_then(|n| n.checked_add(value.as_bytes().len()))
            },
        )
        .and_then(|n| {
            parts
                .headers
                .capacity()
                .checked_mul(128)
                .and_then(|metadata| n.checked_add(metadata))
        });
    let acquired = match input {
        Some(input) => gate.acquire(input).await,
        None => None,
    };
    let (slot, deadline) = match acquired {
        Some(acquired) => acquired,
        None => {
            const DATA: &str = "{\"detail\":\"VOD HTTP capacity unavailable.\"}";
            let mut response = Response::new(if parts.method == Method::HEAD {
                Body::empty()
            } else {
                Body::from(DATA)
            });
            *response.status_mut() = axum::http::StatusCode::SERVICE_UNAVAILABLE;
            for (name, value) in [
                ("content-type", "application/json"),
                ("allow", "GET, HEAD, OPTIONS"),
                ("vary", "Accept, origin"),
            ] {
                response
                    .headers_mut()
                    .insert(name, HeaderValue::from_static(value));
            }
            response
                .headers_mut()
                .insert("content-length", DATA.len().into());
            return response;
        }
    };
    let head = parts.method == Method::HEAD;
    let selected_indent = json_indent(&parts.headers);
    // Only this peer's clean trailing-slash routes are registered. Suffix,
    // no-slash and other VOD families remain separate compatibility gates.
    let raw_detail = detail.then(|| {
        parts
            .uri
            .path()
            .trim_end_matches('/')
            .rsplit('/')
            .next()
            .unwrap_or("")
            .to_owned()
    });
    // Use the shared trusted-proxy origin builder. Presented credentials and
    // client-address ACL processing remain in actual scoped AuthService.
    let origin = crate::logos::origin(&auth, peer.ip(), &parts.headers);
    let input = ReadRequest {
        peer,
        headers: parts.headers,
        resource,
        detail: None,
        query: Vec::new(),
        origin,
        path: parts.uri.path().into(),
    };
    let result = reader
        .dispatch(
            input,
            Some(HttpRead {
                method: parts.method,
                uri: parts.uri,
                raw_detail,
                scope: Some(slot.clone()),
                deadline,
            }),
        )
        .await;
    let mut response = match result {
        Ok(data) => {
            let length = data.len();
            let mut r = Response::new(if head {
                Body::empty()
            } else {
                Body::from(bytes::Bytes::from_owner(HttpFrame {
                    data,
                    _slot: slot.clone(),
                }))
            });
            r.headers_mut()
                .insert("content-type", HeaderValue::from_static("application/json"));
            r.headers_mut().insert("content-length", length.into());
            r
        }
        Err(ReadError::Source(StoreError::UiPermissionFault)) => {
            return super::vod_api_category_http::source_server_error(slot, head)
        }
        Err(error) => {
            let source = match error {
                ReadError::Source(source) => source,
                ReadError::Unsupported(reason) => StoreError::Unsupported(reason),
                _ => {
                    StoreError::StatusBody(503, json!({"detail":"VOD read capacity unavailable."}))
                }
            };
            let response = source.into_response();
            let (parts, body) = response.into_parts();
            // Source errors here have bounded internal messages; consume only
            // this small error representation to retain correct HEAD length.
            let mut parts = parts;
            let data = match to_bytes(body, 64 * 1024).await {
                Ok(data) => match selected_indent {
                    Some(indent) => match pretty_document(
                        &data,
                        indent,
                        super::vod_api_owned_read::Document::bounded(64 * 1024),
                    ) {
                        Ok(data) => bytes::Bytes::from(data),
                        Err(_) => {
                            parts.status = axum::http::StatusCode::SERVICE_UNAVAILABLE;
                            bytes::Bytes::from_static(
                                b"{\"detail\":\"VOD error representation exceeds capacity.\"}",
                            )
                        }
                    },
                    None => data,
                },
                Err(_) => {
                    parts.status = axum::http::StatusCode::SERVICE_UNAVAILABLE;
                    bytes::Bytes::from_static(
                        b"{\"detail\":\"VOD error representation exceeds capacity.\"}",
                    )
                }
            };
            let length = data.len();
            let mut r = Response::from_parts(
                parts,
                if head {
                    Body::empty()
                } else {
                    Body::from(bytes::Bytes::from_owner(HttpFrame { data, _slot: slot }))
                },
            );
            r.headers_mut().insert("content-length", length.into());
            r
        }
    };
    response
        .headers_mut()
        .insert("allow", HeaderValue::from_static("GET, HEAD, OPTIONS"));
    response
        .headers_mut()
        .insert("vary", HeaderValue::from_static("Accept, origin"));
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn scheduler_observer_tracks_this_http_runtime_and_held_scope() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://fixture:fixture@127.0.0.1:1/fixture")
            .unwrap();
        let auth = crate::AuthService::new(
            crate::Store::from_pool(pool),
            crate::AuthConfig::new("fixture-only".into()).unwrap(),
        );
        let service = VodReads::new(
            auth,
            super::super::vod_api_owned_read::ReadLimits::default(),
        )
        .unwrap();
        let before = service.scheduler_state();
        assert_eq!(before["inputs_used"], 0);
        assert!(before
            .as_object()
            .unwrap()
            .values()
            .all(|value| value.is_boolean() || value.is_u64()));
        let (scope, _) = service.shared_gate().acquire(7).await.unwrap();
        assert_eq!(service.scheduler_state()["inputs_used"], 1);
        assert!(!service.shutdown(Duration::from_millis(10)).await);
        assert_eq!(service.scheduler_state()["admission_closed"], true);
        drop(scope);
        assert!(service.shutdown(Duration::from_secs(1)).await);
        assert_eq!(service.scheduler_state()["inputs_used"], 0);
        assert_eq!(service.scheduler_state()["bytes"], 0);
    }
    #[test]
    fn bounded_formatter_preserves_nested_string_syntax_and_capacity() {
        let value = json!({"quote":"[\"{},]","nested":[{},[],{"x":1}],"empty":{}});
        let compact = serde_json::to_vec(&value).unwrap();
        let mut expected = Vec::new();
        let formatter = serde_json::ser::PrettyFormatter::with_indent(b"    ");
        let mut serializer = serde_json::Serializer::with_formatter(&mut expected, formatter);
        serde::Serialize::serialize(&value, &mut serializer).unwrap();
        assert_eq!(
            pretty_document(
                &compact,
                4,
                super::super::vod_api_owned_read::Document::bounded(4096)
            )
            .unwrap(),
            expected
        );
        assert!(matches!(
            pretty_document(
                &compact,
                4,
                super::super::vod_api_owned_read::Document::bounded(7)
            ),
            Err(ReadError::Capacity)
        ));
    }
}
