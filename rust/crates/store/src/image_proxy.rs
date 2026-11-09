//! Native core.image_proxy contracts from original Dispatcharr contributors.
//! All original application credit remains in ATTRIBUTION.md.
use crate::{AuthService, StoreError};
use axum::{
    body::Body,
    extract::{ConnectInfo, OriginalUri, Path, State},
    http::{HeaderMap, HeaderValue},
    response::{IntoResponse, Response},
};
use base64::Engine;
use bytes::Bytes;
use futures_util::StreamExt;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::{Duration, Instant},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
/// Preserve explicit original raw-image MIME through generic app defaults.
#[derive(Clone, Debug)]
pub struct SourceImageResponse;
const MAX_BYTES: usize = 5 * 1024 * 1024;
const FAIL_TTL: Duration = Duration::from_secs(300);
const CSP: &str = "default-src 'none'; style-src 'unsafe-inline'; sandbox";
#[derive(Clone)]
pub(crate) struct ImageRuntime {
    slots: Arc<Semaphore>,
    waiting: Arc<Semaphore>,
    memory: Arc<AtomicUsize>,
    failures: Arc<Mutex<HashMap<String, Instant>>>,
}
impl ImageRuntime {
    pub(crate) fn new() -> Self {
        Self {
            slots: Arc::new(Semaphore::new(4)),
            waiting: Arc::new(Semaphore::new(64)),
            memory: Arc::new(AtomicUsize::new(0)),
            failures: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    pub(crate) async fn reserve(&self) -> Result<OwnedSemaphorePermit, StoreError> {
        let busy = || StoreError::StatusBody(503, json!({"detail":"Image service busy."}));
        if let Ok(permit) = self.slots.clone().try_acquire_owned() {
            return Ok(permit);
        }
        let _waiting = self
            .waiting
            .clone()
            .try_acquire_owned()
            .map_err(|_| busy())?;
        tokio::time::timeout(Duration::from_secs(15), self.slots.clone().acquire_owned())
            .await
            .map_err(|_| busy())?
            .map_err(|_| busy())
    }

    fn resources(&self) -> Value {
        json!({"admitted":4-self.slots.available_permits(),"waiting":64-self.waiting.available_permits(),"max_admitted":4,"max_waiting":64,"retained_response_bytes":self.memory.load(Ordering::Relaxed),"negative_entries":self.failures.lock().map(|v|v.len()).unwrap_or(0)})
    }
    fn remember(&self, url: &str) {
        let Ok(mut cache) = self.failures.lock() else {
            return;
        };
        let now = Instant::now();
        if cache.len() >= 256 {
            cache.retain(|_, expiry| *expiry > now);
        }
        if cache.len() >= 1024 {
            if let Some(key) = cache
                .iter()
                .min_by_key(|(_, expiry)| **expiry)
                .map(|(key, _)| key.clone())
            {
                cache.remove(&key);
            }
        }
        if url.len() <= 16 * 1024 {
            cache.insert(url.into(), now + FAIL_TTL);
        }
    }
    fn failed(&self, url: &str) -> bool {
        self.failures.lock().is_ok_and(|cache| {
            cache
                .get(url)
                .is_some_and(|expiry| *expiry > Instant::now())
        })
    }
    fn forget(&self, url: &str) {
        if let Ok(mut cache) = self.failures.lock() {
            cache.remove(url);
        }
    }
}
impl AuthService {
    /// Read-only native image admission/retained-response diagnostics. Counts
    /// include response frames still owned by HTTP transport after handler exit.
    /// Original CoreSettings.get_default_user_agent shared string/version cache.
    pub async fn source_default_user_agent(&self) -> Result<String, StoreError> {
        user_agent(self).await
    }
    pub fn image_resources(&self) -> Value {
        self.images.resources()
    }
    /// Source core.image_proxy response. The route remains responsible for
    /// authentication and selecting the authorized image URL.
    pub async fn image_response(&self, url: &str) -> Result<Response, StoreError> {
        if url.is_empty() {
            return Err(missing("Image not found"));
        }
        let permit = self.images.reserve().await?;
        if url.starts_with("/data") {
            return local(url.to_owned(), permit, self.images.memory.clone()).await;
        }
        if !url.starts_with("http://") && !url.starts_with("https://") {
            return Err(missing("Image not found"));
        }
        remote(self, url, permit).await
    }
}
fn missing(message: &str) -> StoreError {
    StoreError::StatusBody(404, json!({"detail":message}))
}
/// Source allow_private=True policy, including Python3.13 mapped-IP behavior.
pub(crate) fn address_allowed(address: IpAddr) -> bool {
    static NETWORKS: OnceLock<Vec<ipnet::IpNet>> = OnceLock::new();
    let networks = NETWORKS.get_or_init(|| {
        let data: Value =
            serde_json::from_str(include_str!("../datasets/source-image-ip-policy.json"))
                .expect("source image IP policy");
        ["4", "6"]
            .iter()
            .flat_map(|key| {
                data["allowed"][*key]
                    .as_array()
                    .expect("network array")
                    .iter()
            })
            .map(|v| v.as_str().expect("CIDR").parse().expect("source CIDR"))
            .collect()
    });
    networks.iter().any(|network| network.contains(&address))
}
async fn validate_url(url: &str) -> Result<(), StoreError> {
    let url = reqwest::Url::parse(url).map_err(|_| missing("Remote image not allowed"))?;
    if !["http", "https"].contains(&url.scheme()) {
        return Err(missing("Remote image not allowed"));
    }
    let host = url
        .host_str()
        .ok_or_else(|| missing("Remote image not allowed"))?;
    let addresses = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::net::lookup_host((
            host.trim_matches(['[', ']']),
            url.port_or_known_default().unwrap_or(80),
        )),
    )
    .await
    .map_err(|_| missing("Remote image not allowed"))?
    .map_err(|_| missing("Remote image not allowed"))?
    .collect::<Vec<_>>();
    if addresses.is_empty() || addresses.iter().any(|a| !address_allowed(a.ip())) {
        return Err(missing("Remote image not allowed"));
    }
    Ok(())
}
pub(crate) fn append_bounded(bytes: &mut Vec<u8>, frame: &[u8], maximum: usize) {
    let required = bytes.len() + frame.len();
    debug_assert!(required <= maximum);
    if required > bytes.capacity() {
        let capacity = bytes
            .capacity()
            .saturating_mul(2)
            .max(required)
            .min(maximum);
        bytes.reserve_exact(capacity - bytes.len());
    }
    bytes.extend_from_slice(frame);
}
pub(crate) fn sniff(body: &[u8]) -> Option<&'static str> {
    if body.starts_with(b"\xff\xd8\xff") {
        return Some("image/jpeg");
    }
    if body.starts_with(b"\x89PNG\r\n\x1a\n") {
        return Some("image/png");
    }
    if body.starts_with(b"GIF87a") || body.starts_with(b"GIF89a") {
        return Some("image/gif");
    }
    if body.len() >= 12 && body.starts_with(b"RIFF") && &body[8..12] == b"WEBP" {
        return Some("image/webp");
    }
    if body.starts_with(b"\0\0\x01\0") || body.starts_with(b"\0\0\x02\0") {
        return Some("image/x-icon");
    }
    if body.len() >= 6 && body.starts_with(b"BM") {
        return Some("image/bmp");
    }
    let strip = |bytes: &[u8]| {
        bytes
            .iter()
            .position(|v| !b" \t\n\r\x0b\x0c".contains(v))
            .unwrap_or(bytes.len())
    };
    let sample = &body[..body.len().min(8192)];
    let sample = &sample[strip(sample)..];
    let sample = sample.strip_prefix(b"\xef\xbb\xbf").unwrap_or(sample);
    let sample = &sample[strip(sample)..];
    let lower = sample.to_ascii_lowercase();
    let head = &lower[..lower.len().min(64)];
    if [
        b"<!doctype html".as_slice(),
        b"<html",
        b"<head",
        b"<body",
        b"<script",
    ]
    .iter()
    .any(|p| head.starts_with(p))
        || lower.windows(5).any(|p| p == b"<html")
    {
        return None;
    }
    if lower.windows(4).any(|p| p == b"<svg") {
        Some("image/svg+xml")
    } else {
        None
    }
}
fn security(response: &mut Response, content_type: &str) {
    response.headers_mut().insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    if content_type.starts_with("image/svg") {
        response
            .headers_mut()
            .insert("content-security-policy", HeaderValue::from_static(CSP));
    }
}
fn header(response: &mut Response, key: &'static str, value: &str) -> Result<(), StoreError> {
    if value.contains(['\r', '\n']) {
        return Err(StoreError::StatusBody(
            500,
            json!({"detail":"Invalid image response header."}),
        ));
    }
    let latin = value
        .chars()
        .map(|c| u8::try_from(c as u32))
        .collect::<Result<Vec<_>, _>>();
    let bytes = match latin {
        Ok(v) => v,
        Err(_) => {
            let q = value
                .as_bytes()
                .iter()
                .map(|byte| match byte {
                    b' ' => "_".into(),
                    b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'!' | b'*' | b'+' | b'-' | b'/' => {
                        char::from(*byte).to_string()
                    }
                    v => format!("={v:02X}"),
                })
                .collect::<String>();
            let b = base64::engine::general_purpose::STANDARD.encode(value.as_bytes());
            if b.len() < q.len() {
                format!("=?utf-8?b?{b}?=").into_bytes()
            } else {
                format!("=?utf-8?q?{q}?=").into_bytes()
            }
        }
    };
    response.headers_mut().insert(
        key,
        HeaderValue::from_bytes(&bytes).map_err(|_| {
            StoreError::StatusBody(500, json!({"detail":"Invalid image response header."}))
        })?,
    );
    Ok(())
}

struct Retained {
    bytes: Vec<u8>,
    _permit: Arc<OwnedSemaphorePermit>,
    memory: Arc<AtomicUsize>,
}
impl Retained {
    fn new(bytes: Vec<u8>, permit: Arc<OwnedSemaphorePermit>, memory: Arc<AtomicUsize>) -> Self {
        memory.fetch_add(bytes.capacity(), Ordering::Relaxed);
        Self {
            bytes,
            _permit: permit,
            memory,
        }
    }
}
impl Drop for Retained {
    fn drop(&mut self) {
        self.memory
            .fetch_sub(self.bytes.capacity(), Ordering::Relaxed);
    }
}
impl AsRef<[u8]> for Retained {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
fn local_error() -> Response {
    let mut response = axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response();
    response.headers_mut().insert(
        "content-type",
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    response
}
async fn local(
    url: String,
    permit: OwnedSemaphorePermit,
    memory: Arc<AtomicUsize>,
) -> Result<Response, StoreError> {
    let safe = tokio::task::spawn_blocking(move || {
        let root = std::fs::canonicalize("/data/logos").ok()?;
        let path = std::fs::canonicalize(url).ok()?;
        path.starts_with(root).then_some(path)
    })
    .await
    .ok()
    .flatten()
    .ok_or_else(|| missing("Image not found"))?;
    let file = match tokio::fs::File::open(&safe).await {
        Ok(v) => v,
        Err(_) => return Ok(local_error()),
    };
    let metadata = match file.metadata().await {
        Ok(v) if !v.is_dir() => v,
        _ => return Ok(local_error()),
    };
    let mime = mime_guess::from_path(&safe)
        .first_raw()
        .unwrap_or("image/jpeg");
    let permit = Arc::new(permit);
    let stream = tokio_util::io::ReaderStream::with_capacity(file, 8192).map(move |frame| {
        frame.map(|bytes| {
            Bytes::from_owner(Retained::new(
                bytes.to_vec(),
                permit.clone(),
                memory.clone(),
            ))
        })
    });
    let mut response = Body::from_stream(stream).into_response();
    response.extensions_mut().insert(SourceImageResponse);
    header(&mut response, "content-type", mime)?;
    header(&mut response, "cache-control", "public, max-age=14400")?;
    if let Ok(modified) = metadata.modified() {
        header(
            &mut response,
            "last-modified",
            &httpdate::fmt_http_date(modified),
        )?;
    }
    let filename = safe.file_name().and_then(|v| v.to_str()).unwrap_or("");
    header(
        &mut response,
        "content-disposition",
        &format!("inline; filename=\"{filename}\""),
    )?;
    security(&mut response, mime);
    Ok(response)
}
async fn user_agent(auth: &AuthService) -> Result<String, StoreError> {
    let load = || async {
        let settings = auth.source_stream_settings().await?;
        let id = settings.get("default_user_agent").and_then(|v| {
            v.as_i64()
                .or_else(|| {
                    v.as_f64()
                        .filter(|v| {
                            v.is_finite() && *v >= i64::MIN as f64 && *v < (i64::MAX as f64)
                        })
                        .map(|v| v as i64)
                })
                .or_else(|| {
                    dispatcharr_ingest_core::source_integer_literal(v.as_str()?)?
                        .parse()
                        .ok()
                })
                .or_else(|| v.as_bool().map(i64::from))
        });
        let value = if let Some(id) = id {
            sqlx::query_scalar::<_, String>("SELECT user_agent FROM core_useragent WHERE id=$1")
                .bind(id)
                .fetch_optional(auth.store.pool())
                .await?
                .filter(|v| !v.is_empty())
        } else {
            None
        };
        Ok::<_, StoreError>(value.unwrap_or_else(|| "Dispatcharr/0.33.0".into()))
    };
    let Some(cache) = auth.notification_cache.as_ref() else {
        return load().await;
    };
    let key = ":1:coresettings:default_user_agent";
    let version = ":1:coresettings:default_user_agent:ver";
    match cache.get(key).await {
        Ok(Some(raw)) => {
            if let Some(value) = crate::decode_source_cache(&raw)
                .and_then(|v| v.as_str().filter(|v| !v.is_empty()).map(str::to_owned))
            {
                return Ok(value);
            }
        }
        Ok(None) => {}
        Err(_) => return load().await,
    }
    let before = match cache.get(version).await {
        Ok(v) => v,
        Err(_) => return load().await,
    };
    let value = load().await?;
    let after = match cache.get(version).await {
        Ok(v) => v,
        Err(_) => return Ok(value),
    };
    if before == after {
        let _ = cache.set_for(key, &json!(value), 300).await;
    }
    Ok(value)
}
async fn remote(
    auth: &AuthService,
    url: &str,
    permit: OwnedSemaphorePermit,
) -> Result<Response, StoreError> {
    if auth.images.failed(url) {
        return Err(missing("Remote image temporarily unavailable"));
    }
    let agent = user_agent(auth).await?;
    let mut current = url.to_owned();
    for _ in 0..=5 {
        if let Err(error) = validate_url(&current).await {
            auth.images.remember(url);
            return Err(error);
        }
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(3))
            .read_timeout(Duration::from_secs(5))
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .pool_max_idle_per_host(0)
            .http1_only()
            .no_gzip()
            .no_deflate()
            .no_brotli()
            .no_zstd()
            .build()
            .map_err(|_| missing("Error fetching remote image"))?;
        let agent = agent
            .chars()
            .map(|v| u8::try_from(v as u32))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| missing("Error fetching remote image"))?;
        let response = match client
            .get(&current)
            .header("user-agent", agent)
            .header("accept", "*/*")
            .header("accept-encoding", "gzip, deflate")
            .send()
            .await
        {
            Ok(v) => v,
            Err(_) => {
                auth.images.remember(url);
                return Err(missing("Error fetching remote image"));
            }
        };
        let status = response.status().as_u16();
        if [301, 302, 303, 307, 308].contains(&status) {
            let location = response
                .headers()
                .get("location")
                .and_then(|v| v.to_str().ok())
                .filter(|v| !v.is_empty());
            let Some(location) = location else {
                auth.images.remember(url);
                return Err(missing("Remote image not found"));
            };
            current = reqwest::Url::parse(&current)
                .and_then(|v| v.join(location))
                .map_err(|_| missing("Remote image not allowed"))?
                .to_string();
            continue;
        }
        if status != 200 {
            auth.images.remember(url);
            return Err(missing("Remote image not found"));
        }
        let cache_control = response.headers().get("cache-control").cloned();
        let modified = response.headers().get("last-modified").cloned();
        let mut stream = dispatcharr_upstream_http::body(response)
            .map_err(|_| missing("Error fetching remote image"))?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let mut bytes = Vec::new();
        loop {
            let frame = match tokio::time::timeout_at(deadline, stream.next()).await {
                Ok(Some(Ok(frame))) => frame,
                Ok(None) => break,
                Ok(Some(Err(_))) => {
                    auth.images.remember(url);
                    return Err(missing("Error fetching remote image"));
                }
                Err(_) => {
                    auth.images.remember(url);
                    return Err(missing("Remote image fetch timed out"));
                }
            };
            if bytes.len().saturating_add(frame.len()) > MAX_BYTES {
                return Err(missing("Remote image too large"));
            }
            append_bounded(&mut bytes, &frame, MAX_BYTES);
        }
        let Some(mime) = sniff(&bytes) else {
            auth.images.remember(url);
            return Err(missing("Remote image type not allowed"));
        };
        auth.images.forget(url);
        let length = bytes.len();
        let mut response = Body::from(Bytes::from_owner(Retained::new(
            bytes,
            Arc::new(permit),
            auth.images.memory.clone(),
        )))
        .into_response();
        header(&mut response, "content-type", mime)?;
        header(&mut response, "content-length", &length.to_string())?;
        if let Some(value) = cache_control.filter(|v| !v.is_empty()) {
            response.headers_mut().insert("cache-control", value);
        }
        if let Some(value) = modified.filter(|v| !v.is_empty()) {
            response.headers_mut().insert("last-modified", value);
        }
        let filename = url
            .split('?')
            .next()
            .unwrap_or("")
            .rsplit('/')
            .next()
            .filter(|v| !v.is_empty())
            .unwrap_or("image");
        header(
            &mut response,
            "content-disposition",
            &format!("inline; filename=\"{filename}\""),
        )?;
        security(&mut response, mime);
        return Ok(response);
    }
    auth.images.remember(url);
    Err(missing("Remote image redirect limit exceeded"))
}
pub(crate) async fn cache(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    Path(raw): Path<String>,
) -> Result<Response, StoreError> {
    if ["authorization", "x-api-key"]
        .iter()
        .any(|name| headers.get(*name).is_some_and(|v| v.to_str().is_err()))
    {
        return Err(StoreError::Unauthorized("Invalid authentication header"));
    }
    auth.authenticate(&crate::auth::request(peer, headers))
        .await?;
    let row = crate::logos::lookup(&auth, &raw, &uri).await?;
    let url = row["url"].as_str().unwrap_or("");
    auth.image_response(url).await
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_policy_boundary_vectors() {
        let source: Value =
            serde_json::from_str(include_str!("../datasets/source-image-ip-policy.json")).unwrap();
        for case in source["vectors"].as_array().unwrap() {
            assert_eq!(
                address_allowed(case[0].as_str().unwrap().parse().unwrap()),
                case[1].as_bool().unwrap(),
                "{}",
                case[0]
            );
        }
    }
    #[test]
    fn sniff_magic_and_active_html_boundaries() {
        assert_eq!(
            sniff(b"\x89PNG\r\n\x1a\nno-decoder-required"),
            Some("image/png")
        );
        assert_eq!(sniff(b" \xef\xbb\xbf<!--x--><sVg/>"), Some("image/svg+xml"));
        assert_eq!(sniff(b"<html><svg/></html>"), None);
        assert_eq!(sniff(b"BM123"), None);
        assert_eq!(sniff(b"BM1234"), Some("image/bmp"));
    }
    #[tokio::test]
    async fn response_bytes_keep_admission_until_final_clone_dropped() {
        let runtime = ImageRuntime::new();
        let permit = runtime.reserve().await.unwrap();
        let bytes = Bytes::from_owner(Retained::new(
            vec![1],
            Arc::new(permit),
            runtime.memory.clone(),
        ));
        let clone = bytes.clone();
        drop(bytes);
        assert_eq!(runtime.slots.available_permits(), 3);
        drop(clone);
        assert_eq!(runtime.slots.available_permits(), 4);
    }
    #[tokio::test]
    async fn bounded_waiters_cancel_without_leaking_admission() {
        let runtime = ImageRuntime::new();
        let mut held = Vec::new();
        for _ in 0..4 {
            held.push(runtime.reserve().await.unwrap());
        }
        let mut waiting = Vec::new();
        for _ in 0..64 {
            let runtime = runtime.clone();
            waiting.push(tokio::spawn(async move { runtime.reserve().await }));
        }
        for _ in 0..100 {
            if runtime.waiting.available_permits() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(runtime.waiting.available_permits(), 0);
        assert!(matches!(
            runtime.reserve().await,
            Err(StoreError::StatusBody(503, _))
        ));
        for task in waiting {
            task.abort();
            let _ = task.await;
        }
        assert_eq!(runtime.waiting.available_permits(), 64);
        drop(held);
        assert_eq!(runtime.slots.available_permits(), 4);
    }
    #[test]
    fn image_buffer_capacity_never_grows_past_limit() {
        let mut bytes = Vec::new();
        for _ in 0..640 {
            append_bounded(&mut bytes, &[1; 8192], MAX_BYTES);
        }
        assert_eq!(bytes.len(), MAX_BYTES);
        assert_eq!(bytes.capacity(), MAX_BYTES);
    }
}
