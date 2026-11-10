//! XMLTV HTTP/cache adapter ported from the original Dispatcharr output views.
//! Original authors retain full credit. Packaged proof remains a separate gate.
use crate::{
    cache::{Cache, CacheConfig, EntryState},
    content,
    delivery::{Delivery, Source},
    rows, window,
};
use axum::{
    body::Body,
    extract::{ConnectInfo, OriginalUri, State},
    http::{HeaderMap, Method, Request, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use bytes::Bytes;
use dispatcharr_contracts::PlaybackRequest;
use dispatcharr_store::{AuthService, Store, StoreError};
use md5::{Digest, Md5};
use serde_json::json;
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore};

#[derive(Clone)]
pub struct Config {
    pub max_requests: usize,
    pub max_builds: usize,
    pub max_memory_bytes: usize,
    pub max_body_bytes: usize,
    pub max_frame_bytes: usize,
    pub max_number_bytes: usize,
    pub setup_deadline: Duration,
    pub stream_deadline: Duration,
    pub rows: rows::Limits,
    pub cache: CacheConfig,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            max_requests: 64,
            max_builds: 2,
            max_memory_bytes: 512 * 1024 * 1024,
            max_body_bytes: 64 * 1024,
            max_frame_bytes: 1024 * 1024,
            max_number_bytes: 104 * 1024 * 1024,
            setup_deadline: Duration::from_secs(15),
            stream_deadline: Duration::from_secs(600),
            rows: rows::Limits {
                max_channels: 50_000,
                max_metadata_bytes: 16 * 1024 * 1024,
                max_programme_metadata_bytes: 1024 * 1024,
                max_programmes: 2_000_000,
            },
            cache: CacheConfig::default(),
        }
    }
}
struct Inner {
    store: Store,
    auth: AuthService,
    cache: Cache,
    config: Config,
    requests: Arc<Semaphore>,
    builds: Arc<Semaphore>,
    memory: Arc<Semaphore>,
    stop: watch::Sender<bool>,
    stopped: AtomicBool,
    workers: AtomicUsize,
}
#[derive(Clone)]
pub struct Gateway {
    inner: Arc<Inner>,
}
struct Worker(Arc<Inner>);
impl Drop for Worker {
    fn drop(&mut self) {
        self.0.workers.fetch_sub(1, Ordering::AcqRel);
    }
}
struct Memory {
    _permit: OwnedSemaphorePermit,
}
struct RequestLease {
    _request: OwnedSemaphorePermit,
    memory: Arc<Memory>,
}
fn unavailable() -> StoreError {
    StoreError::StatusBody(
        503,
        json!({"detail":"XMLTV resource capacity unavailable."}),
    )
}
fn build_bytes(config: &Config) -> Option<usize> {
    config
        .rows
        .max_metadata_bytes
        .checked_mul(3)?
        .checked_add(config.max_number_bytes.checked_mul(2)?)?
        .checked_add(config.rows.max_programme_metadata_bytes.checked_mul(64)?)?
        .checked_add(config.max_frame_bytes.checked_mul(4)?)?
        .checked_add(1024 * 1024)
}
impl Gateway {
    pub async fn new(
        store: Store,
        auth: AuthService,
        redis_url: &str,
        config: Config,
    ) -> Result<Self, StoreError> {
        config.rows.validate()?;
        if config.max_requests == 0
            || config.max_requests > 128
            || config.max_builds == 0
            || config.max_builds > 16
            || config.max_body_bytes == 0
            || config.max_body_bytes > 1024 * 1024
            || config.max_frame_bytes < 1024
            || config.max_frame_bytes > 8 * 1024 * 1024
            || config.max_number_bytes > 128 * 1024 * 1024
            || config.max_memory_bytes > 1024 * 1024 * 1024
            || build_bytes(&config).is_none_or(|v| v > config.max_memory_bytes)
            || config.setup_deadline.is_zero()
            || config.stream_deadline.is_zero()
            || config.stream_deadline > Duration::from_secs(600)
        {
            return Err(unavailable());
        }
        let cache = Cache::connect(redis_url, config.cache.clone())
            .await
            .map_err(|_| unavailable())?;
        let (stop, _) = watch::channel(false);
        Ok(Self {
            inner: Arc::new(Inner {
                store,
                auth,
                cache,
                requests: Arc::new(Semaphore::new(config.max_requests)),
                builds: Arc::new(Semaphore::new(config.max_builds)),
                memory: Arc::new(Semaphore::new(config.max_memory_bytes)),
                config,
                stop,
                stopped: AtomicBool::new(false),
                workers: AtomicUsize::new(0),
            }),
        })
    }
    pub fn router(self) -> Router {
        Router::new()
            .route("/output/epg", any(dispatch))
            .route("/output/epg/", any(dispatch))
            .route("/output/epg/{profile}", any(dispatch))
            .route("/output/epg/{profile}/", any(dispatch))
            .route("/xmltv.php", any(dispatch))
            .with_state(self)
    }
    fn worker(&self) -> Worker {
        self.inner.workers.fetch_add(1, Ordering::AcqRel);
        Worker(self.inner.clone())
    }
    pub fn active_requests(&self) -> usize {
        self.inner.config.max_requests - self.inner.requests.available_permits()
    }
    /// Bounded process-local counters; no public application HTTP route.
    pub fn cache_diagnostics(&self) -> serde_json::Value {
        self.inner.cache.diagnostics()
    }
    pub fn available_memory_bytes(&self) -> usize {
        self.inner.memory.available_permits()
    }
    pub async fn shutdown(&self) -> bool {
        self.inner.stopped.store(true, Ordering::Release);
        self.inner.stop.send_replace(true);
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.inner.workers.load(Ordering::Acquire) > 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            self.inner.cache.drain_cleanup(Duration::from_secs(5)).await
        })
        .await
        .unwrap_or(false)
    }
    fn attachment(&self, body: Body) -> Response {
        (
            [
                ("content-type", "application/xml"),
                (
                    "content-disposition",
                    "attachment; filename=\"Dispatcharr.xml\"",
                ),
                ("cache-control", "no-cache"),
            ],
            body,
        )
            .into_response()
    }
    async fn response(
        &self,
        identity: PlaybackRequest,
        path: String,
        method: Method,
        headers: HeaderMap,
        body: Vec<u8>,
        lease: Arc<RequestLease>,
    ) -> Result<Response, StoreError> {
        if let Some(failure) =
            dispatcharr_output_http::source_csrf_failure(&method, &headers, &body)
        {
            return Ok((
                StatusCode::FORBIDDEN,
                [("content-type", "text/html; charset=utf-8")],
                failure,
            )
                .into_response());
        }
        let profile = path
            .strip_prefix("/output/epg/")
            .map(|s| decode_path(s.trim_end_matches('/')))
            .filter(|s| !s.is_empty());
        let xc = path == "/xmltv.php";
        let user = if xc {
            match self
                .inner
                .auth
                .authorize_xc(
                    &identity,
                    identity.parameter("username").unwrap_or(""),
                    identity.parameter("password").unwrap_or(""),
                    "XC_API",
                )
                .await
            {
                Ok(user) => Some(user),
                Err(error) => {
                    let reason = match &error {
                        StoreError::XcDenied { status: 403, .. } => {
                            "Network access denied (XC API)"
                        }
                        StoreError::XcDenied { status: 429, .. } => "XC auth rate limited",
                        _ => "Invalid XC credentials",
                    };
                    self.event("epg_blocked",json!({"user":identity.parameter("username").unwrap_or("unknown"),"reason":reason,"client_ip":self.inner.auth.client_ip_text(&identity),"user_agent":identity.header("user-agent").unwrap_or("unknown")})).await;
                    return Err(error);
                }
            }
        } else {
            if !self
                .inner
                .auth
                .source_network_allowed(self.inner.auth.client_ip(&identity), "M3U_EPG", None)
                .await?
            {
                self.event("epg_blocked",json!({"profile":profile.as_deref().unwrap_or("all"),"reason":"Network access denied","client_ip":self.inner.auth.client_ip_text(&identity),"user_agent":identity.header("user-agent").unwrap_or("unknown")})).await;
                return Ok(source_json_error(403, None));
            }
            if method == Method::HEAD {
                return Ok(self.attachment(Body::empty()));
            }
            None
        };
        let policy = dispatcharr_store::source_epg_history_policy(
            &self.inner.auth.source_epg_settings().await?,
        )
        .map_err(|_| unavailable())?;
        let window = window::resolve(
            &identity.query,
            user.as_ref().map(|u| &u.custom_properties),
            policy,
        )
        .map_err(|_| unavailable())?;
        let cached_logos = identity
            .parameter("cachedlogos")
            .unwrap_or("true")
            .to_lowercase()
            != "false";
        let tvg_id_source = identity
            .parameter("tvg_id_source")
            .unwrap_or("channel_number")
            .to_lowercase();
        let origin = dispatcharr_output_http::source_output_origin(&self.inner.auth, &identity);
        let username = user
            .as_ref()
            .map_or("anonymous", |user| user.username.as_str());
        let prefix = format!(
            "epg_content:{}:{username}:d={}:p={}:logos={}:tvgid={tvg_id_source}:origin={origin}",
            profile.as_deref().unwrap_or("all"),
            window.forward_days,
            window.prev_days,
            if cached_logos { "True" } else { "False" }
        );
        let mut random = [0u8; 16];
        getrandom::fill(&mut random).map_err(|_| unavailable())?;
        let token = random
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let entry = self
            .inner
            .cache
            .open(&prefix, &token, lease.clone())
            .await
            .map_err(|_| unavailable())?;
        let build = if entry.state == EntryState::Leader {
            let slot = self
                .inner
                .builds
                .clone()
                .try_acquire_owned()
                .map_err(|_| unavailable())?;
            let bytes = build_bytes(&self.inner.config).ok_or_else(unavailable)?;
            let memory = self
                .inner
                .memory
                .clone()
                .try_acquire_many_owned(bytes as u32)
                .map_err(|_| unavailable())?;
            Some(Arc::new((slot, memory)))
        } else {
            None
        };
        if build.is_some() {
            if let rows::Channels::Rows(channels) = rows::channels(
                &self.inner.store,
                user.as_ref(),
                profile.as_deref(),
                &self.inner.config.rows,
            )
            .await?
            {
                if channels.iter().any(rows::requires_custom_adapter) {
                    return Err(StoreError::StatusBody(
                        501,
                        json!({"detail":"Custom XMLTV source properties require a verified regex adapter."}),
                    ));
                }
            }
        }
        let event_profile = profile.as_deref().unwrap_or("all").to_owned();
        let event_user = username.to_owned();
        let ip = self.inner.auth.client_ip_text(&identity);
        let agent = identity
            .header("user-agent")
            .unwrap_or("unknown")
            .to_owned();
        let digest = format!("{:x}", Md5::digest(format!("{ip}:{agent}")));
        let event_key = format!(
            ":1:epg_download:{event_user}:{event_profile}:{}",
            &digest[..12]
        );
        let channel_count = Arc::new(AtomicUsize::new(0));
        let ctx = content::Context {
            origin,
            profile,
            user,
            window,
            cached_logos,
            tvg_id_source,
            now: chrono::Utc::now(),
            max_frame_bytes: self.inner.config.max_frame_bytes,
            numbering_bytes: self.inner.config.max_number_bytes,
        };
        let source: Option<Source> = if build.is_some() {
            Some(content::generate_counted(
                self.inner.store.clone(),
                ctx,
                self.inner.config.rows.clone(),
                Some(channel_count.clone()),
            ))
        } else {
            None
        };
        let mut delivery = Delivery::new(
            self.inner.cache.clone(),
            entry,
            source,
            self.inner.config.stream_deadline,
        )
        .map_err(|_| unavailable())?;
        let (demand, mut receiver) = tokio::sync::mpsc::channel::<
            tokio::sync::oneshot::Sender<Result<Option<Bytes>, std::io::Error>>,
        >(1);
        let mut stopped = self.inner.stop.subscribe();
        let worker = self.worker();
        let engine = self.clone();
        tokio::spawn(async move {
            let _worker = worker;
            let physical_guard: Arc<dyn Send + Sync> = Arc::new((lease.clone(), build));
            loop {
                if *stopped.borrow() {
                    break;
                }
                let reply = tokio::select! {value=receiver.recv()=>match value{Some(value)=>value,None=>break},_=stopped.changed()=>break};
                let next = tokio::select! {value=delivery.next(physical_guard.clone())=>value,_=stopped.changed()=>break};
                match next {
                    Ok(Some(mut bytes)) => {
                        bytes.retain_frame_guard(lease.memory.clone());
                        if reply.send(Ok(Some(Bytes::from_owner(bytes)))).is_err() {
                            break;
                        }
                    }
                    Ok(None) => {
                        if delivery.completed_build()
                            && matches!(
                                engine
                                    .inner
                                    .cache
                                    .event_seen(&event_key, physical_guard.clone())
                                    .await,
                                Ok(false)
                            )
                        {
                            engine.event("epg_download",json!({"profile":event_profile,"user":event_user,"channels":channel_count.load(Ordering::Acquire),"client_ip":ip,"user_agent":agent})).await;
                            let _ = engine
                                .inner
                                .cache
                                .mark_event(&event_key, physical_guard.clone())
                                .await;
                        }
                        let _ = reply.send(Ok(None));
                        break;
                    }
                    Err(_) => {
                        // Original ordinary StreamingHttpResponse closes the partial
                        // 200 entity after generator failure (missing profile preamble).
                        let _ = reply.send(Ok(None));
                        break;
                    }
                }
            }
            drop(delivery);
            drop(engine);
        });
        let stream = futures_util::stream::unfold(demand, |demand| async move {
            let (send, receive) = tokio::sync::oneshot::channel();
            if demand.send(send).await.is_err() {
                return None;
            }
            match receive.await {
                Ok(Ok(Some(bytes))) => Some((Ok::<Bytes, std::io::Error>(bytes), demand)),
                Ok(Err(error)) => Some((Err(error), demand)),
                _ => None,
            }
        });
        Ok(self.attachment(Body::from_stream(stream)))
    }
    async fn event(&self, kind: &str, details: serde_json::Value) {
        // Generic plugin/Connect dispatch remains the tracked M8 bridge.
        if sqlx::query("INSERT INTO core_systemevent(event_type,timestamp,channel_id,channel_name,details)VALUES($1,now(),NULL,NULL,$2)").bind(kind).bind(details).execute(self.inner.store.pool()).await.is_err(){return}
        let settings = self
            .inner
            .auth
            .system_settings()
            .await
            .unwrap_or(serde_json::Value::Null);
        let max = event_limit(settings.get("max_system_events"));
        if max < 0 {
            return;
        }
        let _=async {
            let count:i64=sqlx::query_scalar("SELECT count(*) FROM core_systemevent").fetch_one(self.inner.store.pool()).await?;
            if count>max {
                let cutoff:Option<i64>=sqlx::query_scalar("SELECT id::bigint FROM core_systemevent ORDER BY timestamp DESC LIMIT 1 OFFSET $1").bind(max).fetch_optional(self.inner.store.pool()).await?;
                if let Some(cutoff)=cutoff{sqlx::query("DELETE FROM core_systemevent WHERE id<$1").bind(cutoff).execute(self.inner.store.pool()).await?;}
            }
            Ok::<_,sqlx::Error>(())
        }.await;
    }
}
fn decode_path(raw: &str) -> String {
    let mut bytes = Vec::with_capacity(raw.len());
    let input = raw.as_bytes();
    let mut at = 0;
    while at < input.len() {
        if input[at] == b'%' && at + 2 < input.len() {
            if let (Some(a), Some(b)) = (
                (input[at + 1] as char).to_digit(16),
                (input[at + 2] as char).to_digit(16),
            ) {
                let value = (a * 16 + b) as u8;
                bytes.push(value);
                at += 3;
                continue;
            }
        }
        bytes.push(input[at]);
        at += 1;
    }
    String::from_utf8_lossy(&bytes).into_owned()
}
fn event_limit(value: Option<&serde_json::Value>) -> i64 {
    use serde_json::Value;
    match value {
        Some(Value::Bool(true)) => 1,
        Some(Value::Number(value)) if value.as_f64().is_some_and(|v| v != 0.0) => {
            let text = value.to_string();
            if text.contains(['.', 'e', 'E']) {
                value
                    .as_f64()
                    .filter(|v| v.is_finite() && *v >= i64::MIN as f64 && *v < i64::MAX as f64)
                    .map_or(100, |v| v.trunc() as i64)
            } else {
                text.parse().unwrap_or(i64::MAX)
            }
        }
        Some(Value::String(value)) if !value.is_empty() => {
            dispatcharr_ingest_core::source_integer_literal(value)
                .map_or(100, |value| value.parse().unwrap_or(i64::MAX))
        }
        _ => 100,
    }
}
async fn dispatch(
    State(engine): State<Gateway>,
    OriginalUri(uri): OriginalUri,
    request: Request<Body>,
) -> Response {
    let result=async {
        if engine.inner.stopped.load(Ordering::Acquire){return Err(unavailable())}
        let permit=engine.inner.requests.clone().try_acquire_owned().map_err(|_|unavailable())?;
        let memory=engine.inner.memory.clone().try_acquire_many_owned((engine.inner.config.max_body_bytes*3+1024*1024)as u32).map_err(|_|unavailable())?;
        let lease=Arc::new(RequestLease{_request:permit,memory:Arc::new(Memory{_permit:memory})});
        if uri.query().is_some_and(|q|q.len()>65536) || uri.path().len()>8192 || request.headers().len()>4096 || request.headers().iter().try_fold(0usize,|n,(key,value)|n.checked_add(key.as_str().len()+value.as_bytes().len())).is_none_or(|n|n>65536){return Err(unavailable())}
        let peer_ip=request.extensions().get::<ConnectInfo<SocketAddr>>().ok_or_else(unavailable)?.0.ip();
        let identity=PlaybackRequest{channel_id:String::new(),peer_ip,query:url::form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()).into_owned().collect(),headers:dispatcharr_contracts::source_headers(request.headers().iter().map(|(key,value)|(key.as_str(),value.as_bytes())))};
        let method=request.method().clone();let headers=request.headers().clone();let path=uri.path().to_owned();let worker=engine.worker();let owned=engine.clone();
        tokio::spawn(async move {
            let _worker=worker;let mut stopped=owned.inner.stop.subscribe();if *stopped.borrow(){return Err(unavailable())}
            let work=async {let body=axum::body::to_bytes(request.into_body(),owned.inner.config.max_body_bytes).await.map_err(|_|unavailable())?;owned.response(identity,path,method,headers,body.to_vec(),lease).await};
            tokio::select!{value=tokio::time::timeout(owned.inner.config.setup_deadline,work)=>value.map_err(|_|unavailable())?,_=stopped.changed()=>Err(unavailable())}
        }).await.map_err(|_|unavailable())?
    }.await;
    let mut response = match result {
        Ok(response) => response,
        Err(StoreError::XcDenied {
            status,
            retry_after,
        }) => source_json_error(status, retry_after),
        Err(error) => error.into_response(),
    };
    response
        .headers_mut()
        .insert("x-content-type-options", "nosniff".parse().unwrap());
    response
}

fn source_json_error(status: u16, retry_after: Option<u64>) -> Response {
    let message = match status {
        403 => "Forbidden",
        429 => "Too many attempts",
        _ => "Unauthorized",
    };
    let mut response = (
        StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        [("content-type", "application/json")],
        format!("{{\"error\": \"{message}\"}}"),
    )
        .into_response();
    if let Some(wait) = retry_after {
        if let Ok(value) = wait.to_string().parse() {
            response.headers_mut().insert("retry-after", value);
        }
    }
    response
}
#[cfg(test)]
#[path = "gateway_tests.rs"]
mod tests;
