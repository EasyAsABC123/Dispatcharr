//! Bounded native M3U HTTP adapter ported from Dispatcharr output views.
//! Original Dispatcharr authors/contributors retain all application credit.
use crate::{csrf, reader, render, string_cache};
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
use dispatcharr_output_store::transport::{RedisTransport, TransportConfig};
use dispatcharr_store::{decode_source_cache, encode_source_cache, AuthService, Store, StoreError};
use md5::{Digest, Md5};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore};
const NOT_FOUND:&str="\n<!doctype html>\n<html lang=\"en\">\n<head>\n  <title>Not Found</title>\n</head>\n<body>\n  <h1>Not Found</h1><p>The requested resource was not found on this server.</p>\n</body>\n</html>\n";
#[derive(Clone)]
pub struct OutputConfig {
    pub max_requests: usize,
    pub max_channels: usize,
    pub max_metadata_bytes: usize,
    pub max_export_bytes: usize,
    /// Shared retained response/cache/build allocations, independent of request count.
    pub max_memory_bytes: usize,
    pub max_body_bytes: usize,
    pub build_deadline: Duration,
}
impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            max_requests: 4,
            max_channels: 50000,
            max_metadata_bytes: 8 * 1024 * 1024,
            max_export_bytes: 8 * 1024 * 1024,
            max_memory_bytes: 192 * 1024 * 1024,
            max_body_bytes: 64 * 1024,
            build_deadline: Duration::from_secs(15),
        }
    }
}
struct Inner {
    store: Store,
    auth: AuthService,
    redis: RedisTransport,
    config: OutputConfig,
    requests: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    stop: watch::Sender<bool>,
    stopped: AtomicBool,
}
#[derive(Clone)]
pub struct OutputGateway {
    inner: Arc<Inner>,
}
struct MemoryReservation {
    permit: OwnedSemaphorePermit,
}
struct GuardedBytes {
    bytes: Vec<u8>,
    _memory: Arc<MemoryReservation>,
}
impl AsRef<[u8]> for GuardedBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl OutputGateway {
    pub async fn new(
        store: Store,
        auth: AuthService,
        redis_url: &str,
        config: OutputConfig,
    ) -> Result<Self, StoreError> {
        if config.max_requests == 0
            || config.max_requests > 128
            || config.max_channels == 0
            || config.max_channels > 50000
            || config.max_metadata_bytes == 0
            || config.max_metadata_bytes > 32 * 1024 * 1024
            || config.max_export_bytes == 0
            || config.max_export_bytes > 64 * 1024 * 1024
            || config.max_memory_bytes == 0
            || config.max_memory_bytes > 512 * 1024 * 1024
            || build_reservation(&config).is_none_or(|bytes| bytes > config.max_memory_bytes)
            || config.max_body_bytes == 0
            || config.max_body_bytes > 1024 * 1024
            || config.build_deadline.is_zero()
        {
            return Err(StoreError::Unsupported("invalid output resource budget"));
        }
        let redis = RedisTransport::connect(
            redis_url,
            TransportConfig {
                max_operations: 8,
                max_queued_operations: 128,
                command_timeout: Duration::from_secs(3),
            },
        )
        .await
        .map_err(|_| unavailable())?;
        let requests = Arc::new(Semaphore::new(config.max_requests));
        let bytes = Arc::new(Semaphore::new(config.max_memory_bytes));
        let (stop, _) = watch::channel(false);
        Ok(Self {
            inner: Arc::new(Inner {
                store,
                auth,
                redis,
                config,
                requests,
                bytes,
                stop,
                stopped: AtomicBool::new(false),
            }),
        })
    }
    pub fn router(self) -> Router {
        Router::new()
            .route("/output/m3u", any(dispatch))
            .route("/output/m3u/", any(dispatch))
            .route("/output/m3u/{profile}", any(dispatch))
            .route("/output/m3u/{profile}/", any(dispatch))
            .route("/get.php", any(dispatch))
            .with_state(self)
    }
    pub async fn shutdown(&self) -> bool {
        self.inner.stopped.store(true, Ordering::SeqCst);
        self.inner.stop.send_replace(true);
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.inner.requests.available_permits() != self.inner.config.max_requests {
                tokio::time::sleep(Duration::from_millis(10)).await
            }
        })
        .await
        .is_ok()
    }
    async fn get_cache(
        &self,
        key: &str,
        memory: Arc<MemoryReservation>,
    ) -> Result<Option<Value>, StoreError> {
        let content = key.starts_with("m3u_content:");
        let limit = if content {
            string_cache::max_encoded(self.inner.config.max_export_bytes)
        } else {
            1024 * 1024
        };
        let limit = limit.to_string();
        let (raw,_):(Option<Vec<u8>>,Arc<MemoryReservation>)=self.inner.redis.command("if redis.call('STRLEN',KEYS[1])>tonumber(ARGV[1]) then return redis.error_reply('export cache limit') end return redis.call('GET',KEYS[1])",&[format!(":1:{key}")],&[limit.as_bytes()],memory).await.map_err(|_|unavailable())?;
        raw.map(|value| {
            if content {
                string_cache::decode(value, self.inner.config.max_export_bytes)
                    .map(Value::String)
                    .ok_or_else(unavailable)
            } else {
                decode_source_cache(&value).ok_or_else(unavailable)
            }
        })
        .transpose()
    }
    async fn set_cache(
        &self,
        key: &str,
        value: Value,
        memory: Arc<MemoryReservation>,
    ) -> Result<(), StoreError> {
        let raw = if key.starts_with("m3u_content:") {
            string_cache::encode(
                value.as_str().ok_or_else(unavailable)?,
                self.inner.config.max_export_bytes,
            )
        } else {
            encode_source_cache(&value)
        }
        .ok_or_else(unavailable)?;
        self.inner
            .redis
            .command::<String, _>(
                "return redis.call('SET',KEYS[1],ARGV[1],'EX',2)",
                &[format!(":1:{key}")],
                &[&raw],
                memory,
            )
            .await
            .map_err(|_| unavailable())?;
        Ok(())
    }
    async fn event(&self, kind: &str, details: Value) {
        // As Source, audit failures do not turn a successful download into500.
        // Connect/plugin integration dispatch remains the separately tracked M8 bridge.
        if sqlx::query("INSERT INTO core_systemevent(event_type,timestamp,channel_id,channel_name,details)VALUES($1,now(),NULL,NULL,$2)").bind(kind).bind(details).execute(self.inner.store.pool()).await.is_err(){return;}
        let settings = self
            .inner
            .auth
            .system_settings()
            .await
            .unwrap_or(Value::Null);
        let raw = settings.get("max_system_events");
        let max = match raw {
            Some(Value::Bool(true)) => 1,
            Some(Value::Number(value)) => value
                .as_i64()
                .filter(|v| *v != 0)
                .or_else(|| {
                    value
                        .as_f64()
                        .filter(|v| v.is_finite() && *v != 0.0)
                        .map(|v| v as i64)
                })
                .unwrap_or(100),
            Some(Value::String(value)) if !value.is_empty() => {
                value.trim().replace('_', "").parse().unwrap_or(100)
            }
            _ => 100,
        };
        if max < 0 {
            return;
        }
        let result=async{
            let count:i64=sqlx::query_scalar("SELECT count(*) FROM core_systemevent").fetch_one(self.inner.store.pool()).await?;
            if count>max{let cutoff:Option<i64>=sqlx::query_scalar("SELECT id::bigint FROM core_systemevent ORDER BY timestamp DESC LIMIT 1 OFFSET $1").bind(max).fetch_optional(self.inner.store.pool()).await?;if let Some(cutoff)=cutoff{sqlx::query("DELETE FROM core_systemevent WHERE id<$1").bind(cutoff).execute(self.inner.store.pool()).await?;}}
            Ok::<_,sqlx::Error>(())
        }.await;
        let _ = result;
    }
    async fn response(
        &self,
        identity: PlaybackRequest,
        path: String,
        method: Method,
        headers: HeaderMap,
        body: Vec<u8>,
        reservation: Reservation,
    ) -> Result<Response, StoreError> {
        if let Err(failure) = csrf::check(&method, &headers, &body) {
            return Ok(html(StatusCode::FORBIDDEN, failure.body()));
        }
        let xc = path == "/get.php";
        let profile = path
            .strip_prefix("/output/m3u/")
            .map(|value| percent_path(value.trim_end_matches('/')))
            .filter(|value| !value.is_empty());
        let ip = self.inner.auth.client_ip_text(&identity);
        let agent = identity.header("user-agent").unwrap_or("unknown");
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
                    let status = match error {
                        StoreError::XcDenied { status, .. } => status,
                        _ => 500,
                    };
                    self.event("m3u_blocked",json!({"user":identity.parameter("username").unwrap_or("unknown"),"reason":match status{403=>"Network access denied (XC API)",429=>"XC auth rate limited",_=>"Invalid XC credentials"},"client_ip":ip,"user_agent":agent})).await;
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
                self.event("m3u_blocked",json!({"profile":profile.as_deref().unwrap_or("all"),"reason":"Network access denied","client_ip":ip,"user_agent":agent})).await;
                return Ok((
                    StatusCode::FORBIDDEN,
                    axum::Json(json!({"error":"Forbidden"})),
                )
                    .into_response());
            }
            if method == Method::HEAD {
                return Ok(self.attachment(Vec::new(), reservation));
            }
            None
        };
        if !matches!(method, Method::GET | Method::POST | Method::HEAD) {
            let mut response = StatusCode::METHOD_NOT_ALLOWED.into_response();
            response
                .headers_mut()
                .insert("allow", "GET, POST, HEAD".parse().unwrap());
            response
                .headers_mut()
                .insert("content-type", "text/html; charset=utf-8".parse().unwrap());
            return Ok(response);
        }
        if method == Method::POST && !body.is_empty() && body != b"{}" {
            return Ok(html(
                StatusCode::FORBIDDEN,
                "POST requests with body are not allowed.",
            ));
        }
        let origin = origin(&self.inner.auth, &identity, &headers);
        let username = user
            .as_ref()
            .map_or("anonymous", |user| user.username.as_str());
        let key = format!(
            "m3u_content:{}:{}:{}:origin={origin}",
            profile.as_deref().unwrap_or("all"),
            username,
            grouped_query(&identity.query)
        );
        if let Some(Value::String(content)) =
            self.get_cache(&key, reservation.memory.clone()).await?
        {
            if !content.is_empty() {
                return Ok(self.attachment(content.into_bytes(), reservation));
            }
        }
        let direct = identity
            .parameter("direct")
            .is_some_and(|value| value.eq_ignore_ascii_case("true"))
            && user.as_ref().is_none_or(|user| user.user_level >= 10);
        let channels = match reader::read(
            &self.inner.store,
            reader::Read {
                user: user.as_ref(),
                profile: profile.as_deref(),
                direct,
                max_channels: self.inner.config.max_channels,
                max_metadata_bytes: self.inner.config.max_metadata_bytes,
            },
        )
        .await
        {
            Err(StoreError::NotFound) => return Ok(html(StatusCode::NOT_FOUND, NOT_FOUND)),
            value => value?,
        };
        let settings = self.inner.auth.system_settings().await?;
        let catchup = user.as_ref().is_none_or(|user| {
            user.custom_properties.get("catchup_enabled") != Some(&Value::Bool(false))
        }) && settings.get("catchup_enabled") != Some(&Value::Bool(false));
        let body = render::render(
            &render::Context {
                origin: &origin,
                profile: profile.as_deref(),
                query: &identity.query,
                xc: if xc {
                    Some((
                        identity.parameter("username").unwrap_or(""),
                        identity.parameter("password").unwrap_or(""),
                    ))
                } else {
                    None
                },
                direct,
                restricted_direct: channels.restricted_direct,
                catchup,
                max_bytes: self.inner.config.max_export_bytes,
            },
            &channels.rows,
        )
        .map_err(|error| match error {
            render::Error::Capacity => {
                StoreError::StatusBody(503, json!({"detail":"M3U output capacity exceeded."}))
            }
            _ => StoreError::Unsupported("M3U value formatting is not yet supported"),
        })?;
        let channel_count = if direct && channels.restricted_direct {
            channels
                .rows
                .iter()
                .filter(|channel| channel.direct.is_some())
                .count()
        } else {
            channels.rows.len()
        };
        drop(channels);
        self.set_cache(
            &key,
            Value::String(String::from_utf8(body.clone()).map_err(|_| unavailable())?),
            reservation.memory.clone(),
        )
        .await?;
        let digest = format!("{:x}", Md5::digest(format!("{ip}:{agent}").as_bytes()));
        let event_key = format!(
            "m3u_download:{username}:{}:{}",
            profile.as_deref().unwrap_or("all"),
            &digest[..12]
        );
        if self
            .get_cache(&event_key, reservation.memory.clone())
            .await?
            .is_none_or(|value| !truthy(&value))
        {
            self.event("m3u_download",json!({"profile":profile.as_deref().unwrap_or("all"),"user":username,"channels":channel_count,"client_ip":ip,"user_agent":agent})).await;
            self.set_cache(&event_key, Value::Bool(true), reservation.memory.clone())
                .await?;
        }
        Ok(self.attachment(body, reservation))
    }
}
fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64() != Some(0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(value) => !value.is_empty(),
        Value::Object(value) => !value.is_empty(),
    }
}
// Charge worst-case Vec growth, SQL metadata, cache encoder/Redis command
// copies, incoming body, and a bounded scratch margin BEFORE any I/O/allocation.
// Retained response frames continue to own weighted permits after task cleanup.
fn build_reservation(config: &OutputConfig) -> Option<usize> {
    config
        .max_metadata_bytes
        .checked_mul(2)?
        .checked_add(config.max_export_bytes.checked_mul(6)?)?
        .checked_add(config.max_body_bytes.checked_mul(2)?)?
        .checked_add(1024 * 1024)
}
fn unavailable() -> StoreError {
    StoreError::StatusBody(503, json!({"detail":"M3U output unavailable."}))
}
struct Reservation {
    request: OwnedSemaphorePermit,
    memory: Arc<MemoryReservation>,
}
impl OutputGateway {
    fn attachment(&self, body: Vec<u8>, mut reservation: Reservation) -> Response {
        let length = body.len();
        // Metadata/cache temporaries are gone. Retain only the body allocation,
        // all possible outstanding frame copies, and fixed actor overhead.
        let retained = body
            .capacity()
            .saturating_add(length)
            .saturating_add(64 * 1024);
        if let Some(memory) = Arc::get_mut(&mut reservation.memory) {
            let excess = memory.permit.num_permits().saturating_sub(retained);
            drop(memory.permit.split(excess));
        }
        let (demand, mut receiver) =
            tokio::sync::mpsc::channel::<tokio::sync::oneshot::Sender<Option<Bytes>>>(1);
        let mut stopped = self.inner.stop.subscribe();
        // No data is queued before the response body is polled. An unpolled
        // held body therefore cannot retain allocations/admission after stop.
        tokio::spawn(async move {
            let Reservation {
                request: _request,
                memory,
            } = reservation;
            let mut offset = 0usize;
            while offset < body.len() && !*stopped.borrow() {
                let reply = tokio::select! {value=receiver.recv()=>match value{Some(value)=>value,None=>break},_=stopped.changed()=>break};
                let end = (offset + 64 * 1024).min(body.len());
                let bytes = Bytes::from_owner(GuardedBytes {
                    bytes: body[offset..end].to_vec(),
                    _memory: memory.clone(),
                });
                if reply.send(Some(bytes)).is_err() {
                    break;
                }
                offset = end;
            }
        });
        let stream = futures_util::stream::unfold(demand, |demand| async move {
            let (reply, receive) = tokio::sync::oneshot::channel();
            demand.send(reply).await.ok()?;
            let bytes = receive.await.ok()??;
            Some((Ok::<_, std::convert::Infallible>(bytes), demand))
        });
        let mut response = Body::from_stream(stream).into_response();
        response
            .headers_mut()
            .insert("content-type", "audio/x-mpegurl".parse().unwrap());
        response.headers_mut().insert(
            "content-disposition",
            "attachment; filename=\"channels.m3u\"".parse().unwrap(),
        );
        response
            .headers_mut()
            .insert("content-length", length.to_string().parse().unwrap());
        response
    }
}

fn html(status: StatusCode, body: &'static str) -> Response {
    (status, [("content-type", "text/html; charset=utf-8")], body).into_response()
}
fn grouped_query(query: &[(String, String)]) -> String {
    let mut positions = HashMap::new();
    let mut groups: Vec<Vec<(String, String)>> = Vec::new();
    for (key, value) in query {
        let index = *positions.entry(key).or_insert_with(|| {
            groups.push(Vec::new());
            groups.len() - 1
        });
        groups[index].push((key.clone(), value.clone()));
    }
    render::urlencode(&groups.into_iter().flatten().collect::<Vec<_>>())
}
fn percent_path(value: &str) -> String {
    // Decode URI path bytes once; query-form separators and '+' are literal.
    let input = value.as_bytes();
    let mut output = Vec::with_capacity(input.len());
    let mut offset = 0;
    while offset < input.len() {
        if input[offset] == b'%' && offset + 2 < input.len() {
            let hex = |byte: u8| (byte as char).to_digit(16);
            if let Some((high, low)) = hex(input[offset + 1]).zip(hex(input[offset + 2])) {
                output.push((high * 16 + low) as u8);
                offset += 3;
                continue;
            }
        }
        output.push(input[offset]);
        offset += 1;
    }
    String::from_utf8_lossy(&output).into_owned()
}

pub(crate) fn origin(auth: &AuthService, identity: &PlaybackRequest, _headers: &HeaderMap) -> String {
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
    if trusted {
        if let Some(host) = header("x-forwarded-host") {
            if let Some((host, port)) = host.split_once(':') {
                return if port == standard {
                    format!("{scheme}://{host}")
                } else {
                    format!("{scheme}://{host}:{port}")
                };
            }
            if let Some(port) = header("x-forwarded-port") {
                return if port == standard {
                    format!("{scheme}://{host}")
                } else {
                    format!("{scheme}://{host}:{port}")
                };
            }
            if header("x-forwarded-proto").is_some() {
                return format!("{scheme}://{host}");
            }
        }
    }
    let host = header("host").unwrap_or("localhost");
    if let Some((host, port)) = host.split_once(':') {
        return if port == standard {
            format!("{scheme}://{host}")
        } else {
            format!("{scheme}://{host}:{port}")
        };
    }
    if trusted {
        if let Some(port) = header("x-forwarded-port") {
            return if port == standard {
                format!("{scheme}://{host}")
            } else {
                format!("{scheme}://{host}:{port}")
            };
        }
    }
    format!("{scheme}://{host}")
}
async fn read_body(
    stop: watch::Sender<bool>,
    body: Body,
    limit: usize,
    deadline: Duration,
) -> Result<Vec<u8>, StoreError> {
    let mut stopped = stop.subscribe();
    // subscribe() marks its current version seen. Check that value so a stop
    // arriving after admission but before subscription is not missed.
    if *stopped.borrow() {
        return Err(unavailable());
    }
    tokio::select! {
        value=tokio::time::timeout(deadline,axum::body::to_bytes(body,limit))=>Ok(value.map_err(|_|unavailable())?.map_err(|_|unavailable())?.to_vec()),
        _=stopped.changed()=>Err(unavailable()),
    }
}
async fn dispatch(
    State(engine): State<OutputGateway>,
    OriginalUri(uri): OriginalUri,
    request: Request<Body>,
) -> Response {
    let result = async {
        if engine.inner.stopped.load(Ordering::SeqCst) {
            return Err(unavailable());
        }
        let permit = engine
            .inner
            .requests
            .clone()
            .try_acquire_owned()
            .map_err(|_| unavailable())?;
        let memory = engine
            .inner
            .bytes
            .clone()
            .try_acquire_many_owned(
                build_reservation(&engine.inner.config).ok_or_else(unavailable)? as u32,
            )
            .map_err(|_| unavailable())?;
        let reservation = Reservation {
            request: permit,
            memory: Arc::new(MemoryReservation { permit: memory }),
        };
        let peer = request
            .extensions()
            .get::<ConnectInfo<SocketAddr>>()
            .ok_or_else(unavailable)?
            .0
            .ip();
        let identity = PlaybackRequest {
            channel_id: String::new(),
            peer_ip: peer,
            query: url::form_urlencoded::parse(uri.query().unwrap_or("").as_bytes())
                .into_owned()
                .collect(),
            headers: dispatcharr_contracts::source_headers(
                request
                    .headers()
                    .iter()
                    .map(|(name, value)| (name.as_str(), value.as_bytes())),
            ),
        };
        let headers = request.headers().clone();
        let method = request.method().clone();
        let path = uri.path().to_owned();
        let body = read_body(
            engine.inner.stop.clone(),
            request.into_body(),
            engine.inner.config.max_body_bytes,
            engine.inner.config.build_deadline,
        )
        .await?;
        let owned = engine.clone();
        let deadline = engine.inner.config.build_deadline;
        tokio::spawn(async move {
            tokio::time::timeout(
                deadline,
                owned.response(identity, path, method, headers, body, reservation),
            )
            .await
            .map_err(|_| unavailable())?
        })
        .await
        .map_err(|_| unavailable())?
    }
    .await;
    let mut response = match result {
        Ok(response) => response,
        Err(error) => error.into_response(),
    };
    response
        .headers_mut()
        .insert("x-content-type-options", "nosniff".parse().unwrap());
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn stop_before_body_subscription_does_not_wait_for_slow_input_deadline() {
        let (stop, _) = watch::channel(false);
        stop.send_replace(true);
        let body = Body::from_stream(futures_util::stream::pending::<
            Result<Bytes, std::convert::Infallible>,
        >());
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            read_body(stop, body, 1024, Duration::from_secs(15)),
        )
        .await
        .unwrap();
        assert!(result.is_err());
    }
    #[tokio::test]
    async fn stop_after_subscription_cancels_slow_input() {
        let (stop, _) = watch::channel(false);
        let observed = stop.clone();
        let body = Body::from_stream(futures_util::stream::pending::<
            Result<Bytes, std::convert::Infallible>,
        >());
        let pending = tokio::spawn(read_body(stop, body, 1024, Duration::from_secs(15)));
        tokio::task::yield_now().await;
        observed.send_replace(true);
        assert!(tokio::time::timeout(Duration::from_secs(1), pending)
            .await
            .unwrap()
            .unwrap()
            .is_err());
    }
    #[test]
    fn path_decoding_retains_raw_separators_plus_unicode_and_single_decode() {
        assert_eq!(percent_path("Kids%20&%20News"), "Kids & News");
        assert_eq!(percent_path("Plus+News"), "Plus+News");
        assert_eq!(percent_path("Caf%C3%A9%20%E6%9D%B1%E4%BA%AC"), "Café 東京");
        assert_eq!(percent_path("Café&Plus+"), "Café&Plus+");
        assert_eq!(percent_path("Percent%2526News"), "Percent%26News");
    }
    async fn fixture() -> OutputGateway {
        fixture_config(OutputConfig {
            max_requests: 1,
            ..Default::default()
        })
        .await
    }
    async fn fixture_config(config: OutputConfig) -> OutputGateway {
        assert_eq!(
            std::env::var("M3U_DISPOSABLE_FIXTURE").as_deref(),
            Ok("playback-fixture-only")
        );
        let database = std::env::var("DATABASE_TEST_URL").unwrap();
        let redis = std::env::var("REDIS_TEST_URL").unwrap();
        assert!(
            database.contains("playback-fixture-only@") && redis.contains("playback-fixture-only@")
        );
        let store = Store::connect(&database, 8).await.unwrap();
        let auth = AuthService::new(
            store.clone(),
            dispatcharr_store::AuthConfig::new("playback-fixture-only".into()).unwrap(),
        )
        .with_notification_cache(
            dispatcharr_store::NotificationCache::connect(&redis)
                .await
                .unwrap(),
        );
        OutputGateway::new(store, auth, &redis, config)
            .await
            .unwrap()
    }
    fn request() -> Request<Body> {
        let mut request = Request::builder()
            .uri(format!(
                "/output/m3u/rust-export-profile?_fixture_resource={}",
                uuid::Uuid::new_v4()
            ))
            .header("host", "resource.fixture.invalid")
            .body(Body::empty())
            .unwrap();
        request
            .extensions_mut()
            .insert(ConnectInfo("127.0.0.1:3456".parse::<SocketAddr>().unwrap()));
        request
    }
    async fn call(gateway: OutputGateway, request: Request<Body>) -> Response {
        let uri = request.uri().clone();
        dispatch(State(gateway), OriginalUri(uri), request).await
    }
    #[tokio::test]
    #[ignore = "requires disposable actual Django SQL/Redis export fixture"]
    async fn retained_string_capacity_and_last_emitted_frame_keep_actual_memory_charge() {
        use futures_util::StreamExt;
        let gateway = fixture().await;
        let mut text = String::with_capacity(4096);
        text.push_str("ok");
        let body = text.into_bytes();
        let allocation = body.capacity();
        assert_eq!(body.len(), 2);
        assert!(allocation >= 4096);
        let total = gateway.inner.config.max_memory_bytes;
        let reservation = Reservation {
            request: gateway.inner.requests.clone().try_acquire_owned().unwrap(),
            memory: Arc::new(MemoryReservation {
                permit:
                    gateway
                        .inner
                        .bytes
                        .clone()
                        .try_acquire_many_owned(
                            build_reservation(&gateway.inner.config).unwrap() as u32
                        )
                        .unwrap(),
            }),
        };
        let held = gateway.attachment(body, reservation);
        let charged = allocation + 2 + 64 * 1024;
        assert_eq!(
            gateway.inner.bytes.available_permits(),
            total - charged,
            "retained Vec capacity, not2-byte payload, must be charged"
        );
        let mut stream = held.into_body().into_data_stream();
        let frame = stream.next().await.unwrap().unwrap();
        assert_eq!(&frame[..], b"ok");
        drop(stream);
        tokio::time::timeout(Duration::from_secs(2), async {
            while gateway.inner.requests.available_permits() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            gateway.inner.bytes.available_permits(),
            total - charged,
            "detached final Bytes owner must keep the shared memory guard"
        );
        drop(frame);
        tokio::time::timeout(Duration::from_secs(2), async {
            while gateway.inner.bytes.available_permits() != total {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(gateway.shutdown().await);
    }
    #[tokio::test]
    #[ignore = "requires disposable actual Django SQL/Redis export fixture"]
    async fn cancelled_large_cache_read_retains_memory_until_real_redis_reply_drains() {
        let mut proxy = crate::test_proxy::start().await;
        let mut gateway = fixture().await;
        Arc::get_mut(&mut gateway.inner).unwrap().redis = RedisTransport::connect(
            &proxy.url,
            TransportConfig {
                max_operations: 1,
                max_queued_operations: 1,
                command_timeout: Duration::from_secs(3),
            },
        )
        .await
        .unwrap();
        let capacity = gateway.inner.config.max_memory_bytes;
        let memory = Arc::new(MemoryReservation {
            permit: gateway
                .inner
                .bytes
                .clone()
                .try_acquire_many_owned(capacity as u32)
                .unwrap(),
        });
        let key = format!("m3u_content:guard-test:{}", uuid::Uuid::new_v4());
        gateway
            .set_cache(&key, Value::String("Ω".repeat(1_500_000)), memory.clone())
            .await
            .unwrap();
        proxy.hold.send_replace(true);
        let reader = gateway.clone();
        let guard = memory.clone();
        let task = tokio::spawn(async move { reader.get_cache(&key, guard).await });
        drop(memory);
        crate::test_proxy::observed(&mut proxy).await;
        task.abort();
        let _ = task.await;
        assert_eq!(
            gateway.inner.bytes.available_permits(),
            0,
            "cancelled caller must not release remote operation's allocation budget"
        );
        proxy.hold.send_replace(false);
        tokio::time::timeout(Duration::from_secs(3), async {
            while gateway.inner.bytes.available_permits() != capacity {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(gateway.shutdown().await);
    }
    #[tokio::test]
    #[ignore = "requires disposable actual Django SQL/Redis export fixture"]
    async fn large_held_response_uses_shared_budget_until_drop_then_shutdown_drains() {
        if std::env::var("M3U_LARGE_OUTPUT").as_deref() != Ok("1") {
            return;
        }
        let mut config = OutputConfig {
            max_requests: 2,
            ..Default::default()
        };
        config.max_memory_bytes = build_reservation(&config).unwrap();
        let gateway = fixture_config(config).await;
        let large_request = || {
            let mut value = request();
            *value.uri_mut() = format!(
                "/output/m3u/rust-export-large?_large_resource={}",
                uuid::Uuid::new_v4()
            )
            .parse()
            .unwrap();
            value
        };
        let held = call(gateway.clone(), large_request()).await;
        assert_eq!(held.status(), StatusCode::OK);
        assert!(
            held.headers()["content-length"]
                .to_str()
                .unwrap()
                .parse::<usize>()
                .unwrap()
                > 2 * 1024 * 1024
        );
        assert_eq!(gateway.inner.requests.available_permits(), 1);
        assert!(gateway.inner.bytes.available_permits() < gateway.inner.config.max_memory_bytes);
        // A request slot is free, but a second cold build cannot fit while a
        // large unpolled response owns its independent byte reservation.
        assert_eq!(
            call(gateway.clone(), large_request()).await.status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        drop(held);
        tokio::time::timeout(Duration::from_secs(2), async {
            while gateway.inner.bytes.available_permits() != gateway.inner.config.max_memory_bytes {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let next = call(gateway.clone(), large_request()).await;
        assert_eq!(next.status(), StatusCode::OK);
        assert!(gateway.shutdown().await);
        assert_eq!(
            gateway.inner.bytes.available_permits(),
            gateway.inner.config.max_memory_bytes
        );
        assert!(axum::body::to_bytes(next.into_body(), 8 * 1024 * 1024)
            .await
            .unwrap()
            .is_empty());
    }
    #[tokio::test]
    #[ignore = "requires disposable actual Django SQL/Redis export fixture"]
    async fn unpolled_export_holds_admission_then_shutdown_releases_all_without_client_poll() {
        let gateway = fixture().await;
        let held = call(gateway.clone(), request()).await;
        assert_eq!(held.status(), StatusCode::OK);
        assert_eq!(gateway.inner.requests.available_permits(), 0);
        let rejected = call(gateway.clone(), request()).await;
        assert_eq!(rejected.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            tokio::time::timeout(Duration::from_secs(2), gateway.shutdown())
                .await
                .unwrap()
        );
        assert_eq!(gateway.inner.requests.available_permits(), 1);
        assert_eq!(
            gateway.inner.bytes.available_permits(),
            gateway.inner.config.max_memory_bytes
        );
        let body = axum::body::to_bytes(held.into_body(), 1024 * 1024)
            .await
            .unwrap();
        assert!(
            body.is_empty(),
            "no payload must be queued before body demand"
        );
        assert_eq!(
            call(gateway.clone(), request()).await.status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
    #[tokio::test]
    #[ignore = "requires disposable actual Django SQL/Redis export fixture"]
    async fn dropping_unpolled_export_releases_task_and_memory_and_allows_next_request() {
        let gateway = fixture().await;
        let held = call(gateway.clone(), request()).await;
        assert_eq!(held.status(), StatusCode::OK);
        drop(held);
        tokio::time::timeout(Duration::from_secs(2), async {
            while gateway.inner.requests.available_permits() != 1 {
                tokio::time::sleep(Duration::from_millis(5)).await
            }
        })
        .await
        .unwrap();
        assert_eq!(
            gateway.inner.bytes.available_permits(),
            gateway.inner.config.max_memory_bytes
        );
        let next = call(gateway.clone(), request()).await;
        assert_eq!(next.status(), StatusCode::OK);
        let body = axum::body::to_bytes(next.into_body(), 1024 * 1024)
            .await
            .unwrap();
        assert!(body.starts_with(b"#EXTM3U "));
        assert!(gateway.shutdown().await);
    }
}
