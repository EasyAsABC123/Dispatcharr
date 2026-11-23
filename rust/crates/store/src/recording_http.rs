//! Additive normal RecordingViewSet routes. Original Dispatcharr/Django authors
//! retain all application/serializer/permission/signal/algorithm credit.
//! Artifact only: no Main attachment or acceptance is implied by this module.
use crate::{channels, AuthService, StoreError, User};
use axum::{
    body::{to_bytes, Body, Bytes},
    extract::{ConnectInfo, Request, State},
    http::{Method, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use chrono::{DateTime, Datelike, SecondsFormat, Utc};
use dispatcharr_contracts::{BoxFuture, UpdateEventSink};
use serde_json::{json, Map, Value};
use sqlx::{Connection, Postgres, QueryBuilder, Row};
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

pub(super) mod recurring;
pub use recurring::{
    execute_recurring_purge, preflight_recurring_purge, CONFIGURED_REBUILD_HORIZON_DAYS,
};
pub type RecordingChannelId = Uuid;
pub type RecordingTaskConnection = sqlx::PgConnection;
const SLOTS: usize = 4;
const RAW: usize = 65536;
const RESPONSE: usize = 4 * 1024 * 1024;
const ROWS: i64 = 512;
/// Move-only observed absence from the actual Collector SQL connection.
pub struct RecordingTaskAbsent {
    task: String,
}
impl RecordingTaskAbsent {
    pub(crate) fn from_collector(task: String) -> Self {
        Self { task }
    }
    pub fn task_key(&self) -> &str {
        &self.task
    }
}
#[derive(Clone, Copy, Debug)]
pub enum RecordingEffectError {
    SettledFailure,
    Unsupported,
    Unsettled,
}
/// Concrete job/media ports avoid a Store→job-transport dependency cycle.
/// Implementations reread real rows/publish real commands; no frame mint here.
pub trait RecordingJobs: Send + Sync {
    /// Check actual scheduling capability before any recording mutation/revoke.
    /// Legacy fixture ports retain their declared behavior; Main binds a real guard.
    fn admit_scheduling(
        &self,
        _owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<(), RecordingEffectError>> {
        Box::pin(async { Ok(()) })
    }
    fn supports_task_session(&self) -> bool {
        false
    }
    fn schedule_from_task_on<'a>(
        &'a self,
        _recording: i64,
        _parent: Uuid,
        _revoked_before_date_save: bool,
        _connection: &'a mut RecordingTaskConnection,
        _owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'a, Result<Vec<String>, RecordingEffectError>> {
        Box::pin(async { Err(RecordingEffectError::Unsupported) })
    }
    fn revoke_on<'a>(
        &'a self,
        _task: String,
        _connection: &'a mut RecordingTaskConnection,
        _owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'a, Result<(), RecordingEffectError>> {
        Box::pin(async { Err(RecordingEffectError::Unsupported) })
    }
    fn supports_task_signals(&self) -> bool {
        false
    }
    /// Actual save-signal child of an already claimed task. The default is an
    /// explicit refusal; HTTP callers retain the ordinary root schedule path.
    fn schedule_from_task(
        &self,
        _recording: i64,
        _parent: Uuid,
        _owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Vec<String>, RecordingEffectError>> {
        Box::pin(async { Err(RecordingEffectError::Unsupported) })
    }
    fn schedule(
        &self,
        recording: i64,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<(), RecordingEffectError>>;
    fn supports_recurring_booking(&self) -> bool {
        false
    }
    fn schedule_recurring(
        &self,
        _recording: i64,
        _owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<(), RecordingEffectError>> {
        Box::pin(async { Err(RecordingEffectError::Unsupported) })
    }
    fn supports_recurring_task_booking(&self) -> bool {
        false
    }
    fn schedule_recurring_from_task(
        &self,
        _recording: i64,
        _parent: Uuid,
        _owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Option<Uuid>, RecordingEffectError>> {
        Box::pin(async { Err(RecordingEffectError::Unsupported) })
    }
    fn revoke(
        &self,
        task: String,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<(), RecordingEffectError>>;
    fn revoke_observed_absent(
        &self,
        proof: RecordingTaskAbsent,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<(), RecordingEffectError>>;
    fn stop_clients(
        &self,
        channel: Uuid,
        recording: i64,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<(), RecordingEffectError>>;
    /// Selected AIO supplies the owned local handoff. Other role/adapter
    /// combinations retain their existing immediate stop behavior.
    fn stop_after_local_recording(
        &self,
        _channel: Uuid,
        _recording: i64,
        _owner: Arc<dyn Send + Sync>,
    ) -> Option<BoxFuture<'_, Result<(), RecordingEffectError>>> {
        None
    }
    fn dvr_settings(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Value, RecordingEffectError>>;
    fn cancelled_event(
        &self,
        details: Value,
        channel: Uuid,
        name: String,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<(), RecordingEffectError>>;
}
#[derive(Clone)]
pub struct RecordingHttp {
    state: Arc<Inner>,
}
struct Inner {
    auth: AuthService,
    jobs: Arc<dyn RecordingJobs>,
    events: Arc<dyn UpdateEventSink>,
    slots: Arc<Semaphore>,
    closed: AtomicBool,
    uncertain: Mutex<Vec<Arc<Owner>>>,
    recurring_writer: tokio::sync::Mutex<()>,
}
struct Owner {
    _slot: OwnedSemaphorePermit,
    state: Arc<Inner>,
}
struct Frame {
    bytes: Vec<u8>,
    _owner: Arc<dyn Send + Sync>,
}
impl AsRef<[u8]> for Frame {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
#[derive(Clone)]
struct Recording {
    id: i64,
    channel: i64,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    task: Option<String>,
    properties: Value,
    uuid: Uuid,
    name: String,
}
impl Recording {
    fn value(&self) -> Value {
        json!({"id":self.id,"channel":self.channel,"start_time":iso(self.start),"end_time":iso(self.end),"task_id":self.task,"custom_properties":self.properties})
    }
}
fn iso(time: DateTime<Utc>) -> String {
    time.to_rfc3339_opts(
        if time.timestamp_subsec_micros() == 0 {
            SecondsFormat::Secs
        } else {
            SecondsFormat::Micros
        },
        true,
    )
}
fn python_iso(time: DateTime<Utc>) -> String {
    time.to_rfc3339_opts(
        if time.timestamp_subsec_micros() == 0 {
            SecondsFormat::Secs
        } else {
            SecondsFormat::Micros
        },
        false,
    )
}
fn python_datetime(time: DateTime<Utc>) -> String {
    python_iso(time).replacen('T', " ", 1)
}
fn missing() -> StoreError {
    StoreError::StatusBody(
        404,
        json!({"detail":"No Recording matches the given query."}),
    )
}
fn field(name: &str, message: &str) -> StoreError {
    StoreError::InvalidInput(json!({name:[message]}))
}
fn effect(error: RecordingEffectError) -> StoreError {
    match error {
        RecordingEffectError::Unsupported => {
            StoreError::Unsupported("Recording effect adapter domain unavailable")
        }
        RecordingEffectError::SettledFailure => StoreError::UnhandledRestFault,
        RecordingEffectError::Unsettled => StoreError::StatusBody(
            503,
            json!({"error":"Recording operation physically unsettled"}),
        ),
    }
}
pub(super) fn reply(
    status: u16,
    value: Value,
    owner: Arc<dyn Send + Sync>,
) -> Result<Response, StoreError> {
    let bytes = serde_json::to_vec(&value).map_err(|_| StoreError::UnhandledRestFault)?;
    reply_bytes(status, bytes, owner)
}

fn reply_bytes(
    status: u16,
    bytes: Vec<u8>,
    owner: Arc<dyn Send + Sync>,
) -> Result<Response, StoreError> {
    if bytes.len() > RESPONSE {
        return Err(StoreError::Unsupported("Recording response budget"));
    }
    let length = bytes.len();
    let body = if status == 204 || status == 304 {
        Body::empty()
    } else {
        Body::from(Bytes::from_owner(Frame {
            bytes,
            _owner: owner,
        }))
    };
    let mut response = Response::new(body);
    *response.status_mut() =
        StatusCode::from_u16(status).map_err(|_| StoreError::UnhandledRestFault)?;
    response.headers_mut().insert(
        "content-type",
        "application/json"
            .parse()
            .map_err(|_| StoreError::UnhandledRestFault)?,
    );
    response.headers_mut().insert(
        "content-length",
        if status == 204 {
            "0".parse().map_err(|_| StoreError::UnhandledRestFault)?
        } else {
            length
                .to_string()
                .parse()
                .map_err(|_| StoreError::UnhandledRestFault)?
        },
    );
    Ok(response)
}
impl RecordingHttp {
    pub fn prepare(
        auth: AuthService,
        jobs: Arc<dyn RecordingJobs>,
        events: Arc<dyn UpdateEventSink>,
    ) -> Self {
        Self {
            state: Arc::new(Inner {
                auth,
                jobs,
                events,
                slots: Arc::new(Semaphore::new(SLOTS)),
                closed: AtomicBool::new(false),
                uncertain: Mutex::new(Vec::with_capacity(SLOTS)),
                recurring_writer: tokio::sync::Mutex::new(()),
            }),
        }
    }
    pub fn router(self) -> Router {
        Router::new()
            .route("/api/channels/recordings/", any(handle))
            .route("/api/channels/recordings/{*tail}", any(handle))
            .route(
                recurring::COLLECTION,
                axum::routing::get(recurring::collection),
            )
            .route(recurring::PATH, any(recurring::handle))
            .with_state(self)
    }
    /// Keep one pair of outer paths: matchit's sibling catchall cannot merge
    /// with a separate dynamic playback subtree. Main supplies the same Auth.
    /// Playback's own authentication/STREAMS/DVR checks run before manage/UI.
    pub fn router_with_playback(self, playback: crate::RecordingPlayback) -> Router {
        Router::new()
            .route("/api/channels/recordings/", any(handle_with_playback))
            .route(
                "/api/channels/recordings/{*tail}",
                any(handle_with_playback),
            )
            .route(
                recurring::COLLECTION,
                axum::routing::get(recurring::collection_with_playback),
            )
            .route(recurring::PATH, any(recurring::with_playback))
            .with_state((self, playback))
    }
    /// Keep management, playback and manual Comskip beneath the same outer
    /// catchall. Matching a separate dynamic Comskip subtree panics in matchit.
    pub fn router_with_playback_and_comskip(
        self,
        playback: crate::RecordingPlayback,
        comskip: crate::comskip_http::ComskipHttp,
    ) -> Router {
        self.router_with_playback(playback)
            .layer(axum::Extension(comskip))
    }
    pub fn close_admission(&self) {
        self.state.closed.store(true, Ordering::Release);
        self.state.slots.close();
    }
    pub async fn shutdown(&self, timeout: Duration) -> bool {
        self.close_admission();
        let deadline = tokio::time::Instant::now() + timeout;
        while self.state.slots.available_permits() != SLOTS {
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        true
    }
}
async fn handle_with_playback(
    State((api, playback)): State<(RecordingHttp, crate::RecordingPlayback)>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Response {
    let eligible = matches!(*request.method(), Method::GET | Method::HEAD)
        && request
            .uri()
            .path()
            .strip_prefix("/api/channels/recordings/")
            .and_then(|tail| tail.split_once('/'))
            .is_some_and(|(id, action)| {
                !id.is_empty()
                    && (action == "file/"
                        || action
                            .strip_prefix("hls/")
                            .is_some_and(|segment| !segment.is_empty()))
            });
    if eligible {
        // No management admission, account_user UI policy or body buffering here.
        // The actual playback service enforces its original Auth4/SQL/file ports.
        playback.dispatch(peer, request).await
    } else {
        handle(State(api), ConnectInfo(peer), request).await
    }
}
async fn handle(
    State(api): State<RecordingHttp>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Response {
    // Select only the canonical action; the delegated service retains its own
    // admission/Auth/SQL/publication and shutdown owner without buffering twice.
    let comskip_id = request
        .uri()
        .path()
        .strip_prefix("/api/channels/recordings/")
        .and_then(|tail| tail.strip_suffix("/comskip/"))
        .filter(|id| !id.is_empty() && !id.contains('/'))
        .map(str::to_owned);
    if let Some(id) = comskip_id {
        if let Some(comskip) = request
            .extensions()
            .get::<crate::comskip_http::ComskipHttp>()
            .cloned()
        {
            return comskip.dispatch(id, peer, request).await;
        }
    }
    let owner = match admit(&api) {
        Ok(owner) => owner,
        Err(error) => return error.into_response(),
    };
    let method = request.method().clone();
    let uri = request.uri().clone();
    let headers = request.headers().clone();
    let body = match to_bytes(request.into_body(), RAW).await {
        Ok(v) => v,
        Err(_) => {
            return StoreError::StatusBody(413, json!({"detail":"Request body too large."}))
                .into_response()
        }
    };
    // Observer loss never drops an SQL write or background file/control owner.
    match tokio::spawn(async move {
        execute(owner, peer, method, uri.path().to_owned(), headers, body).await
    })
    .await
    {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => error.into_response(),
        Err(_) => StoreError::UnhandledRestFault.into_response(),
    }
}
fn admit(api: &RecordingHttp) -> Result<Arc<Owner>, StoreError> {
    if api.state.closed.load(Ordering::Acquire) {
        return Err(StoreError::StatusBody(
            503,
            json!({"error":"Recording intake closed"}),
        ));
    }
    let Ok(slot) = api.state.slots.clone().try_acquire_owned() else {
        return Err(StoreError::StatusBody(
            503,
            json!({"error":"Recording request capacity"}),
        ));
    };
    Ok(Arc::new(Owner {
        _slot: slot,
        state: api.state.clone(),
    }))
}
pub(super) fn access(user: &User, manage: bool) -> Result<(), StoreError> {
    if user.user_level >= 10 {
        return Ok(());
    }
    if user.user_level < 1 {
        return Err(StoreError::PermissionDenied);
    }
    let props = &user.custom_properties;
    let falsy = matches!(props, Value::Null | Value::Bool(false))
        || props.as_str() == Some("")
        || props.as_f64() == Some(0.0)
        || props.as_array().is_some_and(Vec::is_empty);
    if !falsy && !props.is_object() {
        return Err(StoreError::UnhandledRestFault);
    }
    let raw_value = props.get("dvr_access");
    if raw_value.is_some_and(|v| v.is_array() || v.is_object()) {
        return Err(StoreError::UnhandledRestFault);
    }
    let raw = raw_value.and_then(Value::as_str);
    let level = match raw {
        Some("none") => "none",
        Some("manage") => "manage",
        _ => "view",
    };
    if level == "none" || (manage && level != "manage") {
        return Err(StoreError::PermissionDenied);
    }
    Ok(())
}
async fn execute(
    owner: Arc<Owner>,
    peer: SocketAddr,
    method: Method,
    path: String,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Result<Response, StoreError> {
    let auth = owner.state.auth.clone().with_retained_scope(owner.clone());
    let user = auth.account_user(peer, headers, false).await?;
    let tail = path
        .strip_prefix("/api/channels/recordings/")
        .ok_or(StoreError::NotFound)?;
    let parts = tail
        .trim_end_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>();
    let read = (method == Method::GET || method == Method::HEAD) && parts.len() <= 1;
    access(&user, !read)?;
    if method == Method::OPTIONS {
        return Err(StoreError::Unsupported(
            "Recording serializer metadata action is not migrated",
        ));
    }
    if parts.is_empty() {
        if method == Method::GET || method == Method::HEAD {
            return reply(200, list_value(&auth, &user).await?, owner);
        }
        if method == Method::POST {
            return save(owner, auth, user, None, body, false).await;
        }
    } else {
        let id = parts[0]
            .parse::<i64>()
            .ok()
            .filter(|id| *id > 0 && *id <= i64::from(i32::MAX))
            .ok_or_else(missing)?;
        let row = list(&auth, &user, Some(id))
            .await?
            .into_iter()
            .next()
            .ok_or_else(missing)?;
        match (parts.get(1).copied(), method.as_str()) {
            (None, "GET" | "HEAD") => return reply(200, row.value(), owner),
            (None, "PATCH" | "PUT") => {
                return save(owner, auth, user, Some(row), body, method == Method::PATCH).await
            }
            (None, "DELETE") => return destroy(owner, row, &user).await,
            (Some("stop"), "POST") => return stop(owner, row).await,
            (Some("extend"), "POST") => return extend(owner, row, body).await,
            (Some(_), _) => {
                return Err(StoreError::Unsupported("Recording action is not migrated"))
            }
            _ => {}
        }
    }
    Err(StoreError::StatusBody(
        405,
        json!({"detail":format!("Method \"{method}\" not allowed.")}),
    ))
}
async fn settle<T>(
    mut connection: sqlx::pool::PoolConnection<Postgres>,
    result: Result<T, StoreError>,
) -> Result<T, StoreError> {
    let ready = connection.ping().await;
    let closed = connection.close().await;
    if let Err(e) = ready {
        return Err(StoreError::Database(e));
    }
    if let Err(e) = closed {
        return Err(StoreError::Database(e));
    }
    result
}
pub(super) async fn list_value(auth: &AuthService, user: &User) -> Result<Value, StoreError> {
    let values = list(auth, user, None).await?;
    Ok(Value::Array(values.iter().map(Recording::value).collect()))
}

async fn list(
    auth: &AuthService,
    user: &User,
    id: Option<i64>,
) -> Result<Vec<Recording>, StoreError> {
    let mut connection = auth.store.pool().acquire().await?;
    let result=async{
        let mut q=QueryBuilder::<Postgres>::new("SELECT r.id::bigint AS id,r.channel_id::bigint AS channel,r.start_time,r.end_time,CASE WHEN octet_length(r.task_id)<=255 THEN r.task_id ELSE NULL END AS task_id,CASE WHEN r.custom_properties IS NULL THEN 'null'::jsonb WHEN octet_length(r.custom_properties::text)<=8192 THEN r.custom_properties ELSE NULL END AS properties,r.custom_properties IS NULL OR octet_length(r.custom_properties::text)<=8192 AS bounded,c.uuid,CASE WHEN octet_length(c.name)<=255 THEN c.name ELSE NULL END AS name FROM dispatcharr_channels_recording r JOIN dispatcharr_channels_channel c ON c.id=r.channel_id WHERE true");
        if let Some(id)=id{q.push(" AND r.id=").push_bind(id);}channels::scope(&mut q,user);q.push(" ORDER BY r.id LIMIT ").push_bind(ROWS+1);
        let rows=q.build().fetch_all(&mut *connection).await?;if rows.len()>ROWS as usize{return Err(StoreError::Unsupported("Recording list budget"));}
        let mut result=Vec::with_capacity(rows.len());for row in rows{
            if !row.try_get::<bool,_>("bounded")?{return Err(StoreError::Unsupported("Recording property budget"));}
            result.push(Recording{id:row.try_get("id")?,channel:row.try_get("channel")?,start:row.try_get("start_time")?,end:row.try_get("end_time")?,task:row.try_get("task_id")?,properties:row.try_get::<Option<Value>,_>("properties")?.ok_or(StoreError::Unsupported("Recording property representation"))?,uuid:row.try_get("uuid")?,name:row.try_get::<Option<String>,_>("name")?.ok_or(StoreError::Unsupported("Recording channel-name budget"))?});
        }Ok(result)
    }.await;
    settle(connection, result).await
}
fn python_int(value: Option<&Value>) -> Result<Option<i64>, StoreError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if let Some(v) = value.as_i64() {
        return Ok(Some(v));
    }
    if let Some(v) = value.as_bool() {
        return Ok(Some(i64::from(v)));
    }
    if let Some(v) = value.as_f64() {
        if v.is_finite() && v.trunc() >= i64::MIN as f64 && v.trunc() < (i64::MAX as f64) {
            return Ok(Some(v.trunc() as i64));
        }
        return Err(StoreError::Unsupported("Python arbitrary integer range"));
    }
    let Some(raw) = value.as_str() else {
        return Ok(None);
    };
    let raw = raw.trim();
    let digits = raw.strip_prefix(['+', '-']).unwrap_or(raw);
    if digits.chars().any(|v| !v.is_ascii() && v.is_numeric()) {
        return Err(StoreError::Unsupported("Python Unicode decimal integer"));
    }
    if digits.is_empty()
        || digits
            .split('_')
            .any(|part| part.is_empty() || !part.bytes().all(|v| v.is_ascii_digit()))
    {
        return Ok(None);
    }
    raw.replace('_', "")
        .parse::<i64>()
        .map(Some)
        .map_err(|_| StoreError::Unsupported("Python arbitrary integer range"))
}
fn data(body: &[u8]) -> Result<Map<String, Value>, StoreError> {
    serde_json::from_slice::<Value>(body)
        .map_err(|_| StoreError::InvalidInput(json!({"detail":"JSON parse error"})))?
        .as_object()
        .cloned()
        .ok_or_else(|| {
            StoreError::InvalidInput(
                json!({"non_field_errors":["Invalid data. Expected a dictionary."]}),
            )
        })
}
async fn save(
    owner: Arc<Owner>,
    auth: AuthService,
    user: User,
    existing: Option<Recording>,
    body: Bytes,
    partial: bool,
) -> Result<Response, StoreError> {
    let mut input = data(&body)?;
    let mut errors = Map::new();
    for name in ["channel", "start_time", "end_time"] {
        if !input.contains_key(name) {
            if let Some(old) = &existing {
                if partial {
                    input.insert(name.into(), old.value()[name].clone());
                    continue;
                }
            }
            errors.insert(name.into(), json!(["This field is required."]));
        }
    }
    if !errors.is_empty() {
        return Err(StoreError::InvalidInput(Value::Object(errors)));
    }
    let channel = crate::channel_memberships::primary_key(&input["channel"])?
        .ok_or_else(|| field("channel", "Invalid pk - object does not exist."))?;
    let mut connection = auth.store.pool().acquire().await?;
    let exists = async {
        let mut q = QueryBuilder::<Postgres>::new(
            "SELECT c.id::bigint FROM dispatcharr_channels_channel c WHERE c.id=",
        );
        q.push_bind(channel);
        channels::scope(&mut q, &user);
        Ok(q.build_query_scalar::<i64>()
            .fetch_optional(&mut *connection)
            .await?
            .is_some())
    }
    .await;
    if !settle(connection, exists).await? {
        return Err(field(
            "channel",
            &format!("Invalid pk \"{channel}\" - object does not exist."),
        ));
    }
    let mut start = crate::admin_writes::parse_datetime("start_time", &input["start_time"], false)?
        .ok_or_else(|| field("start_time", "This field may not be null."))?;
    let mut end = crate::admin_writes::parse_datetime("end_time", &input["end_time"], false)?
        .ok_or_else(|| field("end_time", "This field may not be null."))?;
    let mut properties = input.get("custom_properties").cloned().unwrap_or_else(|| {
        existing
            .as_ref()
            .map_or_else(|| json!({}), |r| r.properties.clone())
    });
    if !properties.is_null() && !properties.is_object() {
        return Err(field(
            "custom_properties",
            "custom_properties must be an object.",
        ));
    }
    if let Some(props) = properties.as_object_mut() {
        for key in [
            "file_path",
            "_hls_dir",
            "file_name",
            "file_url",
            "output_file_url",
        ] {
            props.remove(key);
            if let Some(value) = existing.as_ref().and_then(|r| r.properties.get(key)) {
                props.insert(key.into(), value.clone());
            }
        }
    }
    if properties.get("program").is_some_and(Value::is_object) {
        let dvr = match owner.state.jobs.dvr_settings(owner.clone()).await {
            Ok(value) => value,
            Err(RecordingEffectError::SettledFailure) => json!({}),
            Err(error) => return Err(effect(error)),
        };
        for (key, time, subtract) in [
            ("pre_offset_minutes", &mut start, true),
            ("post_offset_minutes", &mut end, false),
        ] {
            let value = python_int(dvr.get(key)).ok().flatten().unwrap_or(0);
            if value > 0 {
                // Source catches timedelta/date overflow and leaves that side
                // unchanged; arbitrary supported offsets are not permission ids.
                if let Some(delta) = chrono::Duration::try_minutes(value) {
                    let shifted = if subtract {
                        time.checked_sub_signed(delta)
                    } else {
                        time.checked_add_signed(delta)
                    };
                    if let Some(shifted) = shifted.filter(|v| (1..=9999).contains(&v.year())) {
                        *time = shifted;
                    }
                }
            }
        }
    }
    let now = Utc::now();
    if end < now {
        return Err(StoreError::InvalidInput(
            json!({"non_field_errors":["End time must be in the future."]}),
        ));
    }
    if start < now {
        start = now;
    }
    if end <= start {
        return Err(StoreError::InvalidInput(
            json!({"non_field_errors":["End time must be after start time."]}),
        ));
    }
    // Authenticate, scope and validate first; refuse unsupported execution
    // before revoking an old task or mutating recording/Beat/broker state.
    owner
        .state
        .jobs
        .admit_scheduling(owner.clone())
        .await
        .map_err(effect)?;
    let mut task = existing.as_ref().and_then(|r| r.task.clone());
    if let Some(old) = &existing {
        if (old.channel != channel || old.start != start || old.end != end)
            && old.properties.get("status").and_then(Value::as_str) != Some("recording")
        {
            if let Some(key) = &task {
                owner
                    .state
                    .jobs
                    .revoke(key.clone(), owner.clone())
                    .await
                    .map_err(effect)?;
                task = None;
            }
        }
    }
    if serde_json::to_vec(&properties)
        .map_err(|_| StoreError::UnhandledRestFault)?
        .len()
        > 8192
    {
        return Err(StoreError::Unsupported("Recording property budget"));
    }
    let mut connection = auth.store.pool().acquire().await?;
    let result=async{let id=if let Some(old)=&existing{sqlx::query_scalar::<_,i64>("UPDATE dispatcharr_channels_recording SET channel_id=$2,start_time=$3,end_time=$4,custom_properties=$5,task_id=$6 WHERE id=$1 RETURNING id::bigint").bind(old.id).bind(channel).bind(start).bind(end).bind(&properties).bind(task).fetch_optional(&mut *connection).await?.ok_or_else(missing)?}
        else{sqlx::query_scalar::<_,i64>("INSERT INTO dispatcharr_channels_recording(channel_id,start_time,end_time,custom_properties,task_id) VALUES($1,$2,$3,$4,NULL) RETURNING id::bigint").bind(channel).bind(start).bind(end).bind(&properties).fetch_one(&mut *connection).await?};Ok(id)}.await;
    let id = settle(connection, result).await?;
    // Source post_save catches known settled schedule/artwork errors; pending
    // physical publication is never silently converted into HTTP acceptance.
    match owner.state.jobs.schedule(id, owner.clone()).await {
        Ok(()) | Err(RecordingEffectError::SettledFailure) => {}
        Err(error) => return Err(effect(error)),
    }
    let row = list(&auth, &user, Some(id))
        .await?
        .into_iter()
        .next()
        .ok_or_else(missing)?;
    reply(
        if existing.is_some() { 200 } else { 201 },
        row.value(),
        owner,
    )
}
async fn publish(owner: &Arc<Owner>, payload: Value) {
    let _ = owner
        .state
        .events
        .send_retained(
            "updates",
            json!({"type":"update","data":payload}),
            owner.clone(),
        )
        .await;
}
async fn patch_properties(
    owner: &Arc<Owner>,
    row: &Recording,
    properties: Value,
) -> Result<(), StoreError> {
    let mut c = owner.state.auth.store.pool().acquire().await?;
    let result=sqlx::query("UPDATE dispatcharr_channels_recording SET custom_properties=$2 WHERE id=$1 AND custom_properties IS NOT DISTINCT FROM $3").bind(row.id).bind(properties).bind(&row.properties).execute(&mut *c).await.map_err(StoreError::from).and_then(|r|if r.rows_affected()==1{Ok(())}else{Err(StoreError::StatusBody(409,json!({"detail":"Recording changed during operation."})))});
    settle(c, result).await
}
fn object(value: &Value) -> Result<Map<String, Value>, StoreError> {
    if !channels::truthy(Some(value)) {
        Ok(Map::new())
    } else {
        value
            .as_object()
            .cloned()
            .ok_or(StoreError::UnhandledRestFault)
    }
}
async fn stop_effects(
    jobs: &dyn RecordingJobs,
    channel: Uuid,
    recording: i64,
    task: Option<String>,
    owner: Arc<dyn Send + Sync>,
) -> bool {
    let revoke = async {
        match task {
            Some(key) => jobs.revoke(key, owner.clone()).await,
            None => Ok(()),
        }
    };
    let (clients, revoke) = match jobs.stop_after_local_recording(channel, recording, owner.clone())
    {
        Some(handoff) => tokio::join!(handoff, revoke),
        None => (
            jobs.stop_clients(channel, recording, owner.clone()).await,
            revoke.await,
        ),
    };
    matches!(clients, Err(RecordingEffectError::Unsettled))
        || matches!(revoke, Err(RecordingEffectError::Unsettled))
}

async fn stop(owner: Arc<Owner>, row: Recording) -> Result<Response, StoreError> {
    let status = row
        .properties
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("");
    if matches!(status, "completed" | "interrupted" | "failed") {
        return reply(
            409,
            json!({"success":false,"error":format!("Recording is already {status}")}),
            owner,
        );
    }
    let mut cp = object(&row.properties)?;
    cp.insert("status".into(), json!("stopped"));
    cp.insert("stopped_at".into(), json!(python_datetime(Utc::now())));
    patch_properties(&owner, &row, Value::Object(cp)).await?;
    publish(
        &owner,
        json!({"success":true,"type":"recording_stopped","channel":row.name}),
    )
    .await;
    let held = owner.clone();
    tokio::spawn(async move {
        if stop_effects(
            held.state.jobs.as_ref(),
            row.uuid,
            row.id,
            row.task,
            held.clone(),
        )
        .await
        {
            held.state.closed.store(true, Ordering::Release);
            held.state
                .uncertain
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(held.clone());
        }
    });
    reply(200, json!({"success":true,"status":"stopped"}), owner)
}
async fn extend(owner: Arc<Owner>, row: Recording, body: Bytes) -> Result<Response, StoreError> {
    if matches!(
        row.properties.get("status").and_then(Value::as_str),
        Some("completed" | "stopped" | "interrupted")
    ) {
        return reply(
            400,
            json!({"success":false,"error":"Recording has already finished"}),
            owner,
        );
    }
    let values = data(&body)?;
    let minutes = python_int(values.get("extra_minutes"))?.unwrap_or(0);
    if minutes <= 0 {
        return reply(
            400,
            json!({"success":false,"error":"extra_minutes must be a positive integer"}),
            owner,
        );
    }
    let new_end = row
        .end
        .checked_add_signed(
            chrono::Duration::try_minutes(minutes)
                .ok_or(StoreError::Unsupported("Recording extension duration"))?,
        )
        .ok_or(StoreError::Unsupported("Recording extension datetime"))?;
    let mut c = owner.state.auth.store.pool().acquire().await?;
    let result = sqlx::query("UPDATE dispatcharr_channels_recording SET end_time=$2 WHERE id=$1")
        .bind(row.id)
        .bind(new_end)
        .execute(&mut *c)
        .await
        .map_err(StoreError::from)
        .map(|_| ());
    settle(c, result).await?;
    publish(&owner,json!({"success":true,"type":"recording_extended","recording_id":row.id,"new_end_time":python_iso(new_end),"extra_minutes":minutes,"channel":row.name})).await;
    reply(
        200,
        json!({"success":true,"new_end_time":python_iso(new_end)}),
        owner,
    )
}
async fn destroy(owner: Arc<Owner>, row: Recording, user: &User) -> Result<Response, StoreError> {
    let status = row
        .properties
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("");
    if status == "recording"
        || (status == "stopped" && row.properties.get("remux_success").is_none())
    {
        let details = json!({"recording_id":row.id,"outcome":"cancelled","has_file":false,"failure_reason":null,"status":"cancelled","interrupted_reason":null,"file_path":null,"file_name":null,"file_url":null,"file_size":null,"remux_success":false,"bytes_written":null,"cancelled_by":if user.username.is_empty(){None}else{Some(user.username.as_str())},"cancelled_by_id":user.id,"start_time":python_iso(row.start),"end_time":python_iso(row.end)});
        match owner
            .state
            .jobs
            .cancelled_event(details, row.uuid, row.name.clone(), owner.clone())
            .await
        {
            Ok(()) | Err(RecordingEffectError::SettledFailure) => {}
            Err(error) => return Err(effect(error)),
        }
    }
    let mut c = owner.state.auth.store.pool().acquire().await?;
    let result = async {
        sqlx::query("BEGIN").execute(&mut *c).await?;
        sqlx::query("DELETE FROM dispatcharr_channels_recording WHERE id=$1")
            .bind(row.id)
            .execute(&mut *c)
            .await?;
        // Django post_delete runs inside Collector's Recording transaction.
        // Nested PT/clocked cleanup therefore joins this SAME connection.
        if let Some(key) = &row.task {
            if !delete_periodic_in_collector(&mut c, key).await? {
                owner
                    .state
                    .jobs
                    .revoke_observed_absent(
                        RecordingTaskAbsent { task: key.clone() },
                        owner.clone(),
                    )
                    .await
                    .map_err(effect)?;
            }
        }
        sqlx::query("COMMIT").execute(&mut *c).await?;
        Ok(())
    }
    .await;
    if result.is_err() {
        let _ = sqlx::query("ROLLBACK").execute(&mut *c).await;
    }
    settle(c, result).await?;
    publish(&owner,json!({"success":true,"type":"recording_cancelled","recording_id":row.id,"channel":row.name,"was_in_progress":status=="recording"})).await;
    let held = owner.clone();
    tokio::spawn(async move {
        let mut unsettled = false;
        if matches!(
            row.properties.get("status").and_then(Value::as_str),
            Some("recording" | "stopped")
        ) && matches!(
            held.state
                .jobs
                .stop_clients(row.uuid, row.id, held.clone())
                .await,
            Err(RecordingEffectError::Unsettled)
        ) {
            unsettled = true;
        }
        let files = row.properties.clone();
        let physical = held.clone();
        let result = tokio::task::spawn_blocking(move || {
            let _owner = physical;
            super::recording_http_files::remove_recording(&files)
        })
        .await;
        if !matches!(result, Ok(Ok(()))) {
            // Every filesystem syscall/thread has returned. Source catches
            // cleanup failures; report them without inventing absence or pinning
            // a finished physical operation forever.
            tracing::warn!("Recording file cleanup failed after physical settlement");
        }
        if unsettled {
            held.state.closed.store(true, Ordering::Release);
            held.state
                .uncertain
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(held.clone());
        }
    });
    reply(204, Value::Null, owner)
}

/// Nested post_delete uses the actual Collector transaction, not a second
/// independent autocommit cleanup that could survive a Recording rollback.
pub(crate) async fn delete_periodic_in_collector(
    c: &mut sqlx::PgConnection,
    key: &str,
) -> Result<bool, StoreError> {
    let row:Option<(i64,Option<i64>)>=sqlx::query_as("SELECT id::bigint,clocked_id::bigint FROM django_celery_beat_periodictask WHERE name=$1 LIMIT 1").bind(key).fetch_optional(&mut *c).await?;
    let Some((id, clocked)) = row else {
        return Ok(false);
    };
    sqlx::query("SAVEPOINT native_recording_predelete")
        .execute(&mut *c)
        .await?;
    tracking(c).await?;
    sqlx::query("RELEASE SAVEPOINT native_recording_predelete")
        .execute(&mut *c)
        .await?;
    sqlx::query("UPDATE epg_epgsource SET refresh_task_id=NULL WHERE refresh_task_id=$1")
        .bind(id)
        .execute(&mut *c)
        .await?;
    sqlx::query("UPDATE m3u_m3uaccount SET refresh_task_id=NULL WHERE refresh_task_id=$1")
        .bind(id)
        .execute(&mut *c)
        .await?;
    let deleted =
        sqlx::query("DELETE FROM django_celery_beat_periodictask WHERE id=$1 AND name=$2")
            .bind(id)
            .bind(key)
            .execute(&mut *c)
            .await?;
    if deleted.rows_affected() != 1 {
        return Err(StoreError::UnhandledRestFault);
    }
    // PeriodicTask.delete override calls changed() independently, but still
    // joins this caller's outer atomic transaction through update_or_create.
    sqlx::query("SAVEPOINT native_recording_override")
        .execute(&mut *c)
        .await?;
    tracking(c).await?;
    sqlx::query("RELEASE SAVEPOINT native_recording_override")
        .execute(&mut *c)
        .await?;
    if let Some(clocked) = clocked {
        let deleted=sqlx::query("DELETE FROM django_celery_beat_clockedschedule WHERE id=$1 AND NOT EXISTS(SELECT 1 FROM django_celery_beat_periodictask WHERE clocked_id=$1)").bind(clocked).execute(&mut *c).await?;
        if deleted.rows_affected() == 1 {
            sqlx::query("SAVEPOINT native_recording_clocked_postdelete")
                .execute(&mut *c)
                .await?;
            tracking(c).await?;
            sqlx::query("RELEASE SAVEPOINT native_recording_clocked_postdelete")
                .execute(&mut *c)
                .await?;
        }
    }
    Ok(true)
}
async fn tracking(c: &mut sqlx::PgConnection) -> Result<(), StoreError> {
    sqlx::query("INSERT INTO django_celery_beat_periodictasks(ident,last_update) VALUES(1,$1) ON CONFLICT(ident) DO UPDATE SET last_update=EXCLUDED.last_update").bind(Utc::now()).execute(c).await?;
    Ok(())
}

// Early REST Auth clones and the later selected recording graph share the same
// real port. Binding is single-assignment and creates no SQL/broker resources.
impl crate::AuthService {
    pub fn bind_recording_jobs(
        &self,
        jobs: Arc<dyn RecordingJobs>,
    ) -> Result<(), crate::StoreError> {
        // Core/health readers may retain an earlier Auth clone. A strong port
        // here would close Auth -> Bridge -> Engine -> Core/health -> Auth.
        self.recording_jobs
            .set(Arc::downgrade(&jobs))
            .map_err(|_| crate::StoreError::Unsupported("Recording jobs already composed"))
    }
    /// Upgrade once before accepted HTTP effects. Legacy constructors are
    /// genuinely unbound; an expired normal graph is unavailable, never a
    /// silent fallback to the legacy no-Recording deletion implementation.
    pub(crate) fn recording_collector_jobs(
        &self,
    ) -> Result<Option<Arc<dyn RecordingJobs>>, crate::StoreError> {
        self.recording_jobs
            .get()
            .map(|port| {
                port.upgrade().ok_or_else(|| {
                    crate::StoreError::StatusBody(
                        503,
                        json!({"detail":"Normal recording Collector unavailable."}),
                    )
                })
            })
            .transpose()
    }
}

#[cfg(test)]
mod recording_jobs_binding_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[derive(Default)]
    struct StopProbe {
        revoked: tokio::sync::Notify,
        pending_revoke_completed: std::sync::atomic::AtomicBool,
    }

    struct BackEdgeProbe {
        // Represents the real Engine -> Core/health -> pre-engine Auth edge.
        _pre_engine_auth: AuthService,
        dropped: Arc<AtomicUsize>,
        stop: Option<Arc<StopProbe>>,
    }
    impl Drop for BackEdgeProbe {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::AcqRel);
        }
    }
    impl RecordingJobs for BackEdgeProbe {
        fn schedule(
            &self,
            _: i64,
            _: Arc<dyn Send + Sync>,
        ) -> BoxFuture<'_, Result<(), RecordingEffectError>> {
            Box::pin(async { Err(RecordingEffectError::Unsupported) })
        }
        fn revoke(
            &self,
            task: String,
            _: Arc<dyn Send + Sync>,
        ) -> BoxFuture<'_, Result<(), RecordingEffectError>> {
            Box::pin(async move {
                let Some(stop) = self.stop.as_ref() else {
                    return Err(RecordingEffectError::Unsupported);
                };
                assert_eq!(task, "dvr-recording-17");
                stop.pending_revoke_completed.store(true, Ordering::Release);
                stop.revoked.notify_one();
                Ok(())
            })
        }
        fn revoke_observed_absent(
            &self,
            _: RecordingTaskAbsent,
            _: Arc<dyn Send + Sync>,
        ) -> BoxFuture<'_, Result<(), RecordingEffectError>> {
            Box::pin(async { Err(RecordingEffectError::Unsupported) })
        }
        fn stop_clients(
            &self,
            _: Uuid,
            _: i64,
            _: Arc<dyn Send + Sync>,
        ) -> BoxFuture<'_, Result<(), RecordingEffectError>> {
            Box::pin(async { Err(RecordingEffectError::Unsupported) })
        }
        fn stop_after_local_recording(
            &self,
            channel: Uuid,
            recording: i64,
            _: Arc<dyn Send + Sync>,
        ) -> Option<BoxFuture<'_, Result<(), RecordingEffectError>>> {
            let stop = self.stop.as_ref()?;
            Some(Box::pin(async move {
                assert_eq!((channel, recording), (Uuid::nil(), 17));
                stop.revoked.notified().await;
                assert!(stop.pending_revoke_completed.load(Ordering::Acquire));
                Ok(())
            }))
        }
        fn dvr_settings(
            &self,
            _: Arc<dyn Send + Sync>,
        ) -> BoxFuture<'_, Result<Value, RecordingEffectError>> {
            Box::pin(async { Err(RecordingEffectError::Unsupported) })
        }
        fn cancelled_event(
            &self,
            _: Value,
            _: Uuid,
            _: String,
            _: Arc<dyn Send + Sync>,
        ) -> BoxFuture<'_, Result<(), RecordingEffectError>> {
            Box::pin(async { Err(RecordingEffectError::Unsupported) })
        }
    }
    fn unbound_auth() -> AuthService {
        AuthService::new(
            crate::Store::from_pool(
                sqlx::postgres::PgPoolOptions::new()
                    .max_connections(1)
                    .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
                    .unwrap(),
            ),
            crate::AuthConfig::new("owned-cycle-probe-signing-key".into()).unwrap(),
        )
    }
    #[tokio::test]
    async fn pending_revoke_completes_while_local_recorder_handoff_waits() {
        let state = Arc::new(StopProbe::default());
        let jobs = BackEdgeProbe {
            _pre_engine_auth: unbound_auth(),
            dropped: Arc::new(AtomicUsize::new(0)),
            stop: Some(state.clone()),
        };
        // A serial apply-wait-revoke call stalls forever under this causal
        // fixture. Notify is fired only by the real stop-effect revoke arm.
        let unsettled = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            stop_effects(
                &jobs,
                Uuid::nil(),
                17,
                Some("dvr-recording-17".into()),
                Arc::new(()),
            ),
        )
        .await
        .expect("pending revoke must not wait for the recorder handoff");
        assert!(!unsettled);
        assert!(state.pending_revoke_completed.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn actual_binding_has_no_auth_backedge_cycle_but_accepted_lookup_retains_bridge() {
        let auth = unbound_auth();
        let early = auth.clone();
        assert!(early.recording_collector_jobs().unwrap().is_none());
        let dropped = Arc::new(AtomicUsize::new(0));
        let bridge: Arc<dyn RecordingJobs> = Arc::new(BackEdgeProbe {
            _pre_engine_auth: early.clone(),
            dropped: dropped.clone(),
            stop: None,
        });
        let weak = Arc::downgrade(&bridge);
        auth.bind_recording_jobs(bridge.clone()).unwrap();
        let accepted = early.recording_collector_jobs().unwrap().unwrap();
        assert!(Arc::ptr_eq(&bridge, &accepted));
        drop(bridge);
        assert!(
            weak.upgrade().is_some(),
            "accepted operation lost its actual bridge"
        );
        assert_eq!(dropped.load(Ordering::Acquire), 0);
        drop(accepted);
        assert!(
            weak.upgrade().is_none(),
            "shared Auth cell retained the Engine/Core backedge"
        );
        assert_eq!(dropped.load(Ordering::Acquire), 1);
        assert!(matches!(
            early.recording_collector_jobs(),
            Err(StoreError::StatusBody(503, _))
        ));
        assert!(unbound_auth().recording_collector_jobs().unwrap().is_none());
    }

    struct NoRouteEffects;
    impl UpdateEventSink for NoRouteEffects {
        fn send<'a>(
            &'a self,
            _: &'a str,
            _: Value,
        ) -> BoxFuture<'a, Result<(), dispatcharr_contracts::UpdateEventError>> {
            Box::pin(async { panic!("closed routing probe must not publish events") })
        }
    }
    struct LazyComskipPublisher(AuthService);
    impl crate::comskip_http::Publisher for LazyComskipPublisher {
        fn auth_service(&self) -> AuthService {
            self.0.clone()
        }
        fn admission(&self) -> Result<(), StoreError> {
            panic!("closed routing probe must not admit publication")
        }
        fn publish(
            &self,
            _: i64,
            _: Arc<dyn Send + Sync>,
        ) -> BoxFuture<'_, Result<Uuid, StoreError>> {
            Box::pin(async { panic!("closed routing probe must not publish tasks") })
        }
    }
    #[tokio::test]
    async fn actual_recording_catchall_composes_comskip_playback_and_management_without_sql() {
        use tower::ServiceExt;
        let auth = unbound_auth();
        let api = RecordingHttp::prepare(
            auth.clone(),
            Arc::new(BackEdgeProbe {
                _pre_engine_auth: auth.clone(),
                dropped: Arc::new(AtomicUsize::new(0)),
                stop: None,
            }),
            Arc::new(NoRouteEffects),
        );
        let playback =
            crate::RecordingPlayback::prepare(auth.clone(), |_, _| "unused-closed-playback".into());
        let comskip =
            crate::comskip_http::ComskipHttp::prepare(Arc::new(LazyComskipPublisher(auth)));
        api.close_admission();
        playback.close_admission();
        comskip.close_admission();
        // This is the actual Main composition method; the former separate merge
        // panicked before serving any route. Distinct closed-service responses
        // prove each real handler remains reachable, with no database dependency.
        let router = api.router_with_playback_and_comskip(playback, comskip);
        for (method, path, status, expected) in [
            (
                Method::POST,
                "/api/channels/recordings/17/comskip/",
                StatusCode::NOT_IMPLEMENTED,
                json!({"error":"Comskip intake closed"}),
            ),
            (
                Method::GET,
                "/api/channels/recordings/17/file/",
                StatusCode::SERVICE_UNAVAILABLE,
                json!({"detail":"Recording playback capacity unavailable."}),
            ),
            (
                Method::GET,
                "/api/channels/recordings/17/",
                StatusCode::SERVICE_UNAVAILABLE,
                json!({"error":"Recording intake closed"}),
            ),
        ] {
            let mut request = Request::builder()
                .method(method)
                .uri(path)
                .body(Body::empty())
                .unwrap();
            request
                .extensions_mut()
                .insert(ConnectInfo("127.0.0.1:1".parse::<SocketAddr>().unwrap()));
            let response = router.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), status, "{path}");
            let bytes = to_bytes(response.into_body(), 4096).await.unwrap();
            assert_eq!(
                serde_json::from_slice::<Value>(&bytes).unwrap(),
                expected,
                "{path}"
            );
        }
    }
}
