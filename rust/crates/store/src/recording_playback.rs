//! Recording file/HLS playback ported from Original Dispatcharr RecordingViewSet.
//! Original Dispatcharr authors retain all application and algorithm credit.
//! Specific routes intentionally bypass RecordingHttp's manage/UI action gate.
use crate::{channels, AuthService, RestReadLimits, StoreError, User};
use axum::{
    extract::{ConnectInfo, Request, State},
    http::{HeaderMap, Method},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use dispatcharr_contracts::PlaybackRequest;
use serde_json::{json, Value};
use sqlx::{Postgres, QueryBuilder, Row};
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
/// Main supplies output-http::source_output_origin, sharing its qualified proxy rules.
pub type RecordingPlaybackOrigin = fn(&AuthService, &PlaybackRequest) -> String;
#[derive(Clone)]
pub struct RecordingPlayback {
    inner: Arc<Inner>,
}
struct Inner {
    auth: AuthService,
    origin: RecordingPlaybackOrigin,
    slots: Arc<Semaphore>,
    closed: AtomicBool,
    used: AtomicBool,
}
pub(super) struct Owner {
    _slot: OwnedSemaphorePermit,
    _inner: Arc<Inner>,
}
const SLOTS: usize = 32;
impl RecordingPlayback {
    pub fn prepare(auth: AuthService, origin: RecordingPlaybackOrigin) -> Self {
        Self {
            inner: Arc::new(Inner {
                auth,
                origin,
                slots: Arc::new(Semaphore::new(SLOTS)),
                closed: AtomicBool::new(false),
                used: AtomicBool::new(false),
            }),
        }
    }
    pub fn router(self) -> Router {
        Router::new()
            .route("/api/channels/recordings/", get(list_recordings))
            .route("/api/channels/recordings/{id}/file/", get(handle))
            .route("/api/channels/recordings/{id}/hls/{*segment}", get(handle))
            .with_state(self)
    }
    /// Called only by the combined normal recording route dispatcher. It enters
    /// the identical owned handler used by this service's isolated router.
    pub(crate) async fn dispatch(&self, peer: SocketAddr, request: Request) -> Response {
        handle(State(self.clone()), ConnectInfo(peer), request).await
    }
    fn admit(&self) -> Result<Arc<Owner>, StoreError> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(capacity());
        }
        let slot = self
            .inner
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| capacity())?;
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(capacity());
        }
        self.inner.used.store(true, Ordering::Release);
        Ok(Arc::new(Owner {
            _slot: slot,
            _inner: self.inner.clone(),
        }))
    }

    pub fn was_used(&self) -> bool {
        self.inner.used.load(Ordering::Acquire)
    }

    pub fn close_admission(&self) {
        self.inner.closed.store(true, Ordering::Release);
    }
    /// Bodies retain actual file descriptors and admission until EOF/drop.
    /// A false result means physical readers/cache replies remain, never a drain pass.
    pub async fn shutdown(&self, budget: Duration) -> bool {
        self.close_admission();
        tokio::time::timeout(
            budget,
            self.inner.slots.clone().acquire_many_owned(SLOTS as u32),
        )
        .await
        .is_ok_and(|v| v.is_ok())
    }
}
fn capacity() -> StoreError {
    StoreError::StatusBody(
        503,
        json!({"detail":"Recording playback capacity unavailable."}),
    )
}
async fn list_recordings(
    State(api): State<RecordingPlayback>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Response, StoreError> {
    let owner = api.admit()?;
    tokio::spawn(async move {
        // The DVR collection uses UI and DVR-view permission. File/HLS requests
        // keep the separate Source Auth4 and STREAMS path below.
        let auth = api.inner.auth.clone().with_retained_scope(owner.clone());
        let user = auth.account_user(peer, headers, false).await?;
        super::recording_http::access(&user, false)?;
        let value = super::recording_http::list_value(&auth, &user).await?;
        super::recording_http::reply(200, value, owner)
    })
    .await
    .map_err(|_| StoreError::UnhandledRestFault)?
}

async fn handle(
    State(api): State<RecordingPlayback>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Response {
    let owner = match api.admit() {
        Ok(owner) => owner,
        Err(error) => return error.into_response(),
    };
    let identity = PlaybackRequest {
        channel_id: String::new(),
        query: url::form_urlencoded::parse(request.uri().query().unwrap_or("").as_bytes())
            .into_owned()
            .collect(),
        headers: request
            .headers()
            .iter()
            .filter_map(|(k, v)| Some((k.to_string(), v.to_str().ok()?.to_owned())))
            .collect(),
        peer_ip: peer.ip(),
    };
    let path = request.uri().path().to_owned();
    let head = request.method() == Method::HEAD;
    // Disconnection never cancels an in-flight SQL/cache/open operation.
    match tokio::spawn(async move { execute(api, owner, identity, path, head).await }).await {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => error.into_response(),
        Err(_) => StoreError::UnhandledRestFault.into_response(),
    }
}
fn viewer(user: &User) -> Result<bool, StoreError> {
    if user.user_level >= 10 {
        return Ok(true);
    }
    if user.user_level < 1 {
        return Ok(false);
    }
    let props = &user.custom_properties;
    if channels::truthy(Some(props)) && !props.is_object() {
        return Err(StoreError::UnhandledRestFault);
    }
    let level = props.get("dvr_access");
    if level.is_some_and(|v| v.is_array() || v.is_object()) {
        return Err(StoreError::UnhandledRestFault);
    }
    Ok(level.and_then(Value::as_str) != Some("none"))
}
async fn execute(
    api: RecordingPlayback,
    owner: Arc<Owner>,
    identity: PlaybackRequest,
    path: String,
    head: bool,
) -> Result<Response, StoreError> {
    let auth = api.inner.auth.clone().with_retained_scope(owner.clone());
    let permit = match auth.request_writer_permit.clone() {
        Some(v) => v,
        None => Arc::new(
            auth.user_agent_workers
                .clone()
                .try_acquire_owned()
                .map_err(|_| capacity())?,
        ),
    };
    let auth = auth.with_request_writer_permit(permit)?;
    let mut subject = auth
        .authenticate_read_strategy(&identity, Some(RestReadLimits::default()))
        .await?;
    // Query authentication is the LAST authenticator and silently ignores invalid JWTs.
    // Header authentication errors above therefore cannot fall through to query tokens.
    if subject.is_none() {
        if let Some(claims) = identity
            .parameter("token")
            .filter(|s| !s.is_empty())
            .and_then(|v| auth.decode_token(v, "access").ok())
        {
            if claims.id().is_some() {
                let mut query_identity = identity.clone();
                query_identity
                    .headers
                    .retain(|(k, _)| !k.eq_ignore_ascii_case("authorization"));
                query_identity.headers.push((
                    "authorization".into(),
                    format!("Bearer {}", identity.parameter("token").unwrap_or("")),
                ));
                subject = auth
                    .authenticate_read_strategy(&query_identity, Some(RestReadLimits::default()))
                    .await?;
            }
        }
    }
    if !auth
        .source_network_allowed(auth.client_ip(&identity), "STREAMS", None)
        .await?
    {
        return Err(StoreError::Forbidden);
    }
    let user = &subject.as_ref().ok_or(StoreError::Forbidden)?.user;
    if !viewer(user)? {
        return Err(StoreError::Forbidden);
    }
    let tail = path
        .strip_prefix("/api/channels/recordings/")
        .ok_or(StoreError::NotFound)?;
    let (id, action) = tail.split_once('/').ok_or(StoreError::NotFound)?;
    let id_number = id.parse::<i64>().map_err(|_| StoreError::NotFound)?;
    let mut query=QueryBuilder::<Postgres>::new("SELECT CASE WHEN octet_length(COALESCE(r.custom_properties,'{}'::jsonb)::text)<=65536 THEN COALESCE(r.custom_properties,'{}'::jsonb) ELSE NULL END AS properties FROM dispatcharr_channels_recording r JOIN dispatcharr_channels_channel c ON c.id=r.channel_id WHERE r.id=");
    query.push_bind(id_number);
    channels::scope(&mut query, user);
    let row = query.build().fetch_optional(auth.store.pool()).await?;
    let Some(row) = row else {
        if user.user_level < 10
            && sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM dispatcharr_channels_recording WHERE id=$1)",
            )
            .bind(id_number)
            .fetch_one(auth.store.pool())
            .await?
        {
            return Err(StoreError::Forbidden);
        }
        return Err(StoreError::NotFound);
    };
    let properties: Option<Value> = row.try_get("properties")?;
    let properties = properties.ok_or_else(capacity)?;
    if channels::truthy(Some(&properties)) && !properties.is_object() {
        return Err(StoreError::UnhandledRestFault);
    }
    let origin = (api.inner.origin)(&auth, &identity);
    let suffix = identity
        .parameter("token")
        .filter(|v| !v.is_empty())
        .map(|v| {
            format!(
                "?{}",
                url::form_urlencoded::Serializer::new(String::new())
                    .append_pair("token", v)
                    .finish()
            )
        })
        .unwrap_or_default();
    let base = format!("{origin}/api/channels/recordings/{id}");
    if action == "file/" {
        super::recording_playback_files::file(&properties, &identity, &base, &suffix, head, owner)
            .await
    } else if let Some(segment) = action.strip_prefix("hls/") {
        // Axum's catchall extraction is decoded by Path, URI.path itself is encoded.
        let segment = percent_decode(segment)?;
        super::recording_playback_files::hls(
            &auth,
            id,
            &properties,
            &segment,
            (&base, &suffix),
            head,
            owner,
        )
        .await
    } else {
        Err(StoreError::NotFound)
    }
}
fn percent_decode(raw: &str) -> Result<String, StoreError> {
    // form_urlencoded uses '+' as space; protect literal path plus signs.
    let encoded = format!("v={}", raw.replace('+', "%2B").replace('&', "%26"));
    url::form_urlencoded::parse(encoded.as_bytes())
        .next()
        .map(|(_, v)| v.into_owned())
        .ok_or(StoreError::NotFound)
}
