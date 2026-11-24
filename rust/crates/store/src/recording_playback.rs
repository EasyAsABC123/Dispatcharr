//! Recording file/HLS playback ported from Original Dispatcharr RecordingViewSet.
//! Original Dispatcharr authors retain all application and algorithm credit.
//! Specific routes intentionally bypass RecordingHttp's manage/UI action gate.
use crate::{channels, recording_http, AuthService, RestReadLimits, StoreError, User};
use axum::{
    extract::{ConnectInfo, Path, Request, State},
    http::{header, HeaderMap, HeaderValue, Method},
    response::{IntoResponse, Response},
    routing::{any, get},
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
const WRITER_WAIT: Duration = Duration::from_secs(2);
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
        self.clone().library_alias_router().merge(
            Router::new()
                .route(
                    "/api/channels/recordings/",
                    get(library_read).head(list_recordings),
                )
                .route("/api/channels/recordings/{id}/", any(library_get))
                .route("/api/channels/recordings/{id}", any(library_get))
                .route(recording_http::recurring::COLLECTION, get(recurring_rules))
                .route(recording_http::recurring::PATH, get(recurring_rule))
                .route("/api/channels/recordings/{id}/file/", get(handle))
                .route("/api/channels/recordings/{id}/hls/{*segment}", get(handle))
                .with_state(self),
        )
    }

    pub(super) fn library_alias_router(self) -> Router {
        Router::new()
            .route("/api/channels/recordings.json", any(library_get))
            .route("/api/channels/recordings.json/", any(library_get))
            .route("/api/channels/series-rules/", any(library_get))
            .route(
                recording_http::recurring::JSON_LIST,
                recurring_get_only(recurring_rules),
            )
            .route(
                recording_http::recurring::JSON_LIST_SLASH,
                recurring_get_only(recurring_rules),
            )
            .route(
                recording_http::recurring::JSON_PATH,
                recurring_get_only(recurring_rule),
            )
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
pub(crate) async fn recurring_rules(
    State(api): State<RecordingPlayback>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Result<Response, StoreError> {
    let owner = api.admit()?;
    recording_http::recurring::read_collection(api.inner.auth.clone(), owner, peer, request).await
}

async fn recurring_rule(
    State(api): State<RecordingPlayback>,
    Path(id): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Result<Response, StoreError> {
    let alias_id = recording_http::recurring::detail_alias_id(&request);
    let json_alias = alias_id.is_some();
    if !json_alias && !request.uri().path().ends_with('/') {
        return Err(StoreError::NotFound);
    }
    let id = alias_id.map(str::to_owned).unwrap_or(id);
    let owner = api.admit()?;
    let result = tokio::spawn(async move {
        recording_http::recurring::negotiate(&request, json_alias)?;
        let auth = api.inner.auth.clone().with_retained_scope(owner.clone());
        let user = auth
            .account_user(peer, request.headers().clone(), false)
            .await?;
        recording_http::access(&user, true)?;
        let value = recording_http::recurring::read_value(&auth, &user, &id).await?;
        recording_http::reply(200, value, owner)
    })
    .await
    .map_err(|_| StoreError::UnhandledRestFault)?;
    recording_http::recurring::finish_read(
        result,
        json_alias,
        recording_http::recurring::DETAIL_ALLOW,
    )
}

fn recurring_get_only<H, T>(handler: H) -> axum::routing::MethodRouter<RecordingPlayback>
where
    H: axum::handler::Handler<T, RecordingPlayback>,
    T: 'static,
{
    get(handler)
        .head(crate::core_reads::omitted_json_method)
        .fallback(crate::core_reads::omitted_json_method)
}

async fn library_get(
    state: State<RecordingPlayback>,
    peer: ConnectInfo<SocketAddr>,
    request: Request,
) -> Result<Response, StoreError> {
    if request.method() != Method::GET {
        return Err(StoreError::NotFound);
    }
    library_read(state, peer, request).await
}

const RECORDINGS: &str = "/api/channels/recordings/";

#[derive(Debug, PartialEq, Eq)]
pub(super) enum LibraryRead<'a> {
    Collection,
    Detail(&'a str),
    SeriesRules,
}

impl<'a> LibraryRead<'a> {
    pub(super) fn from_path(path: &'a str) -> Option<Self> {
        match path {
            RECORDINGS | "/api/channels/recordings.json" | "/api/channels/recordings.json/" => {
                return Some(Self::Collection)
            }
            "/api/channels/series-rules/" => return Some(Self::SeriesRules),
            _ => {}
        }
        let tail = path.strip_prefix(RECORDINGS)?;
        let segment = tail.strip_suffix('/').unwrap_or(tail);
        let id = match segment.strip_suffix(".json") {
            Some(id) => id,
            None => tail.strip_suffix('/')?,
        };
        (!id.is_empty() && !id.contains(['.', '/'])).then_some(Self::Detail(id))
    }

    fn allow(&self) -> &'static str {
        match self {
            Self::Collection => "GET, POST, HEAD, OPTIONS",
            Self::Detail(_) => "GET, PUT, PATCH, DELETE, HEAD, OPTIONS",
            Self::SeriesRules => "GET, POST, DELETE, HEAD, OPTIONS",
        }
    }
}

pub(super) async fn library_read(
    State(api): State<RecordingPlayback>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Result<Response, StoreError> {
    let target = LibraryRead::from_path(request.uri().path()).ok_or(StoreError::NotFound)?;
    let allow = target.allow();
    let owner = api.admit()?;
    let result = tokio::spawn(async move {
        crate::epg_program_http::json_negotiation(request.headers(), request.uri())?;
        let auth = api.inner.auth.clone().with_retained_scope(owner.clone());
        let user = auth
            .account_user(peer, request.headers().clone(), false)
            .await?;
        let target = LibraryRead::from_path(request.uri().path()).ok_or(StoreError::NotFound)?;
        recording_http::access(&user, false)?;
        let value = match target {
            LibraryRead::Collection => recording_http::list_value(&auth, &user).await?,
            LibraryRead::Detail(id) => {
                recording_http::detail_value(&auth, &user, &crate::json_compat::unquote_path(id))
                    .await?
            }
            LibraryRead::SeriesRules => {
                recording_http::series_rules_value(&auth, owner.clone()).await?
            }
        };
        recording_http::reply(200, value, owner)
    })
    .await
    .map_err(|_| StoreError::UnhandledRestFault)?;
    let mut response = result.unwrap_or_else(IntoResponse::into_response);
    response
        .headers_mut()
        .insert(header::ALLOW, HeaderValue::from_static(allow));
    // Main's response middleware appends the Source CORS variation.
    response
        .headers_mut()
        .insert(header::VARY, HeaderValue::from_static("Accept"));
    Ok(response)
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
    // Queue before detaching. A viewer that leaves while waiting drops both
    // its FIFO writer request and its recording admission slot.
    let permit = match api.inner.auth.request_writer_permit.clone() {
        Some(inherited) => inherited,
        None => match wait_for_writer(api.inner.auth.user_agent_workers.clone(), WRITER_WAIT).await
        {
            Ok(permit) => Arc::new(permit),
            Err(error) => return error.into_response(),
        },
    };
    // Disconnection never cancels an in-flight SQL/cache/open operation.
    match tokio::spawn(async move { execute(api, owner, identity, path, head, permit).await }).await
    {
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
async fn wait_for_writer(
    writers: Arc<Semaphore>,
    budget: Duration,
) -> Result<OwnedSemaphorePermit, StoreError> {
    // Recording admission already bounds the live requests to SLOTS. Queue
    // their short SQL/auth phase on the existing FIFO writer semaphore. A
    // full writer cohort can then serve later viewers as permits retire.
    tokio::time::timeout(budget, writers.acquire_owned())
        .await
        .map_err(|_| capacity())?
        .map_err(|_| capacity())
}

async fn execute(
    api: RecordingPlayback,
    owner: Arc<Owner>,
    identity: PlaybackRequest,
    path: String,
    head: bool,
    permit: Arc<OwnedSemaphorePermit>,
) -> Result<Response, StoreError> {
    let auth = api
        .inner
        .auth
        .clone()
        .with_retained_scope(owner.clone())
        .with_request_writer_permit(permit)?;
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
    let Ok(id_number) = id.parse::<i64>() else {
        return Ok(recording_lookup_miss(id, action, head));
    };
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
        return Ok(recording_lookup_miss(id, action, head));
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
        let segment = crate::json_compat::unquote_path(segment);
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
fn recording_lookup_miss(id: &str, action: &str, head: bool) -> Response {
    let mut response = StoreError::NotFound.into_response();
    if head || !matches!(action, "hls/index.m3u8" | "hls/index.m3u8/") {
        return response;
    }
    // The stock plain DRF route passes an alphabetic recording key into the
    // numeric lookup on this playlist shape. Reuse its existing generic wire
    // fault after the same access checks without attempting that lookup.
    if action.ends_with('/')
        && id.as_bytes().first().is_some_and(u8::is_ascii_alphabetic)
        && id.as_bytes().last().is_some_and(u8::is_ascii_alphabetic)
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphabetic() || byte == b'-')
    {
        return StoreError::UnhandledRestFault.into_response();
    }
    // These spellings only affect the observed not-found response headers.
    // The parsed recording key for actual data lookup is unchanged.
    let spelling = crate::json_compat::unquote_path(id);
    let digits = spelling.strip_prefix(['+', '-']).unwrap_or(&spelling);
    if !digits.is_empty()
        && digits
            .chars()
            .all(|value| value.is_ascii_digit() || ('٠'..='٩').contains(&value))
    {
        response.headers_mut().insert(
            axum::http::header::ALLOW,
            axum::http::HeaderValue::from_static("GET, HEAD, OPTIONS"),
        );
        response.headers_mut().insert(
            axum::http::header::VARY,
            axum::http::HeaderValue::from_static("Accept"),
        );
    }
    response
}

#[cfg(test)]
mod writer_admission_tests {
    use super::*;

    #[tokio::test]
    async fn ordinary_recurring_json_reads_use_existing_policy_and_fixed_json() {
        use axum::{body::Body, http::StatusCode};
        use tower::ServiceExt;

        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://fixture:fixture@127.0.0.1:1/fixture")
            .unwrap();
        let auth = AuthService::new(
            crate::Store::from_pool(pool),
            crate::AuthConfig::new("owned-recurring-json-fixture".into()).unwrap(),
        );
        let api = RecordingPlayback::prepare(auth, |_, _| {
            panic!("recurring rule reads do not build a media origin")
        });
        let router = api.clone().router();
        for (method, path, expected, allow) in [
            (
                Method::GET,
                "/api/channels/recurring-rules.json",
                StatusCode::NOT_ACCEPTABLE,
                Some("GET, POST, HEAD, OPTIONS"),
            ),
            (
                Method::GET,
                "/api/channels/recurring-rules.json/?format=api",
                StatusCode::NOT_ACCEPTABLE,
                Some("GET, POST, HEAD, OPTIONS"),
            ),
            (
                Method::GET,
                "/api/channels/recurring-rules/19.json",
                StatusCode::NOT_ACCEPTABLE,
                Some("GET, PUT, PATCH, DELETE, HEAD, OPTIONS"),
            ),
            (
                Method::GET,
                "/api/channels/recurring-rules/19.json/",
                StatusCode::NOT_ACCEPTABLE,
                Some("GET, PUT, PATCH, DELETE, HEAD, OPTIONS"),
            ),
            (
                Method::POST,
                "/api/channels/recurring-rules.json",
                StatusCode::NOT_FOUND,
                None,
            ),
            (
                Method::HEAD,
                "/api/channels/recurring-rules/19.json",
                StatusCode::NOT_FOUND,
                None,
            ),
            (
                Method::GET,
                "/api/channels/recurring-rules/19",
                StatusCode::NOT_FOUND,
                None,
            ),
        ] {
            let mut request = Request::builder()
                .method(method)
                .uri(path)
                .header(header::ACCEPT, "text/html")
                .body(Body::empty())
                .unwrap();
            request
                .extensions_mut()
                .insert(ConnectInfo("127.0.0.1:1234".parse::<SocketAddr>().unwrap()));
            let reply = router.clone().oneshot(request).await.unwrap();
            assert_eq!(reply.status(), expected);
            if let Some(allow) = allow {
                assert_eq!(reply.headers()[header::ALLOW], allow);
                assert_eq!(reply.headers()[header::VARY], "Accept");
            }
        }
        drop(router);
        api.close_admission();
    }

    #[test]
    fn source_playlist_lookup_miss_keeps_scoped_wire_and_excludes_other_requests() {
        use axum::http::{header, StatusCode};
        for (id, action) in [
            ("42631", "hls/index.m3u8"),
            ("042631", "hls/index.m3u8/"),
            ("%2B42631", "hls/index.m3u8/"),
            ("-42631", "hls/index.m3u8/"),
            ("%D9%A4%D9%A2%D9%A6%D9%A3%D9%A1", "hls/index.m3u8/"),
        ] {
            let response = recording_lookup_miss(id, action, false);
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
            assert_eq!(response.headers()[header::ALLOW], "GET, HEAD, OPTIONS");
            assert_eq!(response.headers()[header::VARY], "Accept");
            assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
        }
        let error = recording_lookup_miss("owned-missing", "hls/index.m3u8/", false);
        assert_eq!(error.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(error.headers()[header::CONTENT_LENGTH], "145");
        assert_eq!(error.headers()[header::VARY], "origin");
        assert!(!error.headers().contains_key(header::ALLOW));
        for (id, action, head) in [
            ("", "hls/index.m3u8/", false),
            ("+-123", "hls/index.m3u8/", false),
            ("owned-missing", "hls/index.m3u8", false),
            ("owned-missing", "hls/index.m3u8/", true),
            ("owned-missing", "file/", false),
            ("42631", "hls/seg_00001.ts", false),
        ] {
            let response = recording_lookup_miss(id, action, head);
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
            assert!(!response.headers().contains_key(header::ALLOW));
            assert!(!response.headers().contains_key(header::VARY));
        }
    }

    #[tokio::test]
    async fn ten_viewers_wait_in_order_without_increasing_four_writer_limit() {
        let writers = Arc::new(Semaphore::new(4));
        let initial = writers.clone().acquire_many_owned(4).await.unwrap();
        let mut waiting: Vec<_> = (0..10)
            .map(|_| Box::pin(wait_for_writer(writers.clone(), WRITER_WAIT)))
            .collect();
        for viewer in &mut waiting {
            assert!(futures_util::poll!(viewer.as_mut()).is_pending());
        }
        drop(initial);
        let mut viewer = 0;
        while viewer < waiting.len() {
            let cohort_end = (viewer + 4).min(waiting.len());
            let mut admitted = Vec::new();
            for queued in &mut waiting[viewer..cohort_end] {
                admitted.push(
                    tokio::time::timeout(WRITER_WAIT, queued.as_mut())
                        .await
                        .unwrap()
                        .unwrap(),
                );
            }
            if cohort_end < waiting.len() {
                assert!(futures_util::poll!(waiting[cohort_end].as_mut()).is_pending());
            }
            assert_eq!(writers.available_permits(), 4 - admitted.len());
            drop(admitted);
            viewer = cohort_end;
        }
        assert_eq!(writers.available_permits(), 4);
    }

    #[tokio::test]
    async fn cancelled_and_timed_out_waiters_do_not_consume_a_writer() {
        let writers = Arc::new(Semaphore::new(1));
        let initial = writers.clone().acquire_owned().await.unwrap();
        let mut cancelled = Box::pin(wait_for_writer(writers.clone(), WRITER_WAIT));
        assert!(futures_util::poll!(cancelled.as_mut()).is_pending());
        drop(cancelled);

        let expired = wait_for_writer(writers.clone(), Duration::ZERO).await;
        assert!(matches!(expired, Err(StoreError::StatusBody(503, _))));
        assert_eq!(writers.available_permits(), 0);

        let mut waiting = Box::pin(wait_for_writer(writers.clone(), WRITER_WAIT));
        assert!(futures_util::poll!(waiting.as_mut()).is_pending());
        drop(initial);
        let admitted = waiting.await.unwrap();
        assert_eq!(writers.available_permits(), 0);
        drop(admitted);
        assert_eq!(writers.available_permits(), 1);
    }
}
