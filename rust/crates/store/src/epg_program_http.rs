//! Stored programme JSON routes derived from Original Dispatcharr.
//! Original Dispatcharr authors and contributors retain full credit.
use crate::{
    catalogue_options,
    core_reads::{negotiate_literal_json_alias, omitted_json_method, JsonReadId},
    AuthService, EpgReadResource, StoreError,
};
use axum::{
    body::Body,
    extract::{ConnectInfo, OriginalUri, Path, State},
    http::{header, HeaderMap, HeaderValue, Method, Uri},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use bytes::Bytes;
use futures_util::{StreamExt, TryStream, TryStreamExt};
use serde::{ser::SerializeSeq, Serialize};
use serde_json::{
    json,
    ser::{CompactFormatter, Formatter, PrettyFormatter, Serializer},
    Value,
};
use sqlx::{Acquire, Row};
use std::{
    io::{self, Write},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};

mod mutations;
mod programme_html;

#[derive(Clone, Copy, Debug)]
pub struct EpgProgramReadLimits {
    pub concurrent_responses: usize,
    pub response_bytes: usize,
    pub row_bytes: usize,
    pub deadline: Duration,
}
impl Default for EpgProgramReadLimits {
    fn default() -> Self {
        Self {
            concurrent_responses: 4,
            response_bytes: 16 * 1024 * 1024,
            row_bytes: 1024 * 1024,
            deadline: Duration::from_secs(2),
        }
    }
}
#[derive(Clone)]
pub struct EpgProgramReads {
    auth: AuthService,
    limits: EpgProgramReadLimits,
    workers: Arc<Semaphore>,
    current_clock: Arc<dyn Fn() -> chrono::DateTime<chrono::Utc> + Send + Sync>,
    current_file_lookup: Option<(Arc<dyn crate::CurrentFileLookup>, std::path::PathBuf)>,
}
fn unavailable() -> StoreError {
    StoreError::StatusBody(503, json!({"detail":"Guide read capacity unavailable."}))
}
struct OwnedJson {
    data: Vec<u8>,
    _lease: Arc<OwnedSemaphorePermit>,
}
impl AsRef<[u8]> for OwnedJson {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}
pub(crate) struct BoundedJson {
    data: Vec<u8>,
    maximum: usize,
}
impl BoundedJson {
    pub(crate) fn new(maximum: usize) -> Self {
        Self {
            data: Vec::new(),
            maximum,
        }
    }
    pub(crate) fn into_data(self) -> Vec<u8> {
        self.data
    }
}
impl Write for BoundedJson {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let needed = self
            .data
            .len()
            .checked_add(bytes.len())
            .filter(|n| *n <= self.maximum)
            .ok_or_else(|| io::Error::other("Guide response exceeds capacity"))?;
        if needed > self.data.capacity() {
            let next = needed
                .max(self.data.capacity().saturating_mul(2))
                .min(self.maximum);
            self.data.reserve_exact(next - self.data.len());
        }
        self.data.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
async fn write_programmes<S, F>(
    out: &mut BoundedJson,
    mut rows: S,
    detail: bool,
    formatter: F,
) -> Result<(), StoreError>
where
    S: TryStream<Ok = Value, Error = StoreError> + Unpin,
    F: Formatter,
{
    let mut serializer = Serializer::with_formatter(out, formatter);
    if detail {
        let value = rows
            .try_next()
            .await?
            .ok_or_else(|| EpgReadResource::Programs.missing())?;
        value
            .serialize(&mut serializer)
            .map_err(|_| unavailable())?;
        if rows.try_next().await?.is_some() {
            return Err(unavailable());
        }
    } else {
        let mut sequence =
            serde::Serializer::serialize_seq(&mut serializer, None).map_err(|_| unavailable())?;
        while let Some(value) = rows.try_next().await? {
            sequence
                .serialize_element(&value)
                .map_err(|_| unavailable())?;
        }
        sequence.end().map_err(|_| unavailable())?;
    }
    Ok(())
}

impl EpgProgramReads {
    /// Current-programme DB-only component using the same physical admission
    /// and retained response allocation as ordinary programme reads. HTTP and
    /// byte-index fallback are separate gates; this method adds no route.
    pub async fn read_current_db(
        &self,
        peer: SocketAddr,
        headers: HeaderMap,
        body: serde_json::Value,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<Bytes, StoreError> {
        self.auth.account_user(peer, headers, true).await?;
        let selection = crate::CurrentProgramSelection::from_json(&body)?;
        self.read_current_selected(selection, now).await
    }
    pub(crate) async fn read_current_selected(
        &self,
        selection: crate::CurrentProgramSelection,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<Bytes, StoreError> {
        let lease = Arc::new(
            self.workers
                .clone()
                .try_acquire_owned()
                .map_err(|_| unavailable())?,
        );
        let state = self.clone();
        let (sender, receiver) = oneshot::channel();
        tokio::spawn(async move {
            let data = match (&state.current_file_lookup, selection) {
                (Some((lookup, cache_root)), crate::CurrentProgramSelection::EpgData(ids)) => {
                    state
                        .auth
                        .store
                        .current_epg_with_file_lookup(
                            ids,
                            now,
                            state.limits.response_bytes,
                            cache_root.clone(),
                            lookup.clone(),
                            lease.clone(),
                        )
                        .await
                }
                (_, selection) => {
                    state
                        .auth
                        .store
                        .current_programmes_db(selection, now, 50000, state.limits.response_bytes)
                        .await
                }
            };
            let result = data.map(|data| {
                Bytes::from_owner(OwnedJson {
                    data,
                    _lease: lease,
                })
            });
            let _ = sender.send(result);
        });
        tokio::time::timeout(self.limits.deadline, receiver)
            .await
            .map_err(|_| unavailable())?
            .map_err(|_| unavailable())?
    }
    pub fn new(auth: AuthService, limits: EpgProgramReadLimits) -> Result<Self, StoreError> {
        if !(1..=16).contains(&limits.concurrent_responses)
            || !(128..=128 * 1024 * 1024).contains(&limits.response_bytes)
            || !(128..=4 * 1024 * 1024).contains(&limits.row_bytes)
            || !(Duration::from_millis(100)..=Duration::from_secs(30)).contains(&limits.deadline)
        {
            return Err(StoreError::Unsupported("Invalid guide read limits"));
        }
        Ok(Self {
            auth,
            limits,
            workers: Arc::new(Semaphore::new(limits.concurrent_responses)),
            current_clock: Arc::new(chrono::Utc::now),
            current_file_lookup: None,
        })
    }
    /// Optional component adapter. The default and Channels selection remain
    /// DB-only. Callers must supply a separately verified dispatcher/path context.
    pub fn with_current_file_lookup(
        mut self,
        lookup: Arc<dyn crate::CurrentFileLookup>,
        cache_root: std::path::PathBuf,
    ) -> Result<Self, StoreError> {
        if !cache_root.is_absolute() || cache_root.as_os_str().len() > 4096 {
            return Err(StoreError::Unsupported(
                "Invalid current-programme cache root",
            ));
        }
        self.current_file_lookup = Some((lookup, cache_root));
        Ok(self)
    }
    pub fn with_current_clock(
        mut self,
        clock: impl Fn() -> chrono::DateTime<chrono::Utc> + Send + Sync + 'static,
    ) -> Self {
        self.current_clock = Arc::new(clock);
        self
    }
    pub(crate) fn auth(&self) -> &AuthService {
        &self.auth
    }
    pub(crate) fn current_now(&self) -> chrono::DateTime<chrono::Utc> {
        (self.current_clock)()
    }
    /// Separate current-programme adapter; attach only after its HTTP gates.
    pub fn current_router(self) -> Router {
        crate::epg_current_http::router(self)
    }
    /// Counts physical SQL workers and response allocations, including Bytes clones.
    pub fn physical_operations(&self) -> usize {
        self.limits.concurrent_responses - self.workers.available_permits()
    }
    pub async fn shutdown(&self, grace: Duration) -> bool {
        self.workers.close();
        let end = tokio::time::Instant::now() + grace;
        while self.physical_operations() != 0 && tokio::time::Instant::now() < end {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        self.physical_operations() == 0
    }
    /// Auth before lookup. Pending SQL retains its permit after a caller aborts
    /// or times out; local PostgreSQL statement_timeout bounds blocked statements.
    pub async fn read(
        &self,
        peer: SocketAddr,
        headers: HeaderMap,
        raw_id: Option<&str>,
        origin: String,
    ) -> Result<Bytes, StoreError> {
        self.read_with_indentation(peer, headers, raw_id, origin, 0)
            .await
    }

    async fn read_with_indentation(
        &self,
        peer: SocketAddr,
        headers: HeaderMap,
        raw_id: Option<&str>,
        origin: String,
        indentation: u8,
    ) -> Result<Bytes, StoreError> {
        let user = self.auth.account_user(peer, headers, false).await?;
        if user.user_level < 1 {
            return Err(StoreError::PermissionDenied);
        }
        self.read_authorized(raw_id, origin, indentation).await
    }

    async fn read_authorized(
        &self,
        raw_id: Option<&str>,
        origin: String,
        indentation: u8,
    ) -> Result<Bytes, StoreError> {
        let id = raw_id.map(program_id).transpose()?;
        let lease = Arc::new(
            self.workers
                .clone()
                .try_acquire_owned()
                .map_err(|_| unavailable())?,
        );
        let state = self.clone();
        let (sender, receiver) = oneshot::channel();
        tokio::spawn(async move {
            let result = state.query(id, &origin, lease, indentation).await;
            let _ = sender.send(result);
        });
        tokio::time::timeout(self.limits.deadline, receiver)
            .await
            .map_err(|_| unavailable())?
            .map_err(|_| unavailable())?
    }
    async fn query(
        &self,
        id: Option<i64>,
        origin: &str,
        lease: Arc<OwnedSemaphorePermit>,
        indentation: u8,
    ) -> Result<Bytes, StoreError> {
        let mut conn = self.auth.store.pool().acquire().await?;
        let mut tx = conn.begin().await?;
        sqlx::query("SELECT set_config('statement_timeout',$1,true)")
            .bind(format!("{}ms", self.limits.deadline.as_millis()))
            .execute(&mut *tx)
            .await?;
        // Keep the original select_related join. Gate each text/JSON column in
        // SQL, before the client decoder allocates a potentially oversized row.
        let fields = projected_fields("$2");
        let relation = "FROM epg_programdata p JOIN epg_epgdata e ON e.id=p.epg_id";
        let sql=format!("SELECT {fields} {relation} WHERE $1::bigint IS NULL UNION ALL SELECT {fields} {relation} WHERE p.id=$1::bigint");
        let result = async {
            let detail = id.is_some();
            let rows = sqlx::query(&sql)
                .bind(id)
                .bind(self.limits.row_bytes as i64)
                .fetch(&mut *tx)
                .map(|row| {
                    let row = row?;
                    if row.try_get::<bool, _>("oversized")? {
                        return Err(unavailable());
                    }
                    EpgReadResource::Programs.project(row, detail, origin)
                });
            let mut out = BoundedJson::new(self.limits.response_bytes);
            if indentation == 0 {
                write_programmes(&mut out, rows, detail, CompactFormatter).await?;
            } else {
                let spaces = &b"        "[..usize::from(indentation)];
                write_programmes(&mut out, rows, detail, PrettyFormatter::with_indent(spaces))
                    .await?;
            }
            Ok(out.into_data())
        }
        .await;
        // Drain rollback before the worker releases SQL admission. A detached
        // observer never returns a busy connection to the pool early.
        tx.rollback().await?;
        result.map(|data| {
            Bytes::from_owner(OwnedJson {
                data,
                _lease: lease,
            })
        })
    }
    pub fn router(self) -> Router {
        Router::new()
            .route("/api/epg/programs/", get(list).options(programme_options))
            .route(
                "/api/epg/programs.json",
                get(list_json_alias)
                    .head(omitted_json_method)
                    .fallback(omitted_json_method),
            )
            .route(
                "/api/epg/programs/{json_id}",
                get(detail_json_alias)
                    .head(omitted_json_method)
                    .fallback(omitted_json_method),
            )
            .route(
                "/api/epg/programs/{id}/",
                get(detail)
                    .options(programme_options)
                    .patch(mutations::patch)
                    .put(mutations::replace)
                    .delete(mutations::delete),
            )
            .with_state(self)
    }
}
fn program_id(raw: &str) -> Result<i64, StoreError> {
    dispatcharr_ingest_core::source_integer_literal(raw)
        .ok_or(StoreError::NotFound)?
        .parse()
        .map_err(|_| EpgReadResource::Programs.missing())
}

fn projected_fields(limit: &str) -> String {
    let size="coalesce(octet_length(p.title),0)::bigint+coalesce(octet_length(p.sub_title),0)+coalesce(octet_length(p.description),0)+coalesce(octet_length(p.tvg_id),0)+coalesce(octet_length(p.custom_properties::text),0)";
    format!("p.id::bigint AS id,p.start_time,p.end_time,({size}>{limit}) AS oversized,CASE WHEN {size}<={limit} THEN p.title ELSE '' END AS title,CASE WHEN {size}<={limit} THEN p.sub_title END AS sub_title,CASE WHEN {size}<={limit} THEN p.description END AS description,CASE WHEN {size}<={limit} THEN p.tvg_id END AS tvg_id,CASE WHEN {size}<={limit} THEN p.custom_properties END AS custom_properties")
}
fn origin(auth: &AuthService, peer: SocketAddr, headers: &HeaderMap) -> String {
    let header = |name| {
        headers
            .get(name)
            .map(|v| dispatcharr_contracts::source_header_value(v.as_bytes()))
    };
    let trusted = auth.is_trusted_proxy(peer.ip());
    let scheme = if trusted {
        header("x-forwarded-proto")
            .and_then(|v| {
                v.split(',')
                    .next()
                    .map(str::trim)
                    .filter(|v| !v.is_empty())
                    .map(str::to_owned)
            })
            .unwrap_or("http".into())
    } else {
        "http".into()
    };
    let standard = if scheme == "https" { "443" } else { "80" };
    let format = |host: &str, port: Option<&str>| match port.filter(|p| *p != standard) {
        Some(p) => format!("{scheme}://{host}:{p}"),
        None => format!("{scheme}://{host}"),
    };
    if trusted {
        if let Some(host) = header("x-forwarded-host").filter(|v| !v.is_empty()) {
            if let Some((host, port)) = host.split_once(':') {
                return format(host, Some(port));
            }
            if let Some(port) = header("x-forwarded-port").filter(|v| !v.is_empty()) {
                return format(&host, Some(&port));
            }
            if header("x-forwarded-proto").is_some_and(|v| !v.is_empty()) {
                return format(&host, None);
            }
        }
    }
    let host = header("host").unwrap_or("localhost".into());
    if let Some((host, port)) = host.split_once(':') {
        return format(host, Some(port));
    }
    let port = trusted
        .then(|| header("x-forwarded-port"))
        .flatten()
        .filter(|v| !v.is_empty());
    format(&host, port.as_deref())
}
pub(crate) fn json_negotiation(headers: &HeaderMap, uri: &Uri) -> Result<(), StoreError> {
    // Full browsable-API HTML remains an explicit gate. Do not pass it off as
    // a JSON representation. The original rejects unsupported JSON formats
    // and Accept values before authentication during initial negotiation.
    let format = url::form_urlencoded::parse(uri.query().unwrap_or("").as_bytes())
        .find(|(key, _)| key == "format")
        .map(|(_, value)| value.into_owned());
    if format
        .as_deref()
        .is_some_and(|value| !["json", "api"].contains(&value))
    {
        return Err(StoreError::NotFound);
    }
    if format.as_deref() == Some("api") {
        return Err(StoreError::Unsupported(
            "Guide browsable HTML is not yet implemented",
        ));
    }
    if let Some(accept) = headers.get("accept").and_then(|value| value.to_str().ok()) {
        if accept == "text/html" {
            return Err(StoreError::Unsupported(
                "Guide browsable HTML is not yet implemented",
            ));
        }
        if !accept.split(',').any(|part| {
            matches!(
                part.trim().split(';').next(),
                Some("*/*" | "application/*" | "application/json")
            )
        }) {
            return Err(StoreError::StatusBody(
                406,
                json!({"detail":"Could not satisfy the request Accept header."}),
            ));
        }
    }
    Ok(())
}
const PROGRAMME_LIST_ALLOW: &str = "GET, POST, HEAD, OPTIONS";
const PROGRAMME_DETAIL_ALLOW: &str = "GET, PUT, PATCH, DELETE, HEAD, OPTIONS";
const PROGRAMME_JSON_VARY: &str = "Accept, origin";

fn programme_json_authorization_error(error: &StoreError) -> bool {
    match error {
        StoreError::Unauthorized("Authentication credentials were not provided.")
        | StoreError::PermissionDenied => true,
        StoreError::AuthenticationBody(body) => body["code"] == "user_inactive",
        _ => false,
    }
}

fn canonical_programme_read_path(uri: &Uri, id: Option<&str>) -> bool {
    match id {
        None => uri.path() == "/api/epg/programs/",
        Some(id) => uri
            .path()
            .strip_prefix("/api/epg/programs/")
            .and_then(|path| path.strip_suffix('/'))
            .is_some_and(|raw| {
                raw == id
                    && !raw.starts_with('0')
                    && raw.bytes().all(|byte| byte.is_ascii_digit())
                    && raw.parse::<i64>().is_ok()
            }),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProgrammeMedia {
    Html { format_alias: bool },
    ExplicitJson { indentation: u8 },
}

fn accept_quality(value: &str) -> bool {
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    fraction.len() <= 3
        && match whole {
            "0" => fraction.bytes().all(|byte| byte.is_ascii_digit()),
            "1" => fraction.bytes().all(|byte| byte == b'0'),
            _ => false,
        }
}

fn programme_media(method: &Method, uri: &Uri, headers: &HeaderMap) -> Option<ProgrammeMedia> {
    let accept = headers.get("accept")?.to_str().ok()?;
    let format_alias = uri.query() == Some("format=api");
    if method == Method::HEAD {
        return (uri.query().is_none() && accept == "text/html").then_some(ProgrammeMedia::Html {
            format_alias: false,
        });
    }
    if method != Method::GET || (uri.query().is_some() && !format_alias) {
        return None;
    }

    // For these ordinary media ranges DRF selects by specificity, then by
    // renderer order with JSON first. The quality parameter is not a weight.
    let mut html = 0;
    let mut json = 0;
    let mut selected_indentation = 0;
    for range in accept.split(',') {
        let (media_type, parameters) = range.split_once(';').unwrap_or((range, ""));
        let media_type = media_type.trim();
        let is_json = media_type.eq_ignore_ascii_case("application/json");
        let mut indentation = None;
        if !parameters.is_empty()
            && !parameters.split(';').all(|parameter| {
                let Some((name, value)) = parameter.trim().split_once('=') else {
                    return false;
                };
                if name.eq_ignore_ascii_case("q") {
                    return accept_quality(value);
                }
                if is_json && name.eq_ignore_ascii_case("indent") {
                    indentation = Some(value.parse::<i32>().unwrap_or(0).clamp(0, 8) as u8);
                    return true;
                }
                false
            })
        {
            continue;
        }
        match media_type {
            "*/*" => {
                html = html.max(1);
                json = json.max(1);
            }
            media if media.eq_ignore_ascii_case("text/*") => html = html.max(2),
            media if media.eq_ignore_ascii_case("application/*") => json = json.max(2),
            media if media.eq_ignore_ascii_case("text/html") => html = 3,
            _ if is_json => {
                let rank = if indentation.is_some() { 4 } else { 3 };
                if rank > json {
                    json = rank;
                    selected_indentation = indentation.unwrap_or(0);
                }
            }
            _ => {}
        }
    }
    if html != 0 && (format_alias || html > json) {
        Some(ProgrammeMedia::Html { format_alias })
    } else if !format_alias && json >= 3 {
        Some(ProgrammeMedia::ExplicitJson {
            indentation: selected_indentation,
        })
    } else {
        None
    }
}

async fn respond(
    state: EpgProgramReads,
    peer: SocketAddr,
    headers: HeaderMap,
    method: Method,
    id: Option<String>,
    uri: Uri,
    media: Option<ProgrammeMedia>,
) -> Result<Response, StoreError> {
    let allow = if id.is_some() {
        PROGRAMME_DETAIL_ALLOW
    } else {
        PROGRAMME_LIST_ALLOW
    };
    let accept = headers.get_all(header::ACCEPT);
    let mut accept = accept.iter();
    let plain_json_get = method == Method::GET
        && uri.query().is_none()
        && accept
            .next()
            .is_some_and(|value| value == "application/json")
        && accept.next().is_none();
    let (explicit_json, indentation) = match media {
        Some(ProgrammeMedia::ExplicitJson { indentation }) => (true, indentation),
        _ => {
            json_negotiation(&headers, &uri)?;
            let exact_head = method == Method::HEAD
                && uri.query().is_none()
                && headers
                    .get("accept")
                    .is_some_and(|value| value == "application/json");
            (exact_head, 0)
        }
    };
    let origin = origin(&state.auth, peer, &headers);
    let data = match state
        .read_with_indentation(peer, headers, id.as_deref(), origin, indentation)
        .await
    {
        Ok(data) => data,
        Err(error)
            if plain_json_get
                && programme_json_authorization_error(&error)
                && canonical_programme_read_path(&uri, id.as_deref()) =>
        {
            let mut response = error.into_response();
            let headers = response.headers_mut();
            headers.insert(header::ALLOW, HeaderValue::from_static(allow));
            headers.insert(header::VARY, HeaderValue::from_static(PROGRAMME_JSON_VARY));
            return Ok(response);
        }
        Err(error) => return Err(error),
    };
    let mut response = Response::builder().header("content-type", "application/json");
    if explicit_json {
        response = response
            .header("vary", PROGRAMME_JSON_VARY)
            .header("allow", allow)
            .header("content-length", data.len().to_string());
    }
    let body = if explicit_json && method == Method::HEAD {
        Body::empty()
    } else {
        Body::from(data)
    };
    Ok(response.body(body).expect("valid programme headers"))
}
async fn respond_json_alias(
    state: EpgProgramReads,
    peer: SocketAddr,
    headers: HeaderMap,
    id: Option<String>,
    uri: Uri,
) -> Result<Response, StoreError> {
    negotiate_literal_json_alias(&headers)?;
    let selected = programme_media(&Method::GET, &Uri::from_static("/"), &headers);
    let indentation = match selected {
        Some(ProgrammeMedia::ExplicitJson { indentation }) => indentation,
        _ => 0,
    };
    respond(
        state,
        peer,
        headers,
        Method::GET,
        id,
        uri,
        Some(ProgrammeMedia::ExplicitJson { indentation }),
    )
    .await
}

async fn list_json_alias(
    State(state): State<EpgProgramReads>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Response, StoreError> {
    respond_json_alias(state, peer, headers, None, uri).await
}

async fn detail_json_alias(
    State(state): State<EpgProgramReads>,
    JsonReadId(id): JsonReadId,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Response, StoreError> {
    respond_json_alias(state, peer, headers, Some(id), uri).await
}

async fn programme_options(
    State(state): State<EpgProgramReads>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> Response {
    catalogue_options::respond(
        state.auth(),
        peer,
        headers,
        uri,
        catalogue_options::Kind::Programme,
    )
    .await
}

async fn list(
    State(state): State<EpgProgramReads>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    method: Method,
    uri: Uri,
) -> Result<Response, StoreError> {
    let media = programme_media(&method, &uri, &headers);
    if let Some(ProgrammeMedia::Html { format_alias }) = media {
        let user = state
            .auth
            .account_user(peer, headers.clone(), false)
            .await?;
        if user.user_level < 1 {
            return Err(StoreError::PermissionDenied);
        }
        let template = match (user.user_level >= 10, format_alias) {
            (true, true) => programme_html::Template::AdminFormatAlias,
            (true, false) => programme_html::Template::Admin,
            (false, true) => programme_html::Template::StandardFormatAlias,
            (false, false) => programme_html::Template::Standard,
        };
        let origin = origin(&state.auth, peer, &headers);
        let programmes = state.read_authorized(None, origin.clone(), 0).await?;
        return programme_html::response(
            &headers,
            method,
            template,
            &user.username,
            &origin,
            programmes,
        );
    }
    respond(state, peer, headers, method, None, uri, media).await
}
async fn detail(
    State(state): State<EpgProgramReads>,
    Path(id): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    method: Method,
    uri: Uri,
) -> Result<Response, StoreError> {
    let media = programme_media(&method, &uri, &headers);
    if let Some(ProgrammeMedia::Html { format_alias }) = media {
        let user = state
            .auth
            .account_user(peer, headers.clone(), false)
            .await?;
        if user.user_level < 1 {
            return Err(StoreError::PermissionDenied);
        }
        let path = uri.path();
        let template = match (user.user_level >= 10, format_alias) {
            (true, true) => programme_html::Template::AdminDetailFormatAlias(path),
            (true, false) => programme_html::Template::AdminDetail(path),
            (false, true) => programme_html::Template::StandardDetailFormatAlias(path),
            (false, false) => programme_html::Template::StandardDetail(path),
        };
        let origin = origin(&state.auth, peer, &headers);
        let programme = state.read_authorized(Some(&id), origin.clone(), 0).await?;
        return programme_html::response(
            &headers,
            method,
            template,
            &user.username,
            &origin,
            programme,
        );
    }
    respond(state, peer, headers, method, Some(id), uri, media).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::to_bytes,
        http::{Request, StatusCode},
        Extension,
    };
    use tower::ServiceExt;

    const BROWSER: &str = concat!(
        "text/html,application/xhtml+xml,application/xml;q=0.9,",
        "image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    );

    #[tokio::test]
    async fn seven_catalogue_json_aliases_use_protected_get_without_forcing_canonical_changes() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy_with(sqlx::postgres::PgConnectOptions::new());
        let auth = AuthService::new(
            crate::Store::from_pool(pool),
            crate::AuthConfig::new("owned-catalogue-alias-fixture".into()).unwrap(),
        );
        let reads = EpgProgramReads::new(auth.clone(), EpgProgramReadLimits::default()).unwrap();
        let peer: SocketAddr = "127.0.0.1:12345".parse().unwrap();
        let router = reads
            .router()
            .merge(crate::channel_groups::router(auth.clone()))
            .merge(crate::provider_reads::router(auth))
            .layer(Extension(ConnectInfo(peer)));
        for path in [
            "/api/channels/groups/1.json",
            "/api/m3u/server-groups.json",
            "/api/m3u/server-groups/1.json",
            "/api/epg/programs.json",
            "/api/epg/programs/1.json",
            "/api/epg/epgdata.json",
            "/api/epg/epgdata/1.json",
        ] {
            for (method, accept, query, expected) in [
                (
                    Method::GET,
                    "application/json",
                    "",
                    StatusCode::UNAUTHORIZED,
                ),
                (
                    Method::GET,
                    "application/json",
                    "?format=api",
                    StatusCode::UNAUTHORIZED,
                ),
                (Method::GET, "text/html", "", StatusCode::NOT_ACCEPTABLE),
                (Method::HEAD, "application/json", "", StatusCode::NOT_FOUND),
                (
                    Method::OPTIONS,
                    "application/json",
                    "",
                    StatusCode::NOT_FOUND,
                ),
                (Method::POST, "application/json", "", StatusCode::NOT_FOUND),
            ] {
                let request = Request::builder()
                    .method(method.clone())
                    .uri(format!("{path}{query}"))
                    .header(header::ACCEPT, accept)
                    .body(Body::empty())
                    .unwrap();
                let response = router.clone().oneshot(request).await.unwrap();
                assert_eq!(response.status(), expected, "{method} {path}{query}");
                assert!(!response.headers().contains_key(header::SET_COOKIE));
            }
        }
        for path in [
            "/api/channels/groups/1",
            "/api/channels/groups/1%2Ejson",
            "/api/m3u/server-groups/1.JSON",
            "/api/m3u/server-groups/1.json.json",
            "/api/epg/programs/1",
            "/api/epg/programs/1.csv",
            "/api/epg/epgdata/.json",
            "/api/epg/epgdata/-1.json",
        ] {
            let request = Request::builder()
                .uri(path)
                .header(header::ACCEPT, "application/json")
                .body(Body::empty())
                .unwrap();
            let response = router.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
        }
    }

    #[tokio::test]
    async fn programme_json_get_authorization_headers_are_route_and_representation_specific() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy_with(sqlx::postgres::PgConnectOptions::new());
        let auth = AuthService::new(
            crate::Store::from_pool(pool),
            crate::AuthConfig::new("programme-router-test-only".into()).unwrap(),
        );
        let reads = EpgProgramReads::new(auth, EpgProgramReadLimits::default()).unwrap();
        let peer: SocketAddr = "127.0.0.1:12345".parse().unwrap();
        let router = reads
            .clone()
            .router()
            .merge(reads.current_router())
            .layer(Extension(ConnectInfo(peer)));
        for (method, path, accept, allow, vary) in [
            (
                Method::GET,
                "/api/epg/programs/",
                "application/json",
                Some(PROGRAMME_LIST_ALLOW),
                Some(PROGRAMME_JSON_VARY),
            ),
            (
                Method::GET,
                "/api/epg/programs/7/",
                "application/json",
                Some(PROGRAMME_DETAIL_ALLOW),
                Some(PROGRAMME_JSON_VARY),
            ),
            (
                Method::GET,
                "/api/epg/current-programs/",
                "application/json",
                Some("POST, OPTIONS"),
                Some("Accept"),
            ),
            (
                Method::GET,
                "/api/epg/programs/7/?format=json",
                "application/json",
                None,
                None,
            ),
            (Method::GET, "/api/epg/programs/7/", "*/*", None, None),
            (
                Method::HEAD,
                "/api/epg/programs/7/",
                "application/json",
                None,
                None,
            ),
        ] {
            let request = Request::builder()
                .method(method.clone())
                .uri(path)
                .header(header::ACCEPT, accept)
                .body(Body::empty())
                .unwrap();
            let response = router.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{path}");
            let headers = response.headers();
            assert_eq!(
                headers.get(header::ALLOW).map(|v| v.to_str().unwrap()),
                allow,
                "{path}"
            );
            assert_eq!(
                headers.get(header::VARY).map(|v| v.to_str().unwrap()),
                vary,
                "{path}"
            );
            assert_eq!(headers[header::WWW_AUTHENTICATE], "Bearer realm=\"api\"");
            let bytes = to_bytes(response.into_body(), 128).await.unwrap();
            let expected: &[u8] = if method == Method::HEAD {
                b""
            } else {
                br#"{"detail":"Authentication credentials were not provided."}"#
            };
            assert_eq!(bytes.as_ref(), expected, "{path}");
        }
    }

    #[test]
    fn programme_json_get_only_decorates_observed_authorization_errors() {
        for (error, expected) in [
            (StoreError::PermissionDenied, true),
            (
                StoreError::AuthenticationBody(json!({"code": "user_inactive"})),
                true,
            ),
            (
                StoreError::AuthenticationBody(json!({"code": "user_not_found"})),
                false,
            ),
            (StoreError::NotFound, false),
            (unavailable(), false),
        ] {
            assert_eq!(programme_json_authorization_error(&error), expected);
        }
    }

    #[test]
    fn ordinary_programme_get_media_matches_captured_representations() {
        let html = Some(ProgrammeMedia::Html {
            format_alias: false,
        });
        let alias = Some(ProgrammeMedia::Html { format_alias: true });
        for base in ["/api/epg/programs/", "/api/epg/programs/7/"] {
            for (accept, suffix, expected) in [
                (BROWSER, "", html),
                ("text/*", "", html),
                (BROWSER, "?format=api", alias),
                (
                    "application/json;q=0.9, text/html;q=0.5",
                    "",
                    Some(ProgrammeMedia::ExplicitJson { indentation: 0 }),
                ),
                (
                    "application/json",
                    "",
                    Some(ProgrammeMedia::ExplicitJson { indentation: 0 }),
                ),
                ("TEXT/HTML, */*;Q=0.8", "", html),
                (
                    "APPLICATION/JSON;Q=0.9, text/html;q=0.5",
                    "",
                    Some(ProgrammeMedia::ExplicitJson { indentation: 0 }),
                ),
                (
                    "application/json; indent=4, text/html",
                    "",
                    Some(ProgrammeMedia::ExplicitJson { indentation: 4 }),
                ),
                ("*/*", "?format=api", alias),
            ] {
                let uri = format!("{base}{suffix}").parse().unwrap();
                let mut headers = HeaderMap::new();
                headers.insert("accept", HeaderValue::from_static(accept));
                assert_eq!(programme_media(&Method::GET, &uri, &headers), expected);
            }
        }
    }

    #[tokio::test]
    async fn indented_programme_rows_keep_source_bytes_and_the_output_budget() {
        let value = json!({"id":7,"title":"Guide é","nested":[1,{"key":true}]});
        let detail = r#"{
    "id": 7,
    "title": "Guide é",
    "nested": [
        1,
        {
            "key": true
        }
    ]
}"#;
        let sequence = r#"[
    {
        "id": 7,
        "title": "Guide é",
        "nested": [
            1,
            {
                "key": true
            }
        ]
    }
]"#;
        for (is_detail, expected) in [(true, detail), (false, sequence)] {
            let rows = || futures_util::stream::iter([Ok(value.clone())]);
            let mut out = BoundedJson::new(expected.len());
            write_programmes(
                &mut out,
                rows(),
                is_detail,
                PrettyFormatter::with_indent(b"    "),
            )
            .await
            .unwrap();
            assert_eq!(out.into_data(), expected.as_bytes());
            let mut limited = BoundedJson::new(expected.len() - 1);
            assert!(write_programmes(
                &mut limited,
                rows(),
                is_detail,
                PrettyFormatter::with_indent(b"    ")
            )
            .await
            .is_err());
        }
        let mut list = BoundedJson::new(16);
        write_programmes(
            &mut list,
            futures_util::stream::empty(),
            false,
            PrettyFormatter::with_indent(b"    "),
        )
        .await
        .unwrap();
        assert_eq!(list.into_data(), b"[]");
    }

    #[test]
    fn programme_media_leaves_other_query_shapes_and_head_negotiation_in_place() {
        let uri: Uri = "/api/epg/programs/7/".parse().unwrap();
        let alias: Uri = "/api/epg/programs/7/?format=api".parse().unwrap();
        let json: Uri = "/api/epg/programs/7/?format=json".parse().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("accept", HeaderValue::from_static(BROWSER));
        assert_eq!(programme_media(&Method::HEAD, &uri, &headers), None);
        assert_eq!(programme_media(&Method::GET, &json, &headers), None);
        headers.insert("accept", HeaderValue::from_static("text/html"));
        assert_eq!(programme_media(&Method::HEAD, &alias, &headers), None);
        assert_eq!(
            programme_media(&Method::HEAD, &uri, &headers),
            Some(ProgrammeMedia::Html {
                format_alias: false
            }),
        );
    }

    #[test]
    fn cloned_response_bytes_retain_admission_until_last_allocation_reference() {
        let workers = Arc::new(Semaphore::new(1));
        let lease = Arc::new(workers.clone().try_acquire_owned().unwrap());
        let data = Bytes::from_owner(OwnedJson {
            data: b"owned programme".to_vec(),
            _lease: lease,
        });
        let clone = data.clone();
        let slice = clone.slice(2..5);
        drop(data);
        drop(clone);
        assert!(workers.clone().try_acquire_owned().is_err());
        drop(slice);
        assert!(workers.clone().try_acquire_owned().is_ok());
    }
    #[test]
    fn escaped_json_expansion_cannot_exceed_serialization_allocation_budget() {
        let mut out = BoundedJson {
            data: Vec::new(),
            maximum: 128,
        };
        assert!(serde_json::to_writer(&mut out, &"\0".repeat(100)).is_err());
        assert!(out.data.len() <= 128);
        assert!(out.data.capacity() <= 128);
    }
}
