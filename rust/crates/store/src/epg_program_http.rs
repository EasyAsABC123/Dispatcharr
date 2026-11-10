//! Stored programme JSON reads derived from original Dispatcharr.
//! Original authors retain full credit. Independent router until packaged gates.
use crate::{AuthService, EpgReadResource, StoreError};
use axum::{
    body::Body,
    extract::{ConnectInfo, Path, State},
    http::{HeaderMap, Uri},
    response::Response,
    routing::get,
    Router,
};
use bytes::Bytes;
use futures_util::TryStreamExt;
use serde_json::json;
use sqlx::{Acquire, Row};
use std::{
    io::{self, Write},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};

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
            let result = state
                .auth
                .store
                .current_programmes_db(selection, now, 50000, state.limits.response_bytes)
                .await
                .map(|data| {
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
        })
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
        let user = self.auth.account_user(peer, headers, false).await?;
        if user.user_level < 1 {
            return Err(StoreError::PermissionDenied);
        }
        let id = raw_id
            .map(|raw| {
                let number = dispatcharr_ingest_core::source_integer_literal(raw)
                    .ok_or(StoreError::NotFound)?;
                number
                    .parse::<i64>()
                    .map_err(|_| EpgReadResource::Programs.missing())
            })
            .transpose()?;
        let lease = Arc::new(
            self.workers
                .clone()
                .try_acquire_owned()
                .map_err(|_| unavailable())?,
        );
        let state = self.clone();
        let (sender, receiver) = oneshot::channel();
        tokio::spawn(async move {
            let result = state.query(id, &origin, lease).await;
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
    ) -> Result<Bytes, StoreError> {
        let mut conn = self.auth.store.pool().acquire().await?;
        let mut tx = conn.begin().await?;
        sqlx::query("SELECT set_config('statement_timeout',$1,true)")
            .bind(format!("{}ms", self.limits.deadline.as_millis()))
            .execute(&mut *tx)
            .await?;
        // Keep the original select_related join. Gate each text/JSON column in
        // SQL, before the client decoder allocates a potentially oversized row.
        let size="coalesce(octet_length(p.title),0)::bigint+coalesce(octet_length(p.sub_title),0)+coalesce(octet_length(p.description),0)+coalesce(octet_length(p.tvg_id),0)+coalesce(octet_length(p.custom_properties::text),0)";
        let fields=format!("p.id::bigint AS id,p.start_time,p.end_time,({size}>$2) AS oversized,CASE WHEN {size}<=$2 THEN p.title ELSE '' END AS title,CASE WHEN {size}<=$2 THEN p.sub_title END AS sub_title,CASE WHEN {size}<=$2 THEN p.description END AS description,CASE WHEN {size}<=$2 THEN p.tvg_id END AS tvg_id,CASE WHEN {size}<=$2 THEN p.custom_properties END AS custom_properties");
        let relation = "FROM epg_programdata p JOIN epg_epgdata e ON e.id=p.epg_id";
        let sql=format!("SELECT {fields} {relation} WHERE $1::bigint IS NULL UNION ALL SELECT {fields} {relation} WHERE p.id=$1::bigint");
        let result = async {
            let mut rows = sqlx::query(&sql)
                .bind(id)
                .bind(self.limits.row_bytes as i64)
                .fetch(&mut *tx);
            let mut out = BoundedJson {
                data: Vec::new(),
                maximum: self.limits.response_bytes,
            };
            let mut count = 0;
            if id.is_none() {
                out.write_all(b"[").map_err(|_| unavailable())?;
            }
            while let Some(row) = rows.try_next().await? {
                if row.try_get::<bool, _>("oversized")? {
                    return Err(unavailable());
                }
                let value = EpgReadResource::Programs.project(row, id.is_some(), origin)?;
                if count > 0 {
                    out.write_all(b",").map_err(|_| unavailable())?;
                }
                serde_json::to_writer(&mut out, &value).map_err(|_| unavailable())?;
                count += 1;
            }
            if id.is_some() && count == 0 {
                return Err(EpgReadResource::Programs.missing());
            }
            if id.is_none() {
                out.write_all(b"]").map_err(|_| unavailable())?;
            }
            Ok(out.data)
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
            .route("/api/epg/programs/", get(list))
            .route("/api/epg/programs/{id}/", get(detail))
            .with_state(self)
    }
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
async fn respond(
    state: EpgProgramReads,
    peer: SocketAddr,
    headers: HeaderMap,
    id: Option<String>,
    uri: Uri,
) -> Result<Response, StoreError> {
    json_negotiation(&headers, &uri)?;
    let origin = origin(&state.auth, peer, &headers);
    let data = state.read(peer, headers, id.as_deref(), origin).await?;
    Ok(Response::builder()
        .header("content-type", "application/json")
        .body(Body::from(data))
        .expect("static programme headers"))
}
async fn list(
    State(state): State<EpgProgramReads>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Response, StoreError> {
    respond(state, peer, headers, None, uri).await
}
async fn detail(
    State(state): State<EpgProgramReads>,
    Path(id): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Response, StoreError> {
    respond(state, peer, headers, Some(id), uri).await
}

#[cfg(test)]
mod tests {
    use super::*;
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
