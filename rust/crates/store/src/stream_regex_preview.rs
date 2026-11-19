//! Stream auto-sync preview from Original Dispatcharr. Original authors retain credit.
use crate::{AuthService, RegexExecutor, StoreError};
use axum::{
    body::Body,
    extract::{ConnectInfo, RawQuery, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{Postgres, QueryBuilder};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};

const SCAN_LIMIT: usize = 5000;
const PATTERN_LIMIT: usize = 512;
const BATCH_NAMES: usize = 2048;
// Keep the old maximum name payload (512 names of 512 code points, each
// escaped to six bytes) while avoiding round trips for ordinary short names.
const BATCH_NAME_BYTES: usize = 512 * (512 * 6 + 3);
const REQUESTS: u32 = 2;
const REQUEST_BUDGET: Duration = Duration::from_secs(5);
const RESPONSE_LIMIT: usize = 1024 * 1024;
const PATH: &str = "/api/channels/streams/regex-preview/";
const FROM: &str = " FROM dispatcharr_channels_stream s JOIN dispatcharr_channels_channelgroup g ON g.id=s.channel_group_id WHERE g.name=";

#[derive(Clone)]
pub struct StreamRegexPreview {
    inner: Arc<Runtime>,
    auth: AuthService,
}

struct Runtime {
    regex: RegexExecutor,
    slots: Arc<Semaphore>,
    closed: AtomicBool,
    used: AtomicBool,
}

struct Rendered {
    body: String,
    _owner: Arc<OwnedSemaphorePermit>,
}

impl AsRef<[u8]> for Rendered {
    fn as_ref(&self) -> &[u8] {
        self.body.as_bytes()
    }
}

impl StreamRegexPreview {
    pub fn prepare(auth: AuthService) -> Self {
        Self {
            inner: Arc::new(Runtime {
                regex: RegexExecutor::prepare(
                    "/usr/local/bin/dispatcharr-regex-python".into(),
                    Arc::new(()),
                ),
                slots: Arc::new(Semaphore::new(REQUESTS as usize)),
                closed: AtomicBool::new(false),
                used: AtomicBool::new(false),
            }),
            auth,
        }
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route(PATH, any(handle))
            .with_state(self.clone())
    }

    pub fn close_admission(&self) {
        self.inner.closed.store(true, Ordering::Release);
    }

    pub fn was_used(&self) -> bool {
        self.inner.used.load(Ordering::Acquire)
    }

    pub async fn shutdown(&self, timeout: Duration) -> bool {
        self.close_admission();
        let deadline = tokio::time::Instant::now() + timeout;
        let regex = &self.inner.regex;
        regex.interrupt_at(deadline);
        let slots = tokio::time::timeout_at(
            deadline,
            self.inner.slots.clone().acquire_many_owned(REQUESTS),
        )
        .await
        .is_ok_and(|result| result.is_ok());
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match regex.shutdown(remaining).await {
            Ok(Some(receipt)) => slots && receipt.wait_observed,
            Ok(None) => {
                while regex.no_spawn_retirement().is_none() {
                    if tokio::time::Instant::now() >= deadline {
                        return false;
                    }
                    tokio::task::yield_now().await;
                }
                slots
            }
            Err(_) => false,
        }
    }

    fn admit(&self) -> Result<Arc<OwnedSemaphorePermit>, StoreError> {
        let permit = self
            .inner
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| unavailable())?;
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        self.inner.used.store(true, Ordering::Release);
        Ok(Arc::new(permit))
    }
}

fn unavailable() -> StoreError {
    StoreError::StatusBody(
        503,
        json!({"detail":"Stream regex preview capacity unavailable."}),
    )
}

fn child_deadline() -> Result<u64, StoreError> {
    let mut clock = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // Python's time.monotonic_ns() reads this same Linux clock.
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut clock) } != 0 {
        return Err(unavailable());
    }
    let nanos = (clock.tv_sec as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(clock.tv_nsec as u64);
    Ok(nanos.saturating_add(REQUEST_BUDGET.as_nanos() as u64))
}

fn bad(detail: impl Into<String>) -> StoreError {
    StoreError::InvalidInput(json!({"detail":detail.into()}))
}

struct Patterns {
    find: Option<String>,
    replace: String,
    include: Option<String>,
    exclude: Option<String>,
    limit: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Account {
    All,
    Id(i64),
    OutsideRange,
}

struct Preview {
    group: String,
    account: Account,
    patterns: Patterns,
}

fn integer(value: &str) -> Option<String> {
    if value.contains('\0') {
        return None;
    }
    dispatcharr_ingest_core::source_integer_literal(value)
}

impl Preview {
    fn parse(params: &HashMap<String, String>) -> Result<Self, StoreError> {
        let nonempty = |key| {
            params
                .get(key)
                .map(String::as_str)
                .filter(|s| !s.is_empty())
        };
        let group = nonempty("channel_group").ok_or_else(|| bad("channel_group is required"))?;
        let account = match params.get("m3u_account_id") {
            None => Account::All,
            Some(value) => integer(value)
                .ok_or_else(|| bad("m3u_account_id must be an integer"))?
                .parse()
                .map_or(Account::OutsideRange, Account::Id),
        };
        for key in ["find", "replace", "match", "exclude"] {
            if nonempty(key).is_some_and(|value| value.chars().count() > PATTERN_LIMIT) {
                return Err(bad(format!("{key} exceeds {PATTERN_LIMIT} characters")));
            }
        }
        let limit = params
            .get("limit")
            .and_then(|value| integer(value))
            .map_or(10, |value| {
                value
                    .parse::<i64>()
                    .unwrap_or_else(|_| if value.starts_with('-') { 1 } else { 50 })
                    .clamp(1, 50) as usize
            });
        Ok(Self {
            group: group.to_owned(),
            account,
            patterns: Patterns {
                find: nonempty("find").map(str::to_owned),
                replace: nonempty("replace").unwrap_or("").to_owned(),
                include: nonempty("match").map(str::to_owned),
                exclude: nonempty("exclude").map(str::to_owned),
                limit,
            },
        })
    }

    fn query(&self, select: &'static str) -> QueryBuilder<'_, Postgres> {
        let mut query = QueryBuilder::new(select);
        query.push(FROM).push_bind(self.group.as_str());
        if let Account::Id(account) = self.account {
            query.push(" AND s.m3u_account_id=").push_bind(account);
        }
        query
    }

    async fn names(&self, auth: &AuthService) -> Result<(i64, Vec<String>), StoreError> {
        if self.account == Account::OutsideRange {
            return Ok((0, Vec::new()));
        }
        let count = self
            .query("SELECT COUNT(*)")
            .build_query_scalar()
            .fetch_one(auth.store.pool())
            .await?;
        let names = self
            .query("SELECT s.name")
            .push(" ORDER BY s.updated_at DESC LIMIT ")
            .push_bind(SCAN_LIMIT as i64)
            .build_query_scalar()
            .fetch_all(auth.store.pool())
            .await?;
        Ok((count, names))
    }
}

#[derive(Deserialize, Serialize)]
struct Rename {
    before: String,
    after: String,
}

#[derive(Deserialize, Serialize)]
struct Match {
    name: String,
    matches: bool,
}

#[derive(Deserialize)]
struct Batch<T> {
    samples: Vec<T>,
    count: usize,
    error: Option<String>,
}

#[derive(Deserialize)]
struct Matches {
    find: Batch<Rename>,
    include: Batch<Match>,
    exclude: Batch<Match>,
}

impl<T> Default for Batch<T> {
    fn default() -> Self {
        Self {
            samples: Vec::new(),
            count: 0,
            error: None,
        }
    }
}

impl<T> Batch<T> {
    fn merge(&mut self, batch: Self, limit: usize) {
        self.samples
            .extend(batch.samples.into_iter().take(limit - self.samples.len()));
        self.count += batch.count;
        if batch.error.is_some() {
            self.error = batch.error;
        }
    }
}

#[derive(Serialize)]
struct FindResult {
    find_matches: Vec<Rename>,
    find_match_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    find_error: Option<String>,
}

#[derive(Serialize)]
struct FilterResult {
    filter_matches: Vec<Match>,
    filter_match_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    match_error: Option<String>,
}

#[derive(Serialize)]
struct ExcludeResult {
    exclude_matches: Vec<Match>,
    exclude_match_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    exclude_error: Option<String>,
}

#[derive(Serialize)]
struct Results {
    total_in_group: i64,
    total_scanned: usize,
    scan_limit_hit: bool,
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    find: Option<FindResult>,
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    include: Option<FilterResult>,
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    exclude: Option<ExcludeResult>,
}

fn batches(mut names: &[String]) -> impl Iterator<Item = &[String]> {
    let mut compile_empty = names.is_empty();
    std::iter::from_fn(move || {
        if std::mem::take(&mut compile_empty) {
            return Some(names);
        }
        if names.is_empty() {
            return None;
        }
        let mut bytes = 0usize;
        let count = names
            .iter()
            .take(BATCH_NAMES)
            .take_while(|name| {
                // Each code point costs at most six bytes in the JSON frame.
                // The three extra bytes cover the string quotes and separator.
                let encoded = name.chars().count().saturating_mul(6).saturating_add(3);
                bytes = bytes.saturating_add(encoded);
                bytes <= BATCH_NAME_BYTES
            })
            .count()
            .max(1);
        let batch;
        (batch, names) = names.split_at(count);
        Some(batch)
    })
}

async fn preview(
    regex: &RegexExecutor,
    owner: Arc<OwnedSemaphorePermit>,
    patterns: Patterns,
    count: i64,
    names: &[String],
    child_deadline_ns: u64,
) -> Result<Results, StoreError> {
    let limit = patterns.limit;
    let mut find = Batch::default();
    let mut include = Batch::default();
    let mut exclude = Batch::default();
    // The empty batch still compiles supplied patterns for an empty group.
    for names in batches(names) {
        let find_pattern = patterns.find.as_deref().filter(|_| find.error.is_none());
        let include_pattern = patterns
            .include
            .as_deref()
            .filter(|_| include.error.is_none());
        let exclude_pattern = patterns
            .exclude
            .as_deref()
            .filter(|_| exclude.error.is_none());
        if find_pattern.is_none() && include_pattern.is_none() && exclude_pattern.is_none() {
            break;
        }
        let reply = regex
            .execute(
                json!({"kind":"stream_preview", "names":names, "config":{
                    "find":find_pattern, "replace":patterns.replace,
                    "match":include_pattern, "exclude":exclude_pattern, "limit":limit,
                    "deadline_ns":child_deadline_ns,
                }}),
                owner.clone(),
            )
            .await
            .map_err(|error| match error {
                crate::plugin_python_host::Error::Remote(value)
                    if value["actual_class"] != "CompatibilityUnavailable" =>
                {
                    StoreError::UnhandledRestFault
                }
                _ => unavailable(),
            })?;
        let value: Matches = serde_json::from_value(reply).map_err(|_| unavailable())?;
        find.merge(value.find, limit);
        include.merge(value.include, limit);
        exclude.merge(value.exclude, limit);
    }
    Ok(Results {
        total_in_group: count,
        total_scanned: names.len(),
        scan_limit_hit: count > SCAN_LIMIT as i64,
        find: patterns.find.map(|_| FindResult {
            find_matches: find.samples,
            find_match_count: find.count,
            find_error: find.error,
        }),
        include: patterns.include.map(|_| FilterResult {
            filter_matches: include.samples,
            filter_match_count: include.count,
            match_error: include.error,
        }),
        exclude: patterns.exclude.map(|_| ExcludeResult {
            exclude_matches: exclude.samples,
            exclude_match_count: exclude.count,
            exclude_error: exclude.error,
        }),
    })
}

async fn handle(
    State(service): State<StreamRegexPreview>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    method: Method,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Response {
    let result: Result<Response, StoreError> = async {
        let permit = service.auth.task_sql_writer()?;
        let auth = service.auth.clone().with_request_writer_permit(permit)?;
        let user = auth.account_user(peer, headers, false).await?;
        if user.user_level < 1 {
            return Err(StoreError::PermissionDenied);
        }
        if method == Method::OPTIONS {
            return Ok((
                [(header::CONTENT_TYPE, "application/json")],
                r#"{"name":"Stream","description":"","renders":["application/json","text/html"],"parses":["application/json","application/x-www-form-urlencoded","multipart/form-data"]}"#,
            )
                .into_response());
        }
        if method != Method::GET && method != Method::HEAD {
            return Err(StoreError::StatusBody(
                405,
                json!({"detail": format!("Method \"{method}\" not allowed.")}),
            ));
        }
        // QueryDict.get observes the last occurrence in the URL.
        let params: HashMap<_, _> = url::form_urlencoded::parse(raw.as_deref().unwrap_or("").as_bytes())
            .into_owned()
            .collect();
        let request = Preview::parse(&params)?;
        let owner = service.admit()?;
        let deadline = Instant::now() + REQUEST_BUDGET;
        let deadline_ns = child_deadline()?;
        // Only the SQL stage keeps a writer permit. A disconnected caller
        // cannot release it before the queries settle or schedule a new batch.
        let sql_owner = owner.clone();
        let sql = tokio::spawn(async move {
            let _owner = sql_owner;
            let (count, names) = request.names(&auth).await?;
            Ok::<_, StoreError>((request.patterns, count, names))
        });
        let (patterns, count, names) = tokio::time::timeout_at(deadline, sql)
            .await
            .map_err(|_| unavailable())?
            .map_err(|_| StoreError::UnhandledRestFault)??;
        let dependency = preview(&service.inner.regex, owner.clone(), patterns, count, &names, deadline_ns);
        let result = tokio::time::timeout_at(deadline, dependency)
            .await
            .map_err(|_| unavailable())??;
        let body = serde_json::to_string(&result)
            .map_err(|_| StoreError::UnhandledRestFault)?
            .replace('\u{2028}', "\\u2028")
            .replace('\u{2029}', "\\u2029");
        if body.len() > RESPONSE_LIMIT {
            return Err(unavailable());
        }
        let length = body.len().to_string();
        let body = if method == Method::HEAD {
            Body::empty()
        } else {
            Body::from(Bytes::from_owner(Rendered {
                body,
                _owner: owner,
            }))
        };
        Ok((
            [(header::CONTENT_TYPE, "application/json"), (header::CONTENT_LENGTH, length.as_str())],
            body,
        )
            .into_response())
    }
    .await;
    let mut response = result.unwrap_or_else(IntoResponse::into_response);
    if response.status() != StatusCode::INTERNAL_SERVER_ERROR {
        response
            .headers_mut()
            .insert(header::VARY, HeaderValue::from_static("Accept"));
        response.headers_mut().insert(
            header::ALLOW,
            HeaderValue::from_static("GET, HEAD, OPTIONS"),
        );
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_normalizes_python_account_and_sample_limit() {
        let parameters = HashMap::from([
            ("channel_group".into(), "Sports".into()),
            ("m3u_account_id".into(), "+١_٢".into()),
            ("limit".into(), "999999999999999999999999999".into()),
            ("find".into(), String::new()),
        ]);
        let query = Preview::parse(&parameters).unwrap();
        assert_eq!(query.account, Account::Id(12));
        assert_eq!(query.patterns.limit, 50);
        assert!(query.patterns.find.is_none());
        let mut parameters = parameters;
        parameters.insert("m3u_account_id".into(), String::new());
        assert!(Preview::parse(&parameters).is_err());
    }

    #[test]
    fn batches_reduce_round_trips_and_keep_wide_frames_bounded() {
        let short: Vec<_> = (0..SCAN_LIMIT).map(|id| format!("Channel {id}")).collect();
        assert_eq!(
            batches(&short).map(<[_]>::len).collect::<Vec<_>>(),
            [2048, 2048, 904]
        );
        assert!(batches(&short).flatten().eq(short.iter()));

        let wide = vec!["😀".repeat(512); 513];
        assert_eq!(batches(&wide).map(<[_]>::len).collect::<Vec<_>>(), [512, 1]);
        assert_eq!(batches(&[]).map(<[_]>::len).collect::<Vec<_>>(), [0]);
    }

    #[test]
    fn result_merges_samples_in_scan_order_with_exact_optionals() {
        let mut first = Batch {
            samples: vec!["new"],
            count: 3,
            error: None,
        };
        first.merge(
            Batch {
                samples: vec!["middle", "old"],
                count: 2,
                error: Some("Pattern timed out: fixture".into()),
            },
            2,
        );
        assert_eq!(first.samples, ["new", "middle"]);
        assert_eq!(first.count, 5);
        assert_eq!(first.error.as_deref(), Some("Pattern timed out: fixture"));
        let results = Results {
            total_in_group: 5001,
            total_scanned: 5000,
            scan_limit_hit: true,
            find: None,
            include: Some(FilterResult {
                filter_matches: vec![],
                filter_match_count: 0,
                match_error: None,
            }),
            exclude: None,
        };
        assert_eq!(
            serde_json::to_string(&results).unwrap(),
            concat!(
                r#"{"total_in_group":5001,"total_scanned":5000,"scan_limit_hit":true,"#,
                r#""filter_matches":[],"filter_match_count":0}"#,
            )
        );
    }
}
