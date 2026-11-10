//! Prepared bounded refresh/ETA authority, retained
//! HTTP publication and ordered shutdown. Original Dispatcharr authors retain
//! full application/algorithm credit. No scheduler or generic task consumer.
//! Source/native HTTP/import/shutdown gates are captured separately from each
//! packaged application build. Non-integer IDs and other FIFO families remain
//! explicit unsupported scope; no whole-job-system acceptance is implied.
use crate::{
    refresh_protocol::{RefreshCall, RefreshMessage},
    refresh_worker::{RefreshExecutor, ServiceExecutor},
    scheduled_refresh_driver::{Config as DriverConfig, ScheduledRefreshDriver},
    transport::{Config as JobsConfig, Jobs},
};
use axum::{
    body::to_bytes,
    extract::{ConnectInfo, Request, State},
    http::{Method, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Json, Router,
};
use dispatcharr_contracts::{BoxFuture, UpdateEventSink};
use dispatcharr_ingest_epg::{
    cache_policy::{CachedSwapPolicy, PolicyClock},
    channels::{ChannelConfig, ChannelImporter},
    file_job::{ChildTask, FileJobConfig, FileJobRunner, FileJobServices, GuideTaskDispatcher},
    reporter::GuideReporter,
    EpgImporter, ImportConfig, ImportError, SwapPolicyContext,
};
use dispatcharr_store::{AuthService, Store, StoreError};
use serde_json::{json, Value};
use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

struct NoUnboundDispatch;
impl GuideTaskDispatcher for NoUnboundDispatch {
    fn dispatch<'a>(
        &'a self,
        _task: ChildTask,
        _context: SwapPolicyContext,
    ) -> BoxFuture<'a, Result<(), ImportError>> {
        Box::pin(async { Err(ImportError::Worker) })
    }
}
struct HttpOwner {
    _permit: OwnedSemaphorePermit,
}
#[derive(Clone)]
pub struct RefreshApplication {
    inner: Arc<Inner>,
}
struct Inner {
    auth: AuthService,
    store: Store,
    jobs: Jobs,
    driver: Mutex<Option<ScheduledRefreshDriver>>,
    executor: Arc<dyn RefreshExecutor>,
    runner: FileJobRunner,
    reporter: GuideReporter,
    channels: ChannelImporter,
    programmes: EpgImporter,
    closed: AtomicBool,
    http: Arc<Semaphore>,
}
pub struct RefreshApplicationOptions {
    pub broker: String,
    pub backend: String,
    pub cache: String,
    pub cache_dir: PathBuf,
}
impl RefreshApplication {
    /// Compatibility constructor: prepare resources, then explicitly start.
    /// New application assembly can prepare all families before opening intake.
    pub async fn connect(
        store: Store,
        auth: AuthService,
        sink: Arc<dyn UpdateEventSink>,
        options: RefreshApplicationOptions,
    ) -> Result<Self, &'static str> {
        let service = Self::prepare(store, auth, sink, options).await?;
        service
            .start()
            .map_err(|_| "Refresh driver configuration failed")?;
        Ok(service)
    }
    pub async fn prepare(
        store: Store,
        auth: AuthService,
        sink: Arc<dyn UpdateEventSink>,
        options: RefreshApplicationOptions,
    ) -> Result<Self, &'static str> {
        let broker = options.broker.as_str();
        let backend = options.backend.as_str();
        let cache = options.cache.as_str();
        let cache_dir = options.cache_dir;
        let jobs = Jobs::connect_refresh(
            broker,
            backend,
            JobsConfig {
                operations: 4,
                command_timeout: Duration::from_secs(2),
                max_queued_jobs: 10000,
            },
        )
        .await
        .map_err(|_| "Refresh broker initialization failed")?;
        let clock: PolicyClock = Arc::new(chrono::Utc::now);
        let reporter = GuideReporter::with_clock(store.clone(), cache, sink, 2, clock.clone())
            .await
            .map_err(|_| "Refresh reporter initialization failed")?;
        let channels = ChannelImporter::new(store.clone(), ChannelConfig::default())
            .map_err(|_| "Refresh channel configuration failed")?;
        let programmes = EpgImporter::new(store.clone(), ImportConfig::default())
            .map_err(|_| "Refresh programme configuration failed")?;
        let locks = dispatcharr_coordination::TaskLocks::connect(
            cache,
            dispatcharr_coordination::TaskLockConfig::default(),
        )
        .await
        .map_err(|_| "Refresh lock initialization failed")?;
        let runner = FileJobRunner::connect(
            FileJobServices {
                store: store.clone(),
                locks,
                reporter: reporter.clone(),
                channels: channels.clone(),
                programmes: programmes.clone(),
                policy: Arc::new(CachedSwapPolicy::with_clock(
                    store.clone(),
                    Some(cache.to_owned()),
                    clock.clone(),
                )),
                dispatcher: Arc::new(NoUnboundDispatch),
                clock: clock.clone(),
            },
            FileJobConfig {
                max_jobs: 1,
                cache_dir,
                release_timeout: Duration::from_secs(5),
            },
            cache,
        )
        .await
        .map_err(|_| "Refresh service initialization failed")?;
        let executor: Arc<dyn RefreshExecutor> =
            Arc::new(ServiceExecutor::new(runner.clone(), store.clone(), clock));
        Ok(Self {
            inner: Arc::new(Inner {
                auth,
                store,
                jobs,
                driver: Mutex::new(None),
                executor,
                runner,
                reporter,
                channels,
                programmes,
                closed: AtomicBool::new(false),
                http: Arc::new(Semaphore::new(4)),
            }),
        })
    }
    pub fn start(&self) -> Result<(), crate::refresh_worker::Error> {
        let mut driver = self.inner.driver.lock().unwrap();
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(crate::refresh_worker::Error::Closed);
        }
        if driver.is_some() {
            return Err(crate::refresh_worker::Error::Configuration);
        }
        *driver = Some(ScheduledRefreshDriver::start(
            self.inner.jobs.clone(),
            self.inner.executor.clone(),
            DriverConfig::default(),
            Arc::new(()),
        )?);
        Ok(())
    }
    pub fn last_intake_error(&self) -> Option<crate::transport::Error> {
        self.inner
            .driver
            .lock()
            .unwrap()
            .as_ref()
            .and_then(ScheduledRefreshDriver::last_intake_error)
    }
    pub fn router(self) -> Router {
        Router::new()
            .route("/api/epg/import/", any(import))
            .with_state(self)
    }
    pub async fn shutdown(&self, grace: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + grace;
        let left = || deadline.saturating_duration_since(tokio::time::Instant::now());
        self.inner.closed.store(true, Ordering::Release);
        let Ok(Ok(_http)) =
            tokio::time::timeout_at(deadline, self.inner.http.clone().acquire_many_owned(4)).await
        else {
            return false;
        };
        let driver = self.inner.driver.lock().unwrap().clone();
        if let Some(driver) = driver {
            if driver.shutdown(left()).await.is_err() {
                return false;
            }
        }
        // Consumer drain precedes service closure: accepted work may still need
        // its reporter and broker child/result reply during graceful shutdown.
        if !self.inner.runner.shutdown(left()).await {
            return false;
        }
        if !self.inner.channels.shutdown(left()).await {
            return false;
        }
        if !self.inner.programmes.shutdown(left()).await {
            return false;
        }
        if !self.inner.reporter.shutdown(left()).await {
            return false;
        }
        self.inner.jobs.shutdown(left()).await.is_ok()
    }
    async fn request(&self, peer: SocketAddr, request: Request, owner: Arc<HttpOwner>) -> Response {
        let method = request.method().clone();
        let read_permission = method == Method::GET || method == Method::HEAD;
        let auth = self.inner.auth.clone().with_retained_scope(owner.clone());
        let user = match auth
            .account_user(peer, request.headers().clone(), !read_permission)
            .await
        {
            Ok(user) => user,
            Err(error) => return error.into_response(),
        };
        if read_permission && user.user_level < 1 {
            return StoreError::PermissionDenied.into_response();
        }
        if method == Method::OPTIONS {
            return Json(json!({"name":"Epg Import Api","description":"Triggers an EPG data refresh","renders":["application/json","text/html"],"parses":["application/json","application/x-www-form-urlencoded","multipart/form-data"]})).into_response();
        }
        if method != Method::POST {
            return (
                StatusCode::METHOD_NOT_ALLOWED,
                Json(json!({"detail":format!("Method \"{method}\" not allowed.")})),
            )
                .into_response();
        }
        let raw = match to_bytes(request.into_body(), 64 * 1024).await {
            Ok(raw) => raw,
            Err(_) => {
                return (
                    StatusCode::PAYLOAD_TOO_LARGE,
                    Json(json!({"error":"Import body limit exceeded"})),
                )
                    .into_response()
            }
        };
        let value: Value = match serde_json::from_slice(&raw) { Ok(value) => value, Err(error) => return (StatusCode::BAD_REQUEST, Json(json!({"detail":format!("JSON parse error - {}", dispatcharr_store::source_json_error(&raw,error))}))).into_response() };
        let Some(object) = value.as_object() else {
            return source_500();
        };
        let id = object.get("id").unwrap_or(&Value::Null);
        // The Source view forwards these other scalar forms unchanged. They
        // need separate typed protocol/execution support; do not fake a202.
        let Some(source_id) = id.as_i64().filter(|id| *id > 0) else {
            return (
                StatusCode::NOT_IMPLEMENTED,
                Json(json!({"error":"This refresh ID shape has not been migrated"})),
            )
                .into_response();
        };
        let dummy = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM epg_epgsource WHERE id=$1 AND source_type='dummy')",
        )
        .bind(source_id)
        .fetch_one(self.inner.store.pool())
        .await;
        match dummy {
            Ok(true) => return (StatusCode::BAD_REQUEST, Json(json!({"success":false,"message":"Dummy EPG sources do not require refreshing."}))).into_response(),
            Ok(false) => {},
            Err(_) => return source_500(),
        }
        let force = object.get("force").is_some_and(python_truth);
        let task_id = Uuid::new_v4();
        let message = RefreshMessage {
            task_id,
            source_id,
            force,
            retry: 0,
            root: task_id,
            parent: None,
            eta: None,
            call: RefreshCall {
                args: vec![json!(source_id)],
                kwargs: json!({"force":force}).as_object().unwrap().clone(),
                list_args: false,
            },
        };
        match self.inner.jobs.publish_refresh(&message, owner).await {
            Ok(_) => (
                StatusCode::ACCEPTED,
                Json(json!({"success":true,"message":"EPG data refresh initiated."})),
            )
                .into_response(),
            Err(_) => source_500(),
        }
    }
}
fn python_truth(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(v) => *v,
        Value::Number(v) => v.as_f64() != Some(0.0),
        Value::String(v) => !v.is_empty(),
        Value::Array(v) => !v.is_empty(),
        Value::Object(v) => !v.is_empty(),
    }
}
fn source_500() -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, [("content-type","text/html; charset=utf-8")], "\n<!doctype html>\n<html lang=\"en\">\n<head>\n  <title>Server Error (500)</title>\n</head>\n<body>\n  <h1>Server Error (500)</h1><p></p>\n</body>\n</html>\n").into_response()
}
async fn import(
    State(service): State<RefreshApplication>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Response {
    if service.inner.closed.load(Ordering::Acquire) {
        return (StatusCode::SERVICE_UNAVAILABLE, "Refresh intake stopping").into_response();
    }
    let Ok(permit) = service.inner.http.clone().try_acquire_owned() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "Refresh request admission full",
        )
            .into_response();
    };
    let owner = Arc::new(HttpOwner { _permit: permit });
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let result = service.request(peer, request, owner).await;
        let _ = tx.send(result);
    });
    match tokio::time::timeout(Duration::from_secs(5), rx).await {
        Ok(Ok(response)) => response,
        _ => (
            StatusCode::SERVICE_UNAVAILABLE,
            "Refresh publication remains unconfirmed",
        )
            .into_response(),
    }
}

#[cfg(test)]
#[path = "refresh_application_peer.rs"]
mod peer;
