//! Prepared bounded refresh/ETA authority, retained
//! HTTP publication and ordered shutdown. Original Dispatcharr authors retain
//! full application/algorithm credit. No scheduler or generic task consumer.
//! Source/native HTTP/import/shutdown gates are captured separately from each
//! packaged application build. Non-integer IDs and other FIFO families remain
//! explicit unsupported scope; no whole-job-system acceptance is implied.
use crate::{
    refresh_protocol::{canonical_decimal_source_id, RefreshCall, RefreshMessage},
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
    service: Arc<ServiceExecutor>,
    series_handoff: Option<crate::refresh_series_handoff::Handoff>,
    runner: FileJobRunner,
    reporter: GuideReporter,
    channels: ChannelImporter,
    programmes: EpgImporter,
    normal_dispatch: AtomicBool,
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
        Self::prepare_inner(store, auth, sink, options, false).await
    }
    /// Explicit paired consumer assembly. Legacy standalone preparation does
    /// not retain child handoffs when no configured consumer is attached.
    pub async fn prepare_configured(
        store: Store,
        auth: AuthService,
        sink: Arc<dyn UpdateEventSink>,
        options: RefreshApplicationOptions,
    ) -> Result<(Self, crate::refresh_series_handoff::Handoff), &'static str> {
        let service = Self::prepare_inner(store, auth, sink, options, true).await?;
        let Some(handoff) = service.inner.series_handoff.clone() else {
            let _ = service.shutdown(Duration::from_secs(4)).await;
            return Err("Missing configured Series handoff");
        };
        Ok((service, handoff))
    }
    async fn prepare_inner(
        store: Store,
        auth: AuthService,
        sink: Arc<dyn UpdateEventSink>,
        options: RefreshApplicationOptions,
        configured_series: bool,
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
        let series_handoff = configured_series
            .then(|| crate::refresh_series_handoff::Handoff::prepare(jobs.clone()));
        let service = Arc::new(ServiceExecutor::new(runner.clone(), store.clone(), clock));
        let executor: Arc<dyn RefreshExecutor> = match &series_handoff {
            Some(handoff) => Arc::new(crate::refresh_series_handoff::Executor {
                inner: service.clone(),
                handoff: handoff.clone(),
            }),
            None => service.clone(),
        };
        Ok(Self {
            inner: Arc::new(Inner {
                auth,
                store,
                jobs,
                driver: Mutex::new(None),
                executor,
                service,
                series_handoff,
                runner,
                reporter,
                channels,
                programmes,
                normal_dispatch: AtomicBool::new(false),
                closed: AtomicBool::new(false),
                http: Arc::new(Semaphore::new(4)),
            }),
        })
    }
    pub fn into_normal_handler_with_hooks(
        self,
        download: dispatcharr_store::xmltv_download::Downloader,
        runtime: dispatcharr_store::PluginRuntime,
    ) -> Result<Arc<dyn crate::milestone_dispatch::Handler>, &'static str> {
        self.inner
            .service
            .bind_hooks(self.inner.auth.clone(), runtime)
            .map_err(|_| "normal XMLTV hook binding failed")?;
        self.into_normal_handler_with_download(download)
    }
    pub fn into_normal_handler_with_download(
        self,
        download: dispatcharr_store::xmltv_download::Downloader,
    ) -> Result<Arc<dyn crate::milestone_dispatch::Handler>, &'static str> {
        self.inner
            .runner
            .bind_remote(download)
            .map_err(|_| "normal remote XMLTV binding failed")?;
        self.into_normal_handler()
    }
    pub fn into_normal_handler(
        self,
    ) -> Result<Arc<dyn crate::milestone_dispatch::Handler>, &'static str> {
        {
            let driver = self
                .inner
                .driver
                .lock()
                .map_err(|_| "normal lifecycle poisoned")?;
            if driver.is_some()
                || self.inner.closed.load(Ordering::Acquire)
                || self.inner.normal_dispatch.swap(true, Ordering::AcqRel)
            {
                return Err("family already started/closed");
            }
        }
        let config = DriverConfig::default();
        self.inner
            .jobs
            .eta_pool(config.reservations)
            .map_err(|_| "normal reservation pool")?;
        let worker = crate::scheduled_refresh::ScheduledRefreshWorker::new(
            self.inner.jobs.clone(),
            self.inner.executor.clone(),
            config.scheduling,
        )
        .map_err(|_| "normal scheduled worker")?;
        Ok(Arc::new(NormalHandler {
            app: self,
            worker,
            limits: config.reservations,
        }))
    }
    pub fn start(&self) -> Result<(), crate::refresh_worker::Error> {
        let mut driver = self.inner.driver.lock().unwrap();
        if self.inner.closed.load(Ordering::Acquire)
            || self.inner.normal_dispatch.load(Ordering::Acquire)
        {
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
        if let Some(handoff) = &self.inner.series_handoff {
            handoff.close_admission();
        }
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
        if matches!(id, Value::Array(_) | Value::Object(_)) {
            return source_500();
        }
        let decimal = id
            .as_str()
            .and_then(canonical_decimal_source_id)
            .filter(|_| {
                object
                    .get("force")
                    .is_none_or(|value| value == &Value::Bool(false))
            });
        let Some(source_id) = id.as_i64().filter(|id| *id > 0).or(decimal) else {
            return unqualified_import_id();
        };
        if decimal.is_some() {
            // Only the existing active XMLTV case has signed, end-to-end Source evidence.
            let eligible = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM epg_epgsource WHERE id=$1 AND is_active AND source_type='xmltv')",
            )
            .bind(source_id)
            .fetch_one(self.inner.store.pool())
            .await;
            match eligible {
                Ok(true) => {}
                Ok(false) => return unqualified_import_id(),
                Err(_) => return source_500(),
            }
        } else {
            let dummy = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM epg_epgsource WHERE id=$1 AND source_type='dummy')",
            )
            .bind(source_id)
            .fetch_one(self.inner.store.pool())
            .await;
            match dummy {
                Ok(true) => return (StatusCode::BAD_REQUEST, Json(json!({"success":false,"message":"Dummy EPG sources do not require refreshing."}))).into_response(),
                Ok(false) => {}
                Err(_) => return source_500(),
            }
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
                args: vec![id.clone()],
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
impl dispatcharr_store::sd_source_http::InitialXmltvRefresh for RefreshApplication {
    fn publish_initial(
        &self,
        source: i64,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move {
            if !self.inner.normal_dispatch.load(Ordering::Acquire)
                || self.inner.closed.load(Ordering::Acquire)
            {
                return Err(StoreError::UnhandledRestFault);
            }
            let task_id = Uuid::new_v4();
            let message = RefreshMessage {
                task_id,
                source_id: source,
                force: false,
                retry: 0,
                root: task_id,
                parent: None,
                eta: None,
                call: RefreshCall::initial(source),
            };
            self.inner
                .jobs
                .publish_refresh(&message, owner)
                .await
                .map_err(|_| StoreError::UnhandledRestFault)?;
            Ok(())
        })
    }
}

fn unqualified_import_id() -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({"error":"This refresh ID shape has not been migrated"})),
    )
        .into_response()
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

struct NormalHandler {
    app: RefreshApplication,
    worker: crate::scheduled_refresh::ScheduledRefreshWorker,
    limits: crate::refresh_eta::Limits,
}
impl crate::milestone_dispatch::Lifecycle for NormalHandler {
    fn stop_admission(&self) {
        self.worker.stop_admission();
        self.app.inner.closed.store(true, Ordering::Release);
        if let Some(handoff) = &self.app.inner.series_handoff {
            handoff.close_admission();
        }
    }
    fn shutdown(
        &self,
        timeout: Duration,
    ) -> dispatcharr_contracts::BoxFuture<'_, Result<(), crate::milestone_dispatch::Disposition>>
    {
        Box::pin(async move {
            let deadline = tokio::time::Instant::now() + timeout;
            self.worker.shutdown(timeout).await.map_err(|error| {
                crate::milestone_dispatch::Disposition::Operational(format!(
                    "scheduled family drain {error:?}"
                ))
            })?;
            if self
                .app
                .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
                .await
            {
                Ok(())
            } else {
                Err(crate::milestone_dispatch::Disposition::Operational(
                    "normal app drain incomplete".into(),
                ))
            }
        })
    }
}
impl crate::milestone_dispatch::Handler for NormalHandler {
    fn supports_selected_head(&self) -> bool {
        true
    }
    fn run_once(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> dispatcharr_contracts::BoxFuture<
        '_,
        Result<crate::milestone_dispatch::Disposition, crate::milestone_dispatch::Disposition>,
    > {
        Box::pin(self.execute(None, owner))
    }
    fn run_selected(
        &self,
        expected: Arc<[u8]>,
        owner: Arc<dyn Send + Sync>,
    ) -> dispatcharr_contracts::BoxFuture<
        '_,
        Result<crate::milestone_dispatch::Disposition, crate::milestone_dispatch::Disposition>,
    > {
        Box::pin(self.execute(Some(expected), owner))
    }
}
impl NormalHandler {
    async fn execute(
        &self,
        expected: Option<Arc<[u8]>>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<crate::milestone_dispatch::Disposition, crate::milestone_dispatch::Disposition>
    {
        use crate::milestone_dispatch::Disposition as D;
        let candidate = match self
            .app
            .inner
            .jobs
            .peek_normal_refresh_candidate(owner.clone(), self.limits)
            .await
        {
            Ok(Some(value)) => value,
            Ok(None) => {
                return Ok(if expected.is_some() {
                    D::HeadChanged
                } else {
                    D::Empty
                })
            }
            Err(crate::transport::Error::UnsupportedTask) => return Ok(D::UnsupportedHead),
            Err(error) => return Err(D::Operational(format!("normal scheduled peek {error:?}"))),
        };
        if expected
            .as_ref()
            .is_some_and(|selected| selected.as_ref() != candidate.receipt.queued_bytes.as_slice())
        {
            return Ok(D::HeadChanged);
        }
        let sources = if candidate.scheduled_message().aggregate() {
            use crate::refresh_aggregate::{Admission, XmltvSources};
            match XmltvSources::read(&self.app.inner.store).await {
                Ok(Admission::Ordinary(sources)) => Some(sources),
                Ok(Admission::Unsupported) => return Ok(D::UnsupportedDomain),
                Err(_) => return Err(D::Operational("aggregate XMLTV selection failed".into())),
            }
        } else {
            None
        };
        let receipt = self
            .app
            .inner
            .jobs
            .reserve_refresh_candidate_settled(candidate, owner.clone(), self.limits)
            .await
            .map_err(|error| match error {
                crate::transport::Error::ClaimRace => D::HeadChanged,
                crate::transport::Error::Capacity => D::Capacity,
                other => D::Operational(format!("normal reservation {other:?}")),
            })?;
        let outcome = self
            .worker
            .start_normal(&receipt, owner, sources)
            .map_err(|error| D::Operational(format!("normal scheduled admission {error:?}")))?
            .wait()
            .await
            .map_err(|error| D::Operational(format!("normal scheduled execution {error:?}")))?;
        Ok(match outcome {
            crate::scheduled_refresh::Outcome::Completed(id)
            | crate::scheduled_refresh::Outcome::AlreadySucceeded(id) => D::Completed(id),
            crate::scheduled_refresh::Outcome::Restored(id) => D::Restored(id),
        })
    }
}
