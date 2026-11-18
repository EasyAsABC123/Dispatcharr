//! Real disabled Python-plugin filesystem/SQL workflow. Original authors retain credit.
//! Enabled executable Python is a required compatibility-host boundary, not metadata.
use crate::{AuthService, StoreError, TaskSqlSession};
use axum::{
    body::{to_bytes, Body, Bytes},
    extract::{
        ConnectInfo, DefaultBodyLimit, FromRequest, Multipart, Path as RoutePath, Request, State,
    },
    http::{Method, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Json, Router,
};
use serde_json::{json, Value};
use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime},
};
use tokio::sync::Notify;
#[path = "plugin_files.rs"]
mod files;
#[path = "plugin_hub.rs"]
mod hub;
#[path = "plugin_managed.rs"]
mod managed;
#[path = "plugin_manifest_fields.rs"]
mod manifest_fields;
#[path = "plugin_registry.rs"]
mod registry;
const UPLOAD: usize = 200 * 1024 * 1024;
fn guard() -> StoreError {
    StoreError::Unsupported(
        "Plugin workflow requires owned Python host or exceeds bounded Source domain",
    )
}
struct Cached {
    token: Option<SystemTime>,
    plugins: Vec<files::Discovered>,
}
/// A same-session callback to the existing configured Beat SQL algorithm.
/// Implementations must not reacquire a pool connection or start a timer.
pub trait PluginRepositorySchedule: Send + Sync {
    fn synchronize<'a>(
        &'a self,
        session: TaskSqlSession,
        owner: Arc<dyn Send + Sync>,
    ) -> dispatcharr_contracts::BoxFuture<'a, Result<(), StoreError>>;
}
struct Inner {
    auth: AuthService,
    root: PathBuf,
    closed: AtomicBool,
    active: AtomicUsize,
    done: Notify,
    cache: Mutex<Option<Cached>>,
    schedule: Option<Arc<dyn PluginRepositorySchedule>>,
    runtime: std::sync::OnceLock<crate::PluginRuntime>,
}
struct Live {
    inner: Arc<Inner>,
    permit: Mutex<Option<Arc<tokio::sync::OwnedSemaphorePermit>>>,
}
impl Drop for Live {
    fn drop(&mut self) {
        self.inner.active.fetch_sub(1, Ordering::AcqRel);
        self.inner.done.notify_waiters();
    }
}
struct Frame {
    bytes: Vec<u8>,
    _live: Arc<Live>,
    _permit: Arc<tokio::sync::OwnedSemaphorePermit>,
}
impl AsRef<[u8]> for Frame {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
#[derive(Clone)]
pub struct PluginActions {
    inner: Arc<Inner>,
}
#[derive(Clone, Copy)]
enum Action {
    List,
    ManagedInstall,
    Available,
    DetailManifest,
    RepoSettings,
    Reload,
    Import,
    Settings,
    Run,
    Enabled,
    Delete,
    Logo,
}
impl PluginActions {
    pub fn prepare(auth: AuthService, root: PathBuf) -> Self {
        Self {
            inner: Arc::new(Inner {
                auth,
                root,
                closed: AtomicBool::new(false),
                active: AtomicUsize::new(0),
                done: Notify::new(),
                cache: Mutex::new(None),
                schedule: None,
                runtime: std::sync::OnceLock::new(),
            }),
        }
    }
    /// Attach only normal Main's actual prepared shared-family runtime.
    pub fn with_runtime(self, runtime: crate::PluginRuntime) -> Self {
        // Startup's physical session can retain an Arc briefly after close;
        // attachment must not silently fail because Arc::get_mut returns None.
        let _ = self.inner.runtime.set(runtime);
        self
    }
    pub fn with_repository_schedule(mut self, schedule: Arc<dyn PluginRepositorySchedule>) -> Self {
        // Inert construction only; no service has been cloned or started yet.
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.schedule = Some(schedule);
        }
        self
    }
    pub fn router(self) -> Router {
        Router::new()
            .route("/api/plugins/plugins/", any(list))
            .route("/api/plugins/repos/install/", any(managed_install))
            .route("/api/plugins/repos/available/", any(available))
            .route("/api/plugins/repos/plugin-detail/", any(detail_manifest))
            .route("/api/plugins/repos/settings/", any(repo_settings))
            .route("/api/plugins/plugins/reload/", any(reload))
            .route("/api/plugins/plugins/import/", any(import))
            .route("/api/plugins/plugins/{key}/settings/", any(settings))
            .route("/api/plugins/plugins/{key}/run/", any(run))
            .route("/api/plugins/plugins/{key}/enabled/", any(enabled))
            .route("/api/plugins/plugins/{key}/delete/", any(delete))
            .route("/api/plugins/plugins/{key}/logo/", any(logo))
            .layer(DefaultBodyLimit::max(UPLOAD + 65536))
            .with_state(self)
    }
    fn enter(&self) -> Result<Arc<Live>, StoreError> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(guard());
        }
        self.inner.active.fetch_add(1, Ordering::AcqRel);
        let live = Arc::new(Live {
            inner: self.inner.clone(),
            permit: Mutex::new(None),
        });
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(guard());
        }
        Ok(live)
    }
    pub fn stop_admission(&self) {
        self.inner.closed.store(true, Ordering::Release);
    }
    pub async fn shutdown(&self, budget: Duration) -> Result<(), StoreError> {
        self.stop_admission();
        tokio::time::timeout(budget, async {
            loop {
                let done = self.inner.done.notified();
                if self.inner.active.load(Ordering::Acquire) == 0 {
                    return;
                }
                done.await;
            }
        })
        .await
        .map_err(|_| guard())
    }
    pub async fn startup_discover(&self, owner: Arc<dyn Send + Sync>) -> Result<(), StoreError> {
        let live = self.enter()?;
        let auth = self.inner.auth.clone();
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let session = auth
                .borrow_task_sql_session(Arc::new((owner, live)))
                .await?;
            let result = async {
                let snap = registry::snapshot(&session).await?;
                registry::mutable(&snap)?;
                let plugins = discovery(inner, false).await?;
                registry::sync_caught(&session, plugins).await
            }
            .await;
            let closed = session.close().await;
            closed?;
            result
        })
        .await
        .map_err(|_| guard())?
    }
    /// Guard import-side-effect workflows until their actual restore-task
    /// context/trail port is composed. The real caller owns the blocking read.
    pub async fn require_empty_restore_plugins(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<(), StoreError> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let _physical = owner;
            if !files::discover(&inner.root)?.is_empty() {
                return Err(StoreError::Unsupported(
                    "Restore plugin modules require an actual restore-task context",
                ));
            }
            if inner
                .cache
                .lock()
                .map_err(|_| guard())?
                .as_ref()
                .is_some_and(|c| !c.plugins.is_empty())
            {
                return Err(StoreError::Unsupported(
                    "Restore plugin modules require an actual restore-task context",
                ));
            }
            Ok(())
        })
        .await
        .map_err(|_| guard())?
    }
    /// Actual accepted restore task supplies its SAME fenced SQL session. This
    /// is not new HTTP admission and stays available through an owned warm drain.
    pub async fn reconcile_backup_restore(
        &self,
        admission: Arc<crate::BackupRestoreAdmission>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<(), StoreError> {
        if !admission.shares_auth_writer(&self.inner.auth) {
            return Err(guard());
        }
        self.require_empty_restore_plugins(owner.clone()).await?;
        let session = admission.session();
        let _physical = (owner, admission);
        let snapshot = registry::snapshot(&session).await?;
        registry::mutable(&snapshot)?;
        let plugins = discovery(self.inner.clone(), false).await?;
        registry::sync_caught(&session, plugins).await
    }
    async fn operation(
        self,
        peer: SocketAddr,
        request: Request,
        key: Option<String>,
        action: Action,
    ) -> Result<Response, StoreError> {
        let permit = Arc::new(
            self.inner
                .auth
                .user_agent_workers
                .clone()
                .try_acquire_owned()
                .map_err(|_| {
                    StoreError::StatusBody(
                        503,
                        json!({"detail":"Plugin request capacity unavailable."}),
                    )
                })?,
        );
        let live = self.enter()?;
        *live.permit.lock().map_err(|_| guard())? = Some(permit.clone());
        let auth = self
            .inner
            .auth
            .clone()
            .with_request_writer_permit(permit.clone())?;
        let observer = live.clone();
        let fallback = permit.clone();
        let work = tokio::spawn(async move {
            let result = self
                .owned(auth, peer, request, key, action, live.clone())
                .await;
            response(result, live, permit, action).await
        });
        match work.await {
            Ok(response) => Ok(response),
            Err(_) => Ok(response(Err(guard()), observer, fallback, action).await),
        }
    }
    async fn owned(
        &self,
        auth: AuthService,
        peer: SocketAddr,
        request: Request,
        key: Option<String>,
        action: Action,
        live: Arc<Live>,
    ) -> Result<Response, StoreError> {
        auth.request_started_display_timezone().await;
        let method = request.method().clone();
        let headers = request.headers().clone();
        let policy = crate::auth::request(peer, headers.clone());
        let reads = auth.retained_rest_reads(Default::default())?;
        let user = reads.authenticate(&policy).await?;
        let allowed = if matches!(action, Action::Logo) && method == Method::OPTIONS {
            true
        } else {
            reads
                .ui_network_allowed(auth.client_ip(&policy), user.as_ref())
                .await?
        };
        if matches!(action, Action::Logo) {
            if !allowed {
                return Ok(reply(
                    403,
                    json!({"success":false,"error":"Network access denied"}),
                ));
            }
        } else {
            let level = user
                .as_ref()
                .ok_or(StoreError::Unauthorized(
                    "Authentication credentials were not provided.",
                ))?
                .user()
                .user_level;
            let needed = if method == Method::GET || method == Method::HEAD {
                1
            } else {
                10
            };
            if !allowed || level < needed {
                return Err(StoreError::PermissionDenied);
            }
        }
        drop(user);
        drop(reads);
        if method == Method::OPTIONS {
            let name = match action {
                Action::List => "Plugins List API",
                Action::ManagedInstall => "Plugin Install From Repo API",
                Action::Available => "Available Plugins API",
                Action::DetailManifest => "Plugin Detail Manifest API",
                Action::RepoSettings => "Plugin Repo Settings API",
                Action::Reload => "Plugin Reload API",
                Action::Import => "Plugin Import API",
                Action::Settings => "Plugin Settings API",
                Action::Run => "Plugin Run API",
                Action::Enabled => "Plugin Enabled API",
                Action::Delete => "Plugin Delete API",
                Action::Logo => "Plugin Logo API",
            };
            let description = if matches!(action, Action::ManagedInstall) {
                "Install a plugin from a managed repo by downloading its release zip."
            } else {
                ""
            };
            return Ok(reply(
                200,
                json!({"name":name,"description":description,"renders":["application/json","text/html"],"parses":["application/json","application/x-www-form-urlencoded","multipart/form-data"]}),
            ));
        }
        let supported = match action {
            Action::List | Action::Logo | Action::Available => {
                method == Method::GET || method == Method::HEAD
            }
            Action::Delete => method == Method::DELETE,
            Action::RepoSettings => {
                method == Method::GET || method == Method::HEAD || method == Method::PUT
            }
            _ => method == Method::POST,
        };
        if !supported {
            return Ok(reply(
                405,
                json!({"detail":format!("Method \"{method}\" not allowed.")}),
            ));
        }
        let origin = crate::logos::origin(&auth, peer.ip(), &headers);
        let session = auth.borrow_task_sql_session(live.clone()).await?;
        let result = self
            .execute(&session, request, key, action, &origin, live)
            .await;
        let closed = session.close().await;
        closed?;
        result
    }
    async fn execute(
        &self,
        session: &TaskSqlSession,
        request: Request,
        key: Option<String>,
        action: Action,
        origin: &str,
        live: Arc<Live>,
    ) -> Result<Response, StoreError> {
        if matches!(action, Action::RepoSettings) {
            return hub::settings(self, session, request, live).await;
        }
        if matches!(action, Action::Available) {
            return Ok(reply(
                200,
                hub::available(&registry::snapshot(session).await?)?,
            ));
        }
        if matches!(action, Action::DetailManifest) {
            return hub::detail(self, session, request, live).await;
        }
        if matches!(action, Action::ManagedInstall) {
            return managed::install(self, session, request, origin, live).await;
        }
        if matches!(action, Action::Settings) {
            let value = json_body(request).await?;
            let settings = if truthy(&value) {
                value
                    .as_object()
                    .ok_or(StoreError::UnhandledRestFault)?
                    .get("settings")
                    .cloned()
                    .unwrap_or(json!({}))
            } else {
                json!({})
            };
            let settings = if truthy(&settings) {
                settings
            } else {
                json!({})
            };
            let key = key.ok_or_else(guard)?;
            let returned=session.run(move|c|Box::pin(async move{
    let raw=crate::json_compat::compact_json_bytes(&settings).map_err(|_|guard())?;if raw.len()>65536{return Err(guard());}
    let result=sqlx::query("UPDATE plugins_pluginconfig SET settings=$2::jsonb,updated_at=$3 WHERE key=$1 RETURNING key").bind(key).bind(String::from_utf8(raw).map_err(|_|guard())?).bind(chrono::Utc::now()).fetch_optional(c).await?;
    Ok::<_,StoreError>(result.map(|_|settings))
   })).await?;
            let returned = match returned {
                Ok(value) => value,
                Err(error) => {
                    if let Some(message) = setting_error(&error) {
                        return Ok(reply(400, json!({"success":false,"error":message})));
                    }
                    return Err(error);
                }
            };
            return Ok(match returned {
                Some(value) => reply(200, json!({"success":true,"settings":value})),
                None => reply(
                    400,
                    json!({"success":false,"error":"PluginConfig matching query does not exist."}),
                ),
            });
        }
        // Logo discovery imports enabled modules through the same owned host as
        // Source PluginLogoAPIView. Static disabled/history guards cannot serve
        // this operation when normal configured plugins are enabled.
        if matches!(action, Action::Logo)
            && key
                .as_deref()
                .is_some_and(|key| key != "." && key != ".." && !key.contains(['/', '\\']))
        {
            if let Some(runtime) = self.inner.runtime.get() {
                let descriptor = runtime
                    .logo_descriptor(
                        key.clone().ok_or_else(guard)?,
                        session.clone(),
                        live.clone(),
                    )
                    .await?;
                match descriptor["status"].as_u64() {
                    Some(400) => {
                        return Ok(reply(
                            400,
                            json!({"success":false,"error":"Invalid plugin path"}),
                        ))
                    }
                    Some(404) => {
                        return Ok(reply(
                            404,
                            json!({"success":false,"error":"Logo not found"}),
                        ))
                    }
                    Some(200) => {
                        let raw_path = descriptor["path"].as_str().ok_or_else(guard)?;
                        let raw_root = descriptor["plugins_root"].as_str().ok_or_else(guard)?;
                        if raw_path.len() > 16384 || raw_root.len() > 16384 {
                            return Err(guard());
                        }
                        let path = PathBuf::from(raw_path);
                        let root = PathBuf::from(raw_root);
                        if !path.is_absolute()
                            || !root.is_absolute()
                            || !path.starts_with(&root)
                            || path.file_name() != Some(std::ffi::OsStr::new("logo.png"))
                            || path
                                .components()
                                .any(|c| matches!(c, std::path::Component::ParentDir))
                        {
                            return Err(guard());
                        }
                        return self.logo_file(request, path, live).await;
                    }
                    _ => return Err(guard()),
                }
            }
        }
        let mut snapshot = registry::snapshot(session).await?;
        if matches!(action, Action::Run) {
            let value = json_body(request).await?;
            let value = value.as_object().ok_or(StoreError::UnhandledRestFault)?;
            if !value.get("action").is_some_and(truthy) {
                return Ok(reply(
                    400,
                    json!({"success":false,"error":"Missing 'action'"}),
                ));
            }
            let config = snapshot
                .configs
                .iter()
                .find(|c| c["key"] == key.as_deref().unwrap_or(""));
            return match config {
                None => Ok(reply(
                    404,
                    json!({"success":false,"error":"Plugin not found"}),
                )),
                Some(c) if c["enabled"] != true => Ok(reply(
                    403,
                    json!({"success":false,"error":"Plugin is disabled"}),
                )),
                Some(_) => {
                    let runtime = self.inner.runtime.get().ok_or_else(guard)?;
                    let result = runtime
                        .run_action(
                            key.clone().ok_or_else(guard)?,
                            value["action"].clone(),
                            value.get("params").cloned().unwrap_or(json!({})),
                            session.clone(),
                            live,
                        )
                        .await?;
                    if result["source_rendering_failed"] == true {
                        return Err(StoreError::UnhandledRestFault);
                    }
                    let status = result["status"]
                        .as_u64()
                        .and_then(|v| u16::try_from(v).ok())
                        .filter(|v| matches!(v, 200 | 403 | 500))
                        .ok_or_else(guard)?;
                    use base64::Engine;
                    let raw = base64::engine::general_purpose::STANDARD
                        .decode(result["body_base64"].as_str().ok_or_else(guard)?)
                        .map_err(|_| guard())?;
                    if raw.len() > 8 * 1024 * 1024 {
                        return Err(guard());
                    }
                    Ok(Response::builder()
                        .status(status)
                        .header("content-type", "application/json")
                        .body(Body::from(raw))
                        .map_err(|_| guard())?)
                }
            };
        }
        if matches!(action, Action::Enabled) {
            let data = json_body(request).await?;
            let data = data.as_object().ok_or(StoreError::UnhandledRestFault)?;
            let Some(raw) = data.get("enabled").filter(|v| !v.is_null()) else {
                return Ok(reply(
                    400,
                    json!({"success":false,"error":"Missing 'enabled' boolean"}),
                ));
            };
            let Some(enable) = parse_enabled(raw) else {
                return Ok(reply(
                    400,
                    json!({"success":false,"error":"Invalid 'enabled' boolean"}),
                ));
            };
            let key = key.ok_or_else(guard)?;
            if !snapshot.configs.iter().any(|c| c["key"] == key) {
                return Ok(reply(
                    404,
                    json!({"success":false,"error":"Plugin not found"}),
                ));
            }
            if let Some(runtime) = self.inner.runtime.get() {
                let result = runtime
                    .change_enabled(key, enable, origin.to_owned(), session.clone(), live)
                    .await?;
                return rendered_response(result);
            }
            // Enabling imports real Python and may resume dynamic tasks. The
            // absent compatibility host is a pre-effect guard, never fake success.
            if enable {
                return Err(guard());
            }
            registry::mutable(&snapshot)?;
            let selected = key.clone();
            session.run(move|c|Box::pin(async move{sqlx::query("UPDATE plugins_pluginconfig SET enabled=false,updated_at=$2 WHERE key=$1").bind(selected).bind(chrono::Utc::now()).execute(c).await.map_err(StoreError::from)})).await??;
            let plugins = discovery(self.inner.clone(), true).await?;
            registry::sync_caught(session, plugins.clone()).await?;
            snapshot = registry::snapshot(session).await?;
            let plugin = registry::list(&snapshot, &plugins, origin)?
                .into_iter()
                .find(|p| p["key"] == key);
            let mut body = json!({"success":true,"enabled":false,"ever_enabled":false});
            if let Some(plugin) = plugin {
                body["plugin"] = plugin;
            }
            return Ok(reply(200, body));
        }
        if matches!(action, Action::Delete) {
            registry::mutable(&snapshot)?;
            let key = key.ok_or_else(guard)?;
            let plugins = discovery(self.inner.clone(), false).await?;
            let folder = plugins
                .iter()
                .find(|p| p.key == key)
                .map(|p| p.folder.clone());
            let root = self.inner.root.clone();
            let target = key.clone();
            // SQL row lock, filesystem removal and DB deletion retain THIS one
            // actual session. Source deliberately does not restore removed files
            // when a later database statement rolls back.
            session
                .run(move |c| {
                    Box::pin(async move {
                        use sqlx::Connection;
                        let mut tx = c.begin().await?;
                        sqlx::query("SELECT id FROM plugins_pluginconfig WHERE key=$1 FOR UPDATE")
                            .bind(&target)
                            .fetch_optional(&mut *tx)
                            .await?;
                        tokio::task::spawn_blocking(move || {
                            let _live = live;
                            files::delete(&root, &key, folder.as_deref())
                        })
                        .await
                        .map_err(|_| guard())??;
                        sqlx::query("DELETE FROM plugins_pluginconfig WHERE key=$1")
                            .bind(target)
                            .execute(&mut *tx)
                            .await?;
                        tx.commit().await?;
                        Ok::<_, StoreError>(())
                    })
                })
                .await??;
            let plugins = discovery(self.inner.clone(), true).await?;
            registry::sync_caught(session, plugins).await?;
            return Ok(reply(200, json!({"success":true})));
        }
        let executable_history = snapshot.configs.iter().any(|c| {
            c["enabled"] == true
                || c["ever_enabled"] == true
                || c["owned_tasks"].as_array().is_some_and(|a| !a.is_empty())
                || c["suspended_schedules"]
                    .as_array()
                    .is_some_and(|a| !a.is_empty())
        });
        if executable_history {
            let runtime = self.inner.runtime.get().ok_or_else(guard)?;
            if matches!(action, Action::List) {
                return rendered_response(
                    runtime
                        .list_plugins(origin.to_owned(), session.clone(), live)
                        .await?,
                );
            }
            if matches!(action, Action::Reload) {
                return Ok(reply(
                    200,
                    runtime.reload_plugins(session.clone(), live).await?,
                ));
            }
        }
        registry::no_history(&snapshot)?;
        if matches!(action, Action::Import | Action::Reload) {
            registry::mutable(&snapshot)?;
        }
        if matches!(action, Action::Import) {
            return self.import(session, request, snapshot, origin, live).await;
        }
        let reload = matches!(action, Action::Reload);
        let plugins = discovery(self.inner.clone(), reload).await?;
        if reload || matches!(action, Action::Logo) {
            registry::sync_caught(session, plugins.clone()).await?;
            snapshot = registry::snapshot(session).await?;
        }
        if reload {
            return Ok(reply(200, json!({"success":true,"count":plugins.len()})));
        }
        if matches!(action, Action::Logo) {
            return self
                .logo(request, key.ok_or_else(guard)?, plugins, live)
                .await;
        }
        Ok(reply(
            200,
            json!({"plugins":registry::list(&snapshot,&plugins,origin)?}),
        ))
    }
    async fn import(
        &self,
        session: &TaskSqlSession,
        request: Request,
        _snapshot: registry::Snapshot,
        origin: &str,
        live: Arc<Live>,
    ) -> Result<Response, StoreError> {
        use tokio::io::AsyncWriteExt;
        if request
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_none_or(|v| v.split(';').next() != Some("multipart/form-data"))
        {
            json_body(request).await?;
            return Ok(reply(
                400,
                json!({"success":false,"error":"Missing 'file' upload"}),
            ));
        }
        tokio::fs::create_dir_all(&self.inner.root)
            .await
            .map_err(|_| guard())?;
        let path = self.inner.root.join(format!(
            ".plugin-upload-{}.zip",
            uuid::Uuid::new_v4().simple()
        ));
        let mut upload = Upload(path.clone());
        let mut form = Multipart::from_request(request, &())
            .await
            .map_err(|_| guard())?;
        let mut name = None;
        let mut overwrite = false;
        let mut fields = 0;
        while let Some(mut field) = form.next_field().await.map_err(|_| guard())? {
            fields += 1;
            if fields > 16 {
                return Err(guard());
            }
            if field.name() == Some("overwrite") {
                let value = field.text().await.map_err(|_| guard())?;
                if value.len() > 65536 {
                    return Err(guard());
                }
                overwrite = !value.is_empty();
            } else if field.name() == Some("file") && field.file_name().is_some() {
                name = Some(field.file_name().unwrap_or("plugin.zip").to_owned());
                let mut file = tokio::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&path)
                    .await
                    .map_err(|_| guard())?;
                let written = async {
                    let mut used = 0usize;
                    while let Some(chunk) = field.chunk().await.map_err(|_| guard())? {
                        used = used.checked_add(chunk.len()).ok_or_else(guard)?;
                        if used > UPLOAD {
                            return Err(guard());
                        }
                        file.write_all(&chunk).await.map_err(|_| guard())?;
                    }
                    Ok::<_, StoreError>(())
                }
                .await;
                let flushed = file.flush().await;
                drop(file.into_std().await);
                flushed.map_err(|_| guard())?;
                written?;
            }
        }
        let Some(name) = name else {
            return Ok(reply(
                400,
                json!({"success":false,"error":"Missing 'file' upload"}),
            ));
        };
        let root = self.inner.root.clone();
        let archive = path.clone();
        let result = tokio::task::spawn_blocking(move || {
            let _live = live;
            files::install(&archive, &root, &name, overwrite)
        })
        .await
        .map_err(|_| guard())??;
        if result["success"] != true {
            return Ok(reply(400, result));
        }
        let key = result["plugin_key"].as_str().ok_or_else(guard)?.to_owned();
        let target = key.clone();
        let was_managed=session.run(move|c|Box::pin(async move{
   let existing:Option<Option<i64>>=sqlx::query_scalar("SELECT source_repo_id::bigint FROM plugins_pluginconfig WHERE key=$1").bind(&target).fetch_optional(&mut *c).await?;
   if existing.is_none(){sqlx::query("INSERT INTO plugins_pluginconfig(key,name,version,description,enabled,ever_enabled,settings,owned_tasks,suspended_schedules,slug,installed_version_is_prerelease,deprecated,created_at,updated_at) VALUES($1,$1,'','',false,false,'{}','[]','[]','',false,false,$2,$2)").bind(&target).bind(chrono::Utc::now()).execute(&mut *c).await?;}
   let managed=existing.flatten().is_some();if managed{sqlx::query("UPDATE plugins_pluginconfig SET source_repo_id=NULL,slug='',updated_at=$2 WHERE key=$1").bind(&target).bind(chrono::Utc::now()).execute(&mut *c).await?;}
   Ok::<_,StoreError>(managed)
  })).await?;
        let was_managed = match was_managed {
            Ok(value) => value,
            Err(StoreError::Database(error)) if error.as_database_error().is_some() => {
                tracing::debug!(target:"dispatcharr::plugin_files","Manual plugin registration deferred after settled server error");
                false
            }
            Err(error) => return Err(error),
        };
        let plugins = discovery(self.inner.clone(), true).await?;
        registry::sync_caught(session, plugins.clone()).await?;
        let snapshot = registry::snapshot(session).await?;
        let plugin = registry::list(&snapshot, &plugins, origin)?
            .into_iter()
            .find(|p| p["key"] == key)
            .ok_or_else(guard)?;
        upload.remove();
        Ok(reply(
            200,
            json!({"success":true,"plugin":plugin,"was_managed":was_managed}),
        ))
    }
    async fn logo(
        &self,
        request: Request,
        key: String,
        plugins: Vec<files::Discovered>,
        live: Arc<Live>,
    ) -> Result<Response, StoreError> {
        if key == "." || key == ".." || key.contains(['/', '\\']) {
            return Ok(reply(
                400,
                json!({"success":false,"error":"Invalid plugin path"}),
            ));
        }
        let folder = plugins
            .iter()
            .find(|p| p.key == key)
            .map(|p| p.folder.as_str())
            .unwrap_or(&key);
        let path = self.inner.root.join(folder).join("logo.png");
        self.logo_file(request, path, live).await
    }
    async fn logo_file(
        &self,
        request: Request,
        path: PathBuf,
        live: Arc<Live>,
    ) -> Result<Response, StoreError> {
        let held = live.clone();
        let opened = tokio::task::spawn_blocking(move || {
            let _held = held;
            let file = std::fs::File::open(path)?;
            let size = file.metadata()?.len();
            Ok::<_, std::io::Error>((Arc::new(Mutex::new(file)), size))
        })
        .await
        .map_err(|_| guard())?;
        let (file, size) = match opened {
            Ok(value) => value,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(reply(
                    404,
                    json!({"success":false,"error":"Logo not found"}),
                ))
            }
            Err(_) => return Err(guard()),
        };
        if size > UPLOAD as u64 {
            return Err(guard());
        }
        let body = if request.method() == Method::HEAD {
            Body::empty()
        } else {
            Body::from_stream(async_stream::stream! {
                let held=live;
                loop{
                    let reader=file.clone();let owner=held.clone();
                    let read=tokio::task::spawn_blocking(move||{
                        use std::io::Read;
                        let _owner=owner;let mut bytes=vec![0u8;65536];
                        let count=reader.lock().map_err(|_|std::io::Error::other("Owned plugin reader poisoned"))?.read(&mut bytes)?;
                        bytes.truncate(count);Ok::<_,std::io::Error>(bytes)
                    }).await;
                    match read{Ok(Ok(bytes))if bytes.is_empty()=>break,Ok(Ok(bytes))=>yield Ok::<_,std::io::Error>(Bytes::from_owner(LogoFrame{bytes,_live:held.clone(),_file:file.clone()})),Ok(Err(error))=>{yield Err(error);break;},Err(_)=>{yield Err(std::io::Error::other("Owned plugin reader join failed"));break;}}
                }
            })
        };
        Response::builder()
            .status(200)
            .header("Content-Type", "image/png")
            .header("Vary", "Accept, origin")
            .header("Content-Length", size.to_string())
            .header("Content-Disposition", "inline; filename=\"logo.png\"")
            .body(body)
            .map_err(|_| guard())
    }
}
struct LogoFrame {
    bytes: Vec<u8>,
    _live: Arc<Live>,
    _file: Arc<Mutex<std::fs::File>>,
}
impl AsRef<[u8]> for LogoFrame {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
struct Upload(PathBuf);
impl Upload {
    fn remove(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
impl Drop for Upload {
    fn drop(&mut self) {
        self.remove();
    }
}
async fn discovery(inner: Arc<Inner>, force: bool) -> Result<Vec<files::Discovered>, StoreError> {
    tokio::task::spawn_blocking(move || {
        let mut cache = inner.cache.lock().map_err(|_| guard())?;
        let token = inner.root.join(".reload_token");
        let current = std::fs::metadata(&token)
            .ok()
            .and_then(|m| m.modified().ok());
        if !force {
            if let Some(cached) = cache.as_ref() {
                if current <= cached.token {
                    return Ok(cached.plugins.clone());
                }
            }
        }
        if force {
            let _ = std::fs::create_dir_all(&inner.root);
            if let Ok(file) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&token)
            {
                let _ = file.set_times(std::fs::FileTimes::new().set_modified(SystemTime::now()));
            }
        }
        let plugins = files::discover(&inner.root)?;
        let token = std::fs::metadata(token)
            .ok()
            .and_then(|m| m.modified().ok());
        *cache = Some(Cached {
            token,
            plugins: plugins.clone(),
        });
        Ok(plugins)
    })
    .await
    .map_err(|_| guard())?
}
fn truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(v) => *v,
        Value::Number(v) => v.as_f64() != Some(0.0),
        Value::String(v) => !v.is_empty(),
        Value::Array(v) => !v.is_empty(),
        Value::Object(v) => !v.is_empty(),
    }
}
async fn json_body(request: Request) -> Result<Value, StoreError> {
    let content_type = request
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let bytes = to_bytes(request.into_body(), 65536)
        .await
        .map_err(|_| guard())?;
    if bytes.is_empty() || content_type.is_none() {
        return Ok(json!({}));
    }
    let content_type = content_type.unwrap_or_default();
    let media = content_type.split(';').next().unwrap_or("").trim();
    if media == "application/json" {
        return serde_json::from_slice(&bytes).map_err(|e|StoreError::StatusBody(400,json!({"detail":format!("JSON parse error - {}",crate::source_json_error(&bytes,e))})));
    }
    if media == "application/x-www-form-urlencoded" {
        let mut values = serde_json::Map::new();
        for (key, value) in url::form_urlencoded::parse(&bytes) {
            values.insert(key.into_owned(), Value::String(value.into_owned()));
        }
        return Ok(Value::Object(values));
    }
    if media == "multipart/form-data" {
        let request = Request::builder()
            .header(axum::http::header::CONTENT_TYPE, &content_type)
            .body(Body::from(bytes))
            .map_err(|_| guard())?;
        let mut data = Multipart::from_request(request, &())
            .await
            .map_err(|_| guard())?;
        let mut values = serde_json::Map::new();
        let mut fields = 0;
        while let Some(field) = data.next_field().await.map_err(|_| guard())? {
            fields += 1;
            if fields > 256 || field.file_name().is_some() {
                return Err(guard());
            }
            let key = field.name().unwrap_or("").to_owned();
            let text = field.text().await.map_err(|_| guard())?;
            values.insert(key, Value::String(text));
        }
        return Ok(Value::Object(values));
    }
    Err(StoreError::StatusBody(
        415,
        json!({"detail":format!("Unsupported media type \"{content_type}\" in request.")}),
    ))
}
/// Same qualified Source PG primary/detail/hint/context spelling as normal
/// channel errors; no invented Python stack or diagnostic fields.
pub(crate) fn setting_error(error: &StoreError) -> Option<String> {
    let StoreError::Database(error) = error else {
        return None;
    };
    let pg = error
        .as_database_error()?
        .try_downcast_ref::<sqlx::postgres::PgDatabaseError>()?;
    if !["P0001", "22001", "23505", "23514"].contains(&pg.code()) || pg.position().is_some() {
        return None;
    }
    let mut message = pg.message().to_owned();
    for (label, value) in [
        ("DETAIL", pg.detail()),
        ("HINT", pg.hint()),
        ("CONTEXT", pg.r#where()),
    ] {
        if let Some(value) = value {
            message.push_str(&format!("\n{label}:  {value}"));
        }
    }
    if message.len() > 8192 {
        return None;
    }
    Some(message.trim().to_owned())
}

fn reply(status: u16, value: Value) -> Response {
    (
        StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        Json(value),
    )
        .into_response()
}
async fn response(
    result: Result<Response, StoreError>,
    live: Arc<Live>,
    permit: Arc<tokio::sync::OwnedSemaphorePermit>,
    action: Action,
) -> Response {
    let mut response = match result {
        Ok(response) => response,
        Err(e) => e.into_response(),
    };
    let allow = match action {
        Action::List | Action::Logo | Action::Available => "GET, HEAD, OPTIONS",
        Action::Delete => "DELETE, OPTIONS",
        Action::RepoSettings => "GET, HEAD, PUT, OPTIONS",
        _ => "POST, OPTIONS",
    };
    if response.status() != StatusCode::INTERNAL_SERVER_ERROR {
        response.headers_mut().insert(
            axum::http::header::ALLOW,
            axum::http::HeaderValue::from_static(allow),
        );
    }
    if response
        .headers()
        .get("content-type")
        .is_some_and(|v| v == "image/png")
    {
        return response;
    }
    let (mut parts, body) = response.into_parts();
    let bytes = match to_bytes(body, 1048576).await {
        Ok(bytes) => bytes.to_vec(),
        Err(_) => {
            parts.status = StatusCode::SERVICE_UNAVAILABLE;
            b"{\"detail\":\"Plugin response capacity unavailable.\"}".to_vec()
        }
    };
    parts.headers.insert(
        axum::http::header::CONTENT_LENGTH,
        axum::http::HeaderValue::from_str(&bytes.len().to_string()).expect("bounded length"),
    );
    Response::from_parts(
        parts,
        Body::from(Bytes::from_owner(Frame {
            bytes,
            _live: live,
            _permit: permit,
        })),
    )
}
macro_rules! endpoint {
    ($name:ident,$action:ident) => {
        async fn $name(
            State(service): State<PluginActions>,
            ConnectInfo(peer): ConnectInfo<SocketAddr>,
            request: Request,
        ) -> Result<Response, StoreError> {
            service
                .operation(peer, request, None, Action::$action)
                .await
        }
    };
}
macro_rules! keyed {
    ($name:ident,$action:ident) => {
        async fn $name(
            State(service): State<PluginActions>,
            ConnectInfo(peer): ConnectInfo<SocketAddr>,
            RoutePath(key): RoutePath<String>,
            request: Request,
        ) -> Result<Response, StoreError> {
            service
                .operation(peer, request, Some(key), Action::$action)
                .await
        }
    };
}
endpoint!(list, List);
endpoint!(managed_install, ManagedInstall);
endpoint!(available, Available);
endpoint!(detail_manifest, DetailManifest);
endpoint!(repo_settings, RepoSettings);
endpoint!(reload, Reload);
endpoint!(import, Import);
keyed!(settings, Settings);
keyed!(run, Run);
keyed!(enabled, Enabled);
keyed!(delete, Delete);
keyed!(logo, Logo);

fn parse_enabled(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(v) => Some(*v),
        Value::Number(v) => match v.as_i64() {
            Some(0) => Some(false),
            Some(1) => Some(true),
            _ => None,
        },
        Value::String(v) => match v.trim().to_lowercase().as_str() {
            "true" | "1" | "yes" | "y" | "on" => Some(true),
            "false" | "0" | "no" | "n" | "off" => Some(false),
            _ => None,
        },
        _ => None,
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_enable_parser_does_not_treat_floats_or_arbitrary_truthy_values_as_boolean() {
        for value in [
            json!(true),
            json!(1),
            json!(" YES "),
            json!("y"),
            json!("on"),
        ] {
            assert_eq!(parse_enabled(&value), Some(true));
        }
        for value in [
            json!(false),
            json!(0),
            json!(" NO "),
            json!("n"),
            json!("off"),
        ] {
            assert_eq!(parse_enabled(&value), Some(false));
        }
        for value in [json!(1.0), json!(2), Value::Null, json!([]), json!({})] {
            assert_eq!(parse_enabled(&value), None);
        }
    }
}

#[cfg(test)]
mod request_compat_tests {
    use super::*;
    fn request(mime: Option<&str>, raw: &'static str) -> Request {
        let mut builder = Request::builder();
        if let Some(mime) = mime {
            builder = builder.header(axum::http::header::CONTENT_TYPE, mime);
        }
        builder.body(Body::from(raw)).unwrap()
    }
    #[tokio::test]
    async fn actual_request_parser_preserves_json_forms_and_drf_empty_content_type() {
        assert_eq!(
            json_body(request(
                Some("application/json"),
                r#"{"settings":[false,1,"text"]}"#
            ))
            .await
            .unwrap(),
            json!({"settings":[false,1,"text"]})
        );
        assert_eq!(
            json_body(request(
                Some("application/x-www-form-urlencoded"),
                "settings=first&settings=last%20value"
            ))
            .await
            .unwrap(),
            json!({"settings":"last value"})
        );
        assert_eq!(
            json_body(request(None, "not JSON")).await.unwrap(),
            json!({})
        );
        assert_eq!(
            json_body(request(Some("application/json"), ""))
                .await
                .unwrap(),
            json!({})
        );
    }
    #[tokio::test]
    async fn actual_request_parser_returns_source_error_details() {
        match json_body(request(Some("application/json"), "{\"settings\":"))
            .await
            .unwrap_err()
        {
            StoreError::StatusBody(400, body) => assert!(body["detail"]
                .as_str()
                .unwrap()
                .starts_with("JSON parse error - Expecting value:")),
            _ => panic!("actual malformed JSON must return DRF detail"),
        }
        match json_body(request(Some("text/plain"), "text"))
            .await
            .unwrap_err()
        {
            StoreError::StatusBody(415, body) => assert_eq!(
                body,
                json!({"detail":"Unsupported media type \"text/plain\" in request."})
            ),
            _ => panic!("actual unsupported MIME must return DRF detail"),
        }
    }
}

fn rendered_response(result: Value) -> Result<Response, StoreError> {
    if result["source_rendering_failed"] == true {
        return Err(StoreError::UnhandledRestFault);
    }
    let status = result["status"]
        .as_u64()
        .and_then(|v| u16::try_from(v).ok())
        .filter(|v| matches!(v, 200 | 403 | 404 | 500))
        .ok_or_else(guard)?;
    use base64::Engine;
    let raw = base64::engine::general_purpose::STANDARD
        .decode(result["body_base64"].as_str().ok_or_else(guard)?)
        .map_err(|_| guard())?;
    if raw.len() > 8 * 1024 * 1024 {
        return Err(guard());
    }
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Body::from(raw))
        .map_err(|_| guard())
}
