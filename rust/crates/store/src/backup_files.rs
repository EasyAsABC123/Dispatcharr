//! Job-free original backup HTTP/filesystem contracts. Original authors retain AGPL credit.
//! Optional explicit shared create/status/current-schema restore adapter. No worker starts here.
use crate::{channels::channel_options_headers, AuthService, StoreError};
use axum::{
    body::{Body, Bytes},
    extract::{ConnectInfo, FromRequest, Multipart, Request, State},
    http::{HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Json, Router,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::StreamExt;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    io::{Read, Seek, Write},
    net::SocketAddr,
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use subtle::ConstantTimeEq;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
#[derive(Clone)]
pub struct BackupFiles {
    auth: AuthService,
    root: Arc<PathBuf>,
    secret: Arc<Vec<u8>>,
    workers: Arc<Semaphore>,
    jobs: Option<Arc<dyn BackupJobApi>>,
    job_access: JobAccess,
    schedule_disable: Option<Arc<dyn BackupScheduleDisableApi>>,
    nginx_accel: bool,
}
#[derive(Clone, Copy)]
enum JobAccess {
    None,
    Create,
    Restore,
    Complete,
}

impl JobAccess {
    fn permits(self, kind: &str) -> bool {
        matches!(
            (self, kind),
            (Self::Create | Self::Complete, "create")
                | (Self::Create | Self::Restore | Self::Complete, "status")
                | (Self::Restore | Self::Complete, "restore")
                | (Self::Complete, "schedule-update")
        )
    }
}

#[cfg(test)]
#[test]
fn durable_job_routes_preserve_their_owner_capabilities() {
    let routes = ["create", "status", "restore", "schedule-update"];
    for (scope, expected) in [
        (JobAccess::None, [false, false, false, false]),
        (JobAccess::Create, [true, true, false, false]),
        (JobAccess::Restore, [false, true, true, false]),
        (JobAccess::Complete, [true, true, true, true]),
    ] {
        assert_eq!(routes.map(|route| scope.permits(route)), expected);
    }
}

/// Explicit adapter avoids a Store -> job-transport dependency cycle. No default
/// constructor, background worker or unrestricted restore authority is implied.
pub trait BackupJobApi: Send + Sync {
    fn create(
        &self,
        caller: Arc<dyn Send + Sync>,
    ) -> dispatcharr_contracts::BoxFuture<'_, Result<String, StoreError>>;
    fn restore(
        &self,
        _filename: String,
        _caller: Arc<dyn Send + Sync>,
    ) -> dispatcharr_contracts::BoxFuture<'_, Result<String, StoreError>> {
        Box::pin(async { Err(StoreError::Unsupported("Backup restore worker unavailable")) })
    }
    fn status<'a>(
        &'a self,
        task_id: &'a str,
        caller: Arc<dyn Send + Sync>,
    ) -> dispatcharr_contracts::BoxFuture<'a, Result<Value, StoreError>>;
    fn update_schedule(
        &self,
        _data: Value,
        _caller: Arc<dyn Send + Sync>,
    ) -> dispatcharr_contracts::BoxFuture<'_, Result<Value, StoreError>> {
        Box::pin(async {
            Err(StoreError::Unsupported(
                "Backup schedule update is not migrated",
            ))
        })
    }
    fn shutdown(&self, grace: Duration) -> dispatcharr_contracts::BoxFuture<'_, bool>;
}
/// A stored schedule disable requires SQL and cache ownership only.
pub trait BackupScheduleDisableApi: Send + Sync {
    fn disable<'a>(
        &'a self,
        data: Value,
        auth: &'a AuthService,
        caller: Arc<dyn Send + Sync>,
    ) -> dispatcharr_contracts::BoxFuture<'a, Result<Value, StoreError>>;
}
type Lease = Arc<OwnedSemaphorePermit>;
fn reply(status: u16, detail: impl Into<String>) -> Response {
    let value = json!({"detail":detail.into()});
    let len = serde_json::to_vec(&value).unwrap().len();
    let mut response = (StatusCode::from_u16(status).unwrap(), Json(value)).into_response();
    response.headers_mut().insert(
        "content-length",
        HeaderValue::from_str(&len.to_string()).unwrap(),
    );
    response
}
fn schedule_payload(raw: Option<&Value>) -> Result<Value, String> {
    let settings = raw.and_then(Value::as_object);
    let get = |name: &str, default: Value| {
        settings
            .and_then(|obj| obj.get(name))
            .cloned()
            .unwrap_or(default)
    };
    let text = |value: Value| {
        value
            .as_str()
            .map(str::to_owned)
            .or_else(|| crate::python_action::collection_text(&value, 1 << 20))
            .unwrap_or_default()
    };
    let enabled = crate::channels::truthy(Some(&get("schedule_enabled", json!(true))));
    let frequency = text(get("schedule_frequency", json!("daily")));
    let time = text(get("schedule_time", json!("03:00")));
    let day = crate::json_compat::python_json_int(&get("schedule_day_of_week", json!(0)))?;
    let retention = crate::json_compat::python_json_int(&get("retention_count", json!(3)))?;
    let cron = text(get("schedule_cron_expression", json!("")));
    Ok(
        json!({"enabled":enabled,"frequency":frequency,"time":time,"day_of_week":day,
              "retention_count":retention,"cron_expression":cron}),
    )
}

#[cfg(test)]
#[test]
fn source_copied_schedule_projection_and_integer_fault_order() {
    assert_eq!(
        schedule_payload(Some(
            &json!({"schedule_enabled":"false", "schedule_day_of_week":" +０_４ ",
             "retention_count":true, "schedule_time":null})
        )),
        Ok(
            json!({"enabled":true,"frequency":"daily","time":"None","day_of_week":4,
              "retention_count":1,"cron_expression":""})
        )
    );
    assert_eq!(
        schedule_payload(Some(
            &json!({"schedule_day_of_week":"x'quote", "retention_count":null})
        )),
        Err("invalid literal for int() with base 10: \"x'quote\"".into())
    );
    assert_eq!(
        schedule_payload(Some(
            &json!({"schedule_day_of_week":4.25, "retention_count":[]})
        )),
        Err(
            "int() argument must be a string, a bytes-like object or a real number, not 'list'"
                .into()
        )
    );
}

fn invalid(name: &str) -> bool {
    name.contains("..") || name.contains('/') || name.contains('\\')
}
fn download_disposition(filename: &str) -> Option<HeaderValue> {
    django_download_header(&format!("attachment; filename=\"{filename}\""))
}

fn django_download_header(value: &str) -> Option<HeaderValue> {
    if let Some(latin) = value
        .chars()
        .map(|c| u8::try_from(c as u32).ok())
        .collect::<Option<Vec<_>>>()
    {
        return HeaderValue::from_bytes(&latin).ok();
    }
    // Django ResponseHeaders falls back to one unfolded UTF-8 encoded word.
    // Keep line-breaking characters outside this filename encoding path.
    if value.chars().any(|c| {
        matches!(
            c,
            '\n' | '\r' | '\u{000b}' | '\u{000c}' | '\u{001c}'
                ..='\u{001e}' | '\u{0085}' | '\u{2028}' | '\u{2029}'
        )
    }) {
        return None;
    }
    let is_q_literal = |byte: u8| byte.is_ascii_alphanumeric() || b"!*+-/ ".contains(&byte);
    let bytes = value.as_bytes();
    let q_len = bytes
        .iter()
        .map(|&byte| if is_q_literal(byte) { 1 } else { 3 })
        .sum::<usize>();
    let b_len = bytes.len().div_ceil(3) * 4;
    let encoded = if b_len < q_len {
        format!("=?utf-8?b?{}?=", STANDARD.encode(bytes))
    } else {
        const HEX: &[u8; 16] = b"0123456789ABCDEF";
        let mut output = String::with_capacity(q_len + "=?utf-8?q??=".len());
        output.push_str("=?utf-8?q?");
        for &byte in bytes {
            if byte == b' ' {
                output.push('_');
            } else if is_q_literal(byte) {
                output.push(char::from(byte));
            } else {
                output.push('=');
                output.push(char::from(HEX[usize::from(byte >> 4)]));
                output.push(char::from(HEX[usize::from(byte & 15)]));
            }
        }
        output.push_str("?=");
        output
    };
    HeaderValue::from_str(&encoded).ok()
}
fn nginx_filename(filename: &str) -> bool {
    let mut components = Path::new(filename).components();
    matches!(
        (components.next(), components.next()),
        (Some(Component::Normal(_)), None)
    ) && !invalid(filename)
        && !filename.contains(['%', '?', '#'])
        && !filename.chars().any(char::is_control)
}

fn nginx_download_response(filename: &str) -> Option<Response> {
    let redirect = django_download_header(&format!("/protected-backups/{filename}"))?;
    let disposition = download_disposition(filename)?;
    Some(
        (
            StatusCode::OK,
            [
                ("x-accel-redirect", redirect),
                ("content-disposition", disposition),
                ("content-type", HeaderValue::from_static("application/zip")),
                ("content-length", HeaderValue::from_static("0")),
            ],
            Body::empty(),
        )
            .into_response(),
    )
}

fn open_backup_download(path: PathBuf, nginx_accel: bool) -> std::io::Result<std::fs::File> {
    if !nginx_accel {
        return std::fs::File::open(path);
    }
    // nginx reopens the named file from its independently configured alias.
    // Validate an actual ordinary file within the same authorized component.
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)?;
        if !file.metadata()?.is_file() {
            return Err(std::io::Error::other("not a regular backup file"));
        }
        Ok(file)
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Err(std::io::Error::other("nginx backup handoff requires Unix"))
    }
}

fn mac(key: &[u8], value: &str) -> String {
    let mut block = [0u8; 64];
    if key.len() > 64 {
        block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        block[..key.len()].copy_from_slice(key)
    }
    let mut inner = Sha256::new();
    inner.update(block.map(|v| v ^ 0x36));
    inner.update(value.as_bytes());
    let mut outer = Sha256::new();
    outer.update(block.map(|v| v ^ 0x5c));
    outer.update(inner.finalize());
    format!("{:x}", outer.finalize())[..32].to_string()
}
impl BackupFiles {
    pub fn new(auth: AuthService, root: PathBuf, signing_key: String) -> Result<Self, StoreError> {
        if signing_key.is_empty() {
            return Err(StoreError::Unsupported("backup signing key is required"));
        }
        Ok(Self {
            auth,
            root: Arc::new(root),
            secret: Arc::new(signing_key.into_bytes()),
            workers: Arc::new(Semaphore::new(4)),
            jobs: None,
            job_access: JobAccess::None,
            schedule_disable: None,
            nginx_accel: false,
        })
    }
    /// Serve authorized downloads through a trusted nginx protected alias.
    /// The proxy must mount this service's backup root at its internal alias.
    pub fn with_nginx_accel(mut self, enabled: bool) -> Self {
        self.nginx_accel = enabled;
        self
    }
    /// Attach only a prepared, reviewed adapter. This starts no worker.
    pub fn with_jobs(mut self, jobs: Arc<dyn BackupJobApi>) -> Self {
        self.jobs = Some(jobs);
        self.job_access = JobAccess::Complete;
        self
    }
    /// Keep the ordinary schedule and restore behavior with a manual create owner.
    pub fn with_create_jobs(mut self, jobs: Arc<dyn BackupJobApi>) -> Self {
        self.jobs = Some(jobs);
        self.job_access = JobAccess::Create;
        self
    }
    /// Enable only restore and signed status using the prepared worker.
    pub fn with_restore_jobs(mut self, jobs: Arc<dyn BackupJobApi>) -> Self {
        self.jobs = Some(jobs);
        self.job_access = JobAccess::Restore;
        self
    }

    fn jobs_for(&self, kind: &str) -> Option<&Arc<dyn BackupJobApi>> {
        self.jobs.as_ref().filter(|_| self.job_access.permits(kind))
    }

    /// Attach schedule-only behavior through the existing request executor.
    pub fn with_schedule_disable(mut self, adapter: Arc<dyn BackupScheduleDisableApi>) -> Self {
        self.schedule_disable = Some(adapter);
        self
    }
    pub fn router(self) -> Router {
        Router::new()
            .route("/api/backups/", any(handle))
            .route("/api/backups/{*tail}", any(handle))
            .layer(axum::extract::DefaultBodyLimit::disable())
            .with_state(self)
    }
    pub async fn shutdown(&self, grace: Duration) -> bool {
        let started = tokio::time::Instant::now();
        let jobs_drained = match &self.jobs {
            Some(jobs) => jobs.shutdown(grace).await,
            None => true,
        };
        let grace = grace.saturating_sub(started.elapsed());
        self.workers.close();
        let deadline = tokio::time::Instant::now() + grace;
        while self.workers.available_permits() != 4 {
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(5)).await
        }
        jobs_drained
    }
    async fn io<T: Send + 'static>(
        &self,
        lease: &Lease,
        operation: impl FnOnce() -> T + Send + 'static,
    ) -> Result<T, StoreError> {
        let lease = lease.clone();
        tokio::task::spawn_blocking(move || {
            let _physical = lease;
            operation()
        })
        .await
        .map_err(|_| StoreError::Unsupported("backup filesystem worker failed"))
    }
    async fn serve(&self, peer: SocketAddr, request: Request) -> Result<Response, StoreError> {
        // Existing four filesystem/request slots, no new budget or SQL pool.
        let lease = Arc::new(self.workers.clone().try_acquire_owned().map_err(|_| {
            StoreError::StatusBody(
                503,
                json!({"detail":"Backup filesystem capacity unavailable"}),
            )
        })?);
        let mut scoped = self.clone();
        scoped.auth = scoped.auth.with_retained_scope(lease.clone());
        // Caller loss detaches this supervisor; cache operations additionally
        // retain this same owner through their physical late reply settlement.
        tokio::spawn(async move { scoped.serve_owned(peer, request, lease).await })
            .await
            .map_err(|_| StoreError::Unsupported("Backup request worker failed"))?
    }
    async fn load_callback_group(&self, key: &'static str) -> Result<Value, StoreError> {
        use sqlx::Connection;
        let mut connection = self.auth.store.pool().acquire().await?;
        let result =
            sqlx::query_scalar::<_, Value>("SELECT value FROM core_coresettings WHERE key=$1")
                .bind(key)
                .fetch_optional(&mut *connection)
                .await;
        // ERROR may arrive before Ready. Keep the actual checked-out existing
        // connection and outer request owner through Sync and physical close.
        let settled = connection.ping().await;
        let closed = connection.close().await;
        settled?;
        closed?;
        let value = result?.unwrap_or(Value::Null);
        Ok(if value.as_object().is_some_and(|v| !v.is_empty()) {
            value
        } else if key == "system_settings" {
            crate::source_system_settings_defaults()
        } else {
            json!({})
        })
    }
    async fn callback_group(&self, key: &'static str) -> Result<Value, StoreError> {
        match self.auth.notification_cache.as_ref() {
            Some(cache) => {
                cache
                    .settings_group(key, || self.load_callback_group(key))
                    .await
            }
            None => self.load_callback_group(key).await,
        }
    }
    async fn serve_owned(
        &self,
        peer: SocketAddr,
        mut request: Request,
        lease: Lease,
    ) -> Result<Response, StoreError> {
        // Pinned request_started -> refresh_display_zone catches failures. This
        // reproduces its grouped settings read, not full display/logging parity.
        let _ = self.callback_group("system_settings").await;

        let identity = crate::auth::request(peer, request.headers().clone());
        let user = self.auth.authenticate(&identity).await?;
        let tail = request
            .uri()
            .path()
            .strip_prefix("/api/backups/")
            .unwrap_or("");
        let parts: Vec<_> = tail.trim_end_matches('/').split('/').collect();
        let (kind, filename) = if tail.is_empty() {
            ("list", None)
        } else if parts.len() == 1 {
            (parts[0], None)
        } else if parts.len() == 2 && parts[0] == "schedule" && parts[1] == "update" {
            ("schedule-update", None)
        } else if parts.len() == 2 && parts[0] == "status" {
            ("status", Some(parts[1].to_owned()))
        } else if parts.len() == 2 {
            (parts[1], Some(parts[0].to_owned()))
        } else {
            return Ok(reply(404, "Not found."));
        };
        let manual = kind == "download" || kind == "status";
        if !manual {
            // Original Authenticated evaluates network access even when the
            // identity is anonymous or below the admin level.
            let policy = self.callback_group("network_access").await?;
            let ip = self.auth.client_ip(&identity);
            let network_allowed = crate::xc_auth::system_allowed(&policy, "UI", ip)?
                && match user.as_ref() {
                    Some(user) => crate::xc_auth::user_allowed(user, "UI", ip)?,
                    None => true,
                };
            let user = user.as_ref().ok_or(StoreError::Unauthorized(
                "Authentication credentials were not provided.",
            ))?;
            if !network_allowed || user.user_level < 10 {
                return Err(StoreError::PermissionDenied);
            }
        }
        let allowed = match kind {
            "list" | "download" | "download-token" | "schedule" | "status" => Method::GET,
            "upload" | "create" | "restore" => Method::POST,
            "delete" => Method::DELETE,
            "schedule-update" => Method::PUT,
            _ => return Ok(reply(404, "Not found.")),
        };
        if request.method() == Method::OPTIONS {
            return Ok(backup_options(kind, &allowed));
        }
        if request.method() != allowed {
            let mut response = reply(405, format!("Method \"{}\" not allowed.", request.method()));
            if request.method() == Method::HEAD {
                *response.body_mut() = Body::empty()
            }
            return Ok(response);
        }
        // A filename is signed as the route value, which is also used for lookup.
        // Decode once so literal percent sequences stay part of the filename.
        let filename = if kind == "status" {
            filename
        } else {
            filename.map(|raw| decode_filename(&raw))
        };
        if manual {
            let token = url::form_urlencoded::parse(request.uri().query().unwrap_or("").as_bytes())
                .filter(|(key, _)| key == "token")
                .last()
                .map(|(_, value)| value.into_owned());
            if let Some(token) = token.filter(|v| !v.is_empty()) {
                if !token.is_ascii() {
                    return Ok(source_html_500());
                }
                let signed = filename.as_deref().unwrap_or("");
                if !bool::from(mac(&self.secret, signed).as_bytes().ct_eq(token.as_bytes())) {
                    return Ok(reply(
                        403,
                        if kind == "download" {
                            "Invalid download token"
                        } else {
                            "Invalid task token"
                        },
                    ));
                }
            } else if user.is_none_or(|user| user.user_level < 10) {
                return Ok(reply(401, "Authentication required"));
            }
        }
        if kind == "status" || kind == "create" {
            let Some(jobs) = self.jobs_for(kind) else {
                return Ok(reply(501, "Native durable backup jobs are not migrated"));
            };
            // Preserve auth -> method -> token/admin ordering above. Create
            // never parses request data, including malformed JSON.
            let value = if kind == "create" {
                let id = jobs.create(lease.clone()).await?;
                json!({"detail":"Backup started","task_id":id,"task_token":mac(&self.secret,&id)})
            } else {
                jobs.status(filename.as_deref().unwrap_or(""), lease.clone())
                    .await?
            };
            let status = if kind == "create" {
                StatusCode::ACCEPTED
            } else {
                StatusCode::OK
            };
            return Ok((status, Json(value)).into_response());
        }
        if kind == "schedule-update" {
            let jobs = self.jobs_for(kind);
            if jobs.is_none() && self.schedule_disable.is_none() {
                return Ok(reply(501, "Backup schedule consumer is not prepared"));
            }
            let value = schedule_update_body(request).await?;
            let updated = if let Some(jobs) = jobs {
                jobs.update_schedule(value, lease.clone()).await?
            } else if let Some(adapter) = &self.schedule_disable {
                adapter.disable(value, &self.auth, lease.clone()).await?
            } else {
                return Ok(reply(501, "Backup schedule consumer is not prepared"));
            };
            return Ok(Json(updated).into_response());
        }
        if kind == "schedule" {
            return self.schedule().await;
        }
        if kind == "upload" {
            return self.upload(&lease, &mut request).await;
        }
        let root = self.root.as_ref().clone();
        let mkdir = root.clone();
        if self
            .io(&lease, move || std::fs::create_dir_all(mkdir))
            .await?
            .is_err()
        {
            return Ok(reply(500, "Failed to access backup directory"));
        }
        if kind == "list" {
            let result = self
                .io(&lease, move || -> std::io::Result<Value> {
                    let mut paths = std::fs::read_dir(root)?
                        .take(100001)
                        .collect::<Result<Vec<_>, _>>()?;
                    if paths.len() > 100000 {
                        return Err(std::io::Error::other("backup directory capacity"));
                    }
                    paths.sort_by_key(|v| std::cmp::Reverse(v.file_name()));
                    let mut values = vec![];
                    for entry in paths {
                        let name = entry.file_name().to_string_lossy().into_owned();
                        if !name.starts_with("dispatcharr-backup-") || !name.ends_with(".zip") {
                            continue;
                        }
                        let metadata = std::fs::metadata(entry.path())?;
                        let created: chrono::DateTime<chrono::Utc> = metadata.modified()?.into();
                        let created = created.to_rfc3339_opts(
                            if created.timestamp_subsec_micros() == 0 {
                                chrono::SecondsFormat::Secs
                            } else {
                                chrono::SecondsFormat::Micros
                            },
                            false,
                        );
                        values.push(json!({"name":name,"size":metadata.len(),"created":created}));
                    }
                    Ok(json!(values))
                })
                .await?;
            return Ok(match result {
                Ok(value) => Json(value).into_response(),
                Err(_) => reply(500, "Failed to list backups"),
            });
        }
        let Some(filename) = filename else {
            return Ok(reply(404, "Not found."));
        };
        if invalid(&filename) {
            return Ok(reply(
                if kind == "delete" { 500 } else { 404 },
                if kind == "delete" {
                    "Delete failed: Invalid filename"
                } else {
                    "Invalid filename"
                },
            ));
        }
        let path = root.join(&filename);
        let candidate = path.clone();
        let metadata = self
            .io(&lease, move || std::fs::metadata(candidate))
            .await?;
        let metadata = match metadata {
            Ok(value) => value,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(reply(404, "Backup file not found"))
            }
            Err(_) => return Ok(reply(500, "Backup file lookup failed")),
        };
        if kind == "download-token" {
            return Ok(Json(json!({"token":mac(&self.secret,&filename)})).into_response());
        }
        if kind == "restore" {
            let Some(jobs) = self.jobs_for(kind) else {
                return Ok(reply(
                    501,
                    "Native durable backup restore jobs are not migrated",
                ));
            };
            let id = jobs.restore(filename, lease.clone()).await?;
            return Ok((StatusCode::ACCEPTED,Json(json!({"detail":"Restore started","task_id":id,"task_token":mac(&self.secret,&id)}))).into_response());
        }
        if kind == "delete" {
            if !metadata.is_file() {
                return Ok(reply(
                    500,
                    format!("Delete failed: Invalid backup file: {filename}"),
                ));
            }
            let result = self.io(&lease, move || std::fs::remove_file(path)).await?;
            return Ok(if result.is_ok() {
                let mut response = reply(204, "Backup deleted successfully");
                response
                    .headers_mut()
                    .insert("content-length", HeaderValue::from_static("40"));
                *response.body_mut() = Body::empty();
                response
            } else {
                reply(500, "Delete failed")
            });
        }
        if !metadata.is_file() {
            return Ok(reply(404, "Backup file not found"));
        }
        let root_check = root.clone();
        let selected = path.clone();
        let inside = self
            .io(&lease, move || {
                std::fs::canonicalize(selected)
                    .ok()
                    .zip(std::fs::canonicalize(root_check).ok())
                    .is_some_and(|(p, r)| p.starts_with(r))
            })
            .await?;
        if !inside {
            return Ok(reply(404, "Invalid filename"));
        }
        let nginx_accel = self.nginx_accel;
        if nginx_accel && !nginx_filename(&filename) {
            return Ok(reply(404, "Invalid filename"));
        }
        let file = match self
            .io(&lease, move || open_backup_download(path, nginx_accel))
            .await?
        {
            Ok(file) => file,
            Err(_) => return Ok(reply(500, "Download failed")),
        };
        if nginx_accel {
            // Keep the upstream response valid HTTP with no entity. nginx
            // supplies the real file or its own error and the corresponding
            // downstream length from the protected alias.
            return Ok(nginx_download_response(&filename)
                .unwrap_or_else(|| reply(404, "Invalid filename")));
        }
        let state = self.clone();
        let stream = async_stream::stream! {
            let mut file=file;
            loop {
                let operation=state.io(&lease,move||{let mut buffer=vec![0;65536];let result=file.read(&mut buffer).map(|count|{buffer.truncate(count);buffer});(file,result)}).await;
                let(returned,result)=match operation {Ok(value)=>value,Err(_)=>{yield Err::<Bytes,std::io::Error>(std::io::Error::other("backup read worker failed"));break}};
                file=returned;
                match result {Ok(bytes)if bytes.is_empty()=>break,Ok(bytes)=>yield Ok::<Bytes,std::io::Error>(Bytes::from(bytes)),Err(error)=>{yield Err::<Bytes,std::io::Error>(error);break}}
            }
        };
        let mut response = Response::new(Body::from_stream(stream));
        response
            .headers_mut()
            .insert("content-type", HeaderValue::from_static("application/zip"));
        response.headers_mut().insert(
            "content-length",
            HeaderValue::from_str(&metadata.len().to_string()).unwrap(),
        );
        if let Some(value) = download_disposition(&filename) {
            response.headers_mut().insert("content-disposition", value);
        } else {
            return Ok(reply(
                501,
                "Backup download filename header cannot be represented",
            ));
        }
        Ok(response)
    }
    async fn schedule(&self) -> Result<Response, StoreError> {
        let raw: Option<Option<Value>> =
            sqlx::query_scalar("SELECT value FROM core_coresettings WHERE key='backup_settings'")
                .fetch_optional(self.auth.store.pool())
                .await?;
        Ok(match schedule_payload(raw.flatten().as_ref()) {
            Ok(value) => Json(value).into_response(),
            Err(error) => reply(500, format!("Failed to get schedule: {error}")),
        })
    }
    async fn upload(&self, lease: &Lease, request: &mut Request) -> Result<Response, StoreError> {
        let incoming = std::mem::replace(request, Request::new(Body::empty()));
        let (parts, body) = incoming.into_parts();
        if parts
            .headers
            .get("content-length")
            .is_some_and(|v| v == "0")
        {
            return Ok(reply(400, "No file uploaded"));
        }
        let content_type = parts
            .headers
            .get("content-type")
            .map(|v| dispatcharr_contracts::source_header_value(v.as_bytes()))
            .unwrap_or_default();
        let mime = content_type
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .to_lowercase();
        if mime == "application/x-www-form-urlencoded" {
            let mut stream = body.into_data_stream();
            let mut size = 0;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|_| {
                    StoreError::StatusBody(400, json!({"detail":"Form parse error"}))
                })?;
                size += chunk.len();
                if size > 2621440 {
                    return Ok(reply(413, "Backup form capacity exceeded"));
                }
            }
            return Ok(reply(400, "No file uploaded"));
        }
        if mime != "multipart/form-data" {
            return Ok(reply(
                415,
                format!("Unsupported media type \"{content_type}\" in request."),
            ));
        }
        let boundary = match multer::parse_boundary(&content_type) {
            Ok(boundary)
                if !boundary.is_empty()
                    && boundary.len() <= 201
                    && boundary.bytes().all(|v| (32..=126).contains(&v))
                    && boundary.as_bytes().last().is_some_and(|v| *v != b' ') =>
            {
                boundary
            }
            _ => {
                return Ok(reply(
                    400,
                    "Multipart form parse error - Invalid boundary in multipart: None",
                ))
            }
        };
        let failure = Arc::new(std::sync::Mutex::new(None));
        let stream_failure = failure.clone();
        let mut guard = multipart_guard::Guard::new(&boundary, 128 << 30);
        let mut incoming = body.into_data_stream();
        let guarded = async_stream::stream! {while let Some(chunk)=incoming.next().await{match chunk{Ok(chunk)=>{if let Err(error)=guard.inspect(&chunk){*stream_failure.lock().unwrap()=Some(error);yield Err::<Bytes,std::io::Error>(std::io::Error::other("bounded multipart envelope rejected"));break}yield Ok::<Bytes,std::io::Error>(chunk)},Err(_)=>{yield Err::<Bytes,std::io::Error>(std::io::Error::other("multipart body interrupted"));break}}}};
        let incoming = Request::from_parts(parts, Body::from_stream(guarded));
        let mut multipart = match Multipart::from_request(incoming, &()).await {
            Ok(value) => value,
            Err(_) => return Ok(reply(400, "Multipart form parse error")),
        };
        let mut selected = None;
        let mut spool = None;
        let mut file = None;
        let mut fields = 0;
        let mut files = 0;
        let mut field_bytes = 0;
        loop {
            let part = match multipart.next_field().await {
                Ok(value) => value,
                Err(_) => return Ok(multipart_error(&failure)),
            };
            let Some(mut part) = part else { break };
            let name = part.name().map(str::to_owned);
            let filename = part.file_name().and_then(sanitize_filename);
            let is_file = filename.is_some();
            if is_file {
                files += 1;
                if files > 100 {
                    return Ok(reply(413, "Backup file count capacity exceeded"));
                }
            } else {
                fields += 1;
                if fields > 1000 {
                    return Ok(reply(413, "Backup form field count capacity exceeded"));
                }
            }
            let chosen = if name.as_deref() == Some("file") {
                filename
            } else {
                None
            };
            if chosen.is_some() {
                if spool.is_none() {
                    let temporary = std::env::temp_dir().join(format!(
                        "dispatcharr-backup-upload-{}",
                        uuid::Uuid::new_v4()
                    ));
                    let path = temporary.clone();
                    let opened = self
                        .io(lease, move || {
                            let mut options = std::fs::OpenOptions::new();
                            options.write(true).create_new(true);
                            #[cfg(unix)]
                            {
                                use std::os::unix::fs::OpenOptionsExt;
                                options.mode(0o600);
                            }
                            options.open(path)
                        })
                        .await?;
                    let opened = match opened {
                        Ok(value) => value,
                        Err(_) => return Ok(reply(500, "Upload failed")),
                    };
                    spool = Some(TemporaryUpload {
                        path: temporary,
                        lease: lease.clone(),
                    });
                    file = Some(opened)
                }
                let mut opened = file.take().unwrap();
                let result = self
                    .io(lease, move || {
                        let result = opened.set_len(0).and_then(|()| opened.rewind());
                        (opened, result)
                    })
                    .await?;
                file = Some(result.0);
                if result.1.is_err() {
                    return Ok(reply(500, "Upload failed"));
                }
            }
            while let Some(chunk) = match part.chunk().await {
                Ok(value) => value,
                Err(_) => return Ok(multipart_error(&failure)),
            } {
                if chosen.is_some() {
                    let mut opened = file.take().unwrap();
                    let result = self
                        .io(lease, move || {
                            let result = opened.write_all(&chunk);
                            (opened, result)
                        })
                        .await?;
                    file = Some(result.0);
                    if result.1.is_err() {
                        return Ok(reply(500, "Upload failed"));
                    }
                } else if !is_file {
                    field_bytes += chunk.len();
                    if field_bytes > 2621440 {
                        return Ok(reply(413, "Backup form capacity exceeded"));
                    }
                }
            }
            if let Some(filename) = chosen {
                selected = Some(filename)
            }
        }
        let Some(filename) = selected else {
            return Ok(reply(400, "No file uploaded"));
        };
        drop(file);
        let temporary = spool.as_ref().unwrap().path.clone();
        let root = self.root.as_ref().clone();
        let outcome = self
            .io(lease, move || -> Result<String, &'static str> {
                std::fs::create_dir_all(&root).map_err(|_| "Upload failed")?;
                let canonical = std::fs::canonicalize(&root).map_err(|_| "Upload failed")?;
                let original = root.join(&filename);
                if original.exists() {
                    let resolved =
                        std::fs::canonicalize(&original).map_err(|_| "Invalid filename.")?;
                    if !resolved.starts_with(&canonical) {
                        return Err("Invalid filename.");
                    }
                }
                let mut path = original;
                let mut counter = 1;
                while path.exists() {
                    let unique = if let Some((stem, extension)) = filename.rsplit_once('.') {
                        format!("{stem}-{counter}.{extension}")
                    } else {
                        format!("{filename}-{counter}")
                    };
                    path = root.join(unique);
                    counter += 1;
                    if counter > 100000 {
                        return Err("Upload filename capacity exceeded");
                    }
                }
                let mut input = std::fs::File::open(temporary).map_err(|_| "Upload failed")?;
                let mut output = std::fs::File::create(&path).map_err(|_| "Upload failed")?;
                std::io::copy(&mut input, &mut output).map_err(|_| "Upload failed")?;
                Ok(path
                    .file_name()
                    .and_then(|v| v.to_str())
                    .ok_or("Invalid filename.")?
                    .to_owned())
            })
            .await?;
        drop(spool);
        Ok(match outcome {
            Ok(filename) => (
                StatusCode::CREATED,
                Json(json!({"detail":"Backup uploaded successfully","filename":filename})),
            )
                .into_response(),
            Err("Invalid filename.") => reply(400, "Invalid filename."),
            Err(error) => reply(500, error),
        })
    }
}
fn backup_options(kind: &str, method: &Method) -> Response {
    let (name, description) = match kind {
        "list" => ("List Backups", "List all available backup files."),
        "download" => ("Download Backup", "Download a backup file.\n\nRequires either:\n- Valid admin authentication, OR\n- Valid download_token query parameter"),
        "create" => ("Create Backup", "Create a new backup (async via Celery)."),
        "upload" => ("Upload Backup", "Upload a backup file for restoration."),
        "schedule" => ("Get Schedule", "Get backup schedule settings."),
        "schedule-update" => ("Update Schedule", "Update backup schedule settings."),
        "status" => ("Backup Status", "Check the status of a backup/restore task.\n\nRequires either:\n- Valid admin authentication, OR\n- Valid task_token query parameter"),
        "download-token" => ("Get Download Token", "Get a signed token for downloading a backup file."),
        "delete" => ("Delete Backup", "Delete a backup file."),
        "restore" => ("Restore Backup", "Restore from a backup file (async via Celery). WARNING: This will flush the database!"),
        _ => return reply(404, "Not found."),
    };
    let allow = match *method {
        Method::GET => "OPTIONS, GET",
        Method::POST => "OPTIONS, POST",
        Method::PUT => "OPTIONS, PUT",
        Method::DELETE => "OPTIONS, DELETE",
        _ => return reply(404, "Not found."),
    };
    let parsers: &[&str] = if kind == "upload" {
        &["multipart/form-data", "application/x-www-form-urlencoded"]
    } else {
        &[
            "application/json",
            "application/x-www-form-urlencoded",
            "multipart/form-data",
        ]
    };
    let response = Json(json!({
        "name": name,
        "description": description,
        "renders": ["application/json", "text/html"],
        "parses": parsers,
    }))
    .into_response();
    channel_options_headers(response, HeaderValue::from_static(allow))
}

async fn handle(
    State(state): State<BackupFiles>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Response {
    match state.serve(peer, request).await {
        Ok(value) => value,
        Err(error) => error.into_response(),
    }
}

fn sanitize_filename(filename: &str) -> Option<String> {
    // Django's multipart filename sanitization runs before safe_upload_path.
    let filename = crate::html_filename::unescape(filename);
    let filename = filename
        .rsplit(['/', '\\'])
        .next()?
        .chars()
        .filter(|c| crate::python_action::printable(*c))
        .collect::<String>();
    let filename = filename.as_str();
    if ["", ".", ".."].contains(&filename) {
        return None;
    }
    let mut filename = filename.to_owned();
    if filename.chars().count() > 255 {
        let (stem, extension) = filename
            .rsplit_once('.')
            .map_or((filename.as_str(), String::new()), |(s, e)| {
                (s, format!(".{e}"))
            });
        let extension = extension.chars().take(255).collect::<String>();
        let stem = stem
            .chars()
            .take(255 - extension.chars().count())
            .collect::<String>();
        filename = stem + &extension;
    }
    Some(filename)
}

#[cfg(test)]
#[path = "backup_files_tests.rs"]
mod tests;

async fn schedule_update_body(request: Request) -> Result<Value, StoreError> {
    let (parts, body) = request.into_parts();
    let raw = axum::body::to_bytes(body, 65536)
        .await
        .map_err(|_| StoreError::StatusBody(400, json!({"detail":"Invalid request body"})))?;
    if raw.is_empty() {
        return Ok(Value::Object(serde_json::Map::new()));
    }
    let content_type = parts
        .headers
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    let media = content_type.split(';').next().unwrap_or("").trim();
    if media.eq_ignore_ascii_case("application/x-www-form-urlencoded") {
        let form = url::form_urlencoded::parse(&raw)
            .map(|(key, value)| (key.into_owned(), Value::String(value.into_owned())))
            .collect();
        return Ok(Value::Object(form));
    }
    if media.eq_ignore_ascii_case("multipart/form-data") {
        let invalid_form =
            || StoreError::StatusBody(400, json!({"detail":"Invalid schedule form"}));
        let body = Request::from_parts(parts, Body::from(raw));
        let mut multipart = Multipart::from_request(body, &())
            .await
            .map_err(|_| invalid_form())?;
        let mut form = serde_json::Map::new();
        while let Some(field) = multipart.next_field().await.map_err(|_| invalid_form())? {
            let Some(name) = field.name().map(str::to_owned) else {
                continue;
            };
            if field.file_name().is_some() {
                return Err(StoreError::Unsupported(
                    "File-valued backup schedule fields",
                ));
            }
            let value = field.text().await.map_err(|_| invalid_form())?;
            form.insert(name, Value::String(value));
        }
        return Ok(Value::Object(form));
    }
    serde_json::from_slice(&raw)
        .map_err(|_| StoreError::StatusBody(400, json!({"detail":"JSON parse error"})))
}

#[path = "backup_multipart_guard.rs"]
mod multipart_guard;

struct TemporaryUpload {
    path: PathBuf,
    lease: Lease,
}
impl Drop for TemporaryUpload {
    fn drop(&mut self) {
        let cleanup = TemporaryCleanup {
            path: self.path.clone(),
            _lease: self.lease.clone(),
        };
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn_blocking(move || drop(cleanup));
        } else {
            drop(cleanup)
        }
    }
}
// If a runtime rejects a queued cleanup during shutdown, dropping its closure
// still unlinks the private file while retaining the physical-operation lease.
struct TemporaryCleanup {
    path: PathBuf,
    _lease: Lease,
}
impl Drop for TemporaryCleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn multipart_error(failure: &std::sync::Mutex<Option<multipart_guard::GuardError>>) -> Response {
    match *failure.lock().unwrap() {
        Some(multipart_guard::GuardError::Headers) => reply(
            400,
            "Multipart form parse error - Request max total header size exceeded.",
        ),
        Some(multipart_guard::GuardError::Body) => reply(413, "Backup upload capacity exceeded"),
        None => reply(400, "Multipart form parse error"),
    }
}

fn source_html_500() -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        [("content-type", "text/html; charset=utf-8")],
        r#"
<!doctype html>
<html lang="en">
<head>
  <title>Server Error (500)</title>
</head>
<body>
  <h1>Server Error (500)</h1><p></p>
</body>
</html>
"#,
    )
        .into_response()
}

fn decode_filename(raw: &str) -> String {
    url::form_urlencoded::parse(
        format!("f={}", raw.replace('&', "%26").replace('+', "%2B")).as_bytes(),
    )
    .next()
    .map(|(_, value)| value.into_owned())
    .unwrap_or_default()
}

#[cfg(test)]
#[path = "backup_callback_physical_tests.rs"]
mod callback_physical_tests;
