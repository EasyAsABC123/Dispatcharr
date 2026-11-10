//! Job-free original backup HTTP/filesystem contracts. Original authors retain AGPL credit.
//! Accepted job-free routes only; create/restore/status jobs remain explicit501.
use crate::{AuthService, StoreError};
use axum::{
    body::{Body, Bytes},
    extract::{ConnectInfo, FromRequest, Multipart, Request, State},
    http::{HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Json, Router,
};
use futures_util::StreamExt;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    io::{Read, Seek, Write},
    net::SocketAddr,
    path::PathBuf,
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
fn invalid(name: &str) -> bool {
    name.contains("..") || name.contains('/') || name.contains('\\')
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
        })
    }
    pub fn router(self) -> Router {
        Router::new()
            .route("/api/backups/", any(handle))
            .route("/api/backups/{*tail}", any(handle))
            .layer(axum::extract::DefaultBodyLimit::disable())
            .with_state(self)
    }
    pub async fn shutdown(&self, grace: Duration) -> bool {
        self.workers.close();
        let deadline = tokio::time::Instant::now() + grace;
        while self.workers.available_permits() != 4 {
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(5)).await
        }
        true
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
    async fn serve(&self, peer: SocketAddr, mut request: Request) -> Result<Response, StoreError> {
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
        } else if parts.len() == 2 && parts[0] == "status" {
            ("status", Some(parts[1].to_owned()))
        } else if parts.len() == 2 {
            (parts[1], Some(parts[0].to_owned()))
        } else {
            return Ok(reply(404, "Not found."));
        };
        let manual = kind == "download" || kind == "status";
        if !manual {
            let user = user.as_ref().ok_or(StoreError::Unauthorized(
                "Authentication credentials were not provided.",
            ))?;
            if user.user_level < 10
                || !self
                    .auth
                    .source_network_allowed(self.auth.client_ip(&identity), "UI", Some(user))
                    .await?
            {
                return Err(StoreError::PermissionDenied);
            }
        }
        let allowed = match kind {
            "list" | "download" | "download-token" | "schedule" | "status" => Method::GET,
            "upload" | "create" | "restore" => Method::POST,
            "delete" => Method::DELETE,
            _ => return Ok(reply(404, "Not found.")),
        };
        if request.method() == Method::OPTIONS {
            return Ok(self.options(kind));
        }
        if request.method() != allowed {
            let mut response = reply(405, format!("Method \"{}\" not allowed.", request.method()));
            if request.method() == Method::HEAD {
                *response.body_mut() = Body::empty()
            }
            return Ok(response);
        }
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
            return Ok(reply(501, "Native durable backup jobs are not migrated"));
        }
        if kind == "schedule" {
            return self.schedule().await;
        }
        let lease = Arc::new(self.workers.clone().try_acquire_owned().map_err(|_| {
            StoreError::StatusBody(
                503,
                json!({"detail":"Backup filesystem capacity unavailable"}),
            )
        })?);
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
        let filename = decode_filename(&filename);
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
            return Ok(reply(
                501,
                "Native durable backup restore jobs are not migrated",
            ));
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
        let file = match self.io(&lease, move || std::fs::File::open(path)).await? {
            Ok(file) => file,
            Err(_) => return Ok(reply(500, "Download failed")),
        };
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
        let disposition = format!("attachment; filename=\"{filename}\"");
        let latin: Option<Vec<u8>> = disposition
            .chars()
            .map(|v| u8::try_from(v as u32).ok())
            .collect();
        if let Some(value) = latin.and_then(|v| HeaderValue::from_bytes(&v).ok()) {
            response.headers_mut().insert("content-disposition", value);
        } else {
            return Ok(reply(
                501,
                "Non-Latin1 backup download filenames are not migrated",
            ));
        }
        Ok(response)
    }
    fn options(&self, kind: &str) -> Response {
        let(name,description)=match kind{"list"=>("List Backups","List all available backup files."),"download"=>("Download Backup","Download a backup file.\n\nRequires either:\n- Valid admin authentication, OR\n- Valid download_token query parameter"),_=>return reply(501,"Backup metadata contract is not migrated")};
        Json(json!({"name":name,"description":description,"renders":["application/json","text/html"],"parses":["application/json","application/x-www-form-urlencoded","multipart/form-data"]})).into_response()
    }
    async fn schedule(&self) -> Result<Response, StoreError> {
        let raw: Option<Option<Value>> =
            sqlx::query_scalar("SELECT value FROM core_coresettings WHERE key='backup_settings'")
                .fetch_optional(self.auth.store.pool())
                .await?;
        let values = raw.flatten().filter(Value::is_object).unwrap_or(json!({}));
        let get = |name: &str, default: Value| values.get(name).cloned().unwrap_or(default);
        let text = |value: Value| {
            value
                .as_str()
                .map(str::to_owned)
                .or_else(|| crate::python_action::collection_text(&value, 1 << 20))
                .unwrap_or_default()
        };
        let integer = |value: Value| -> Option<Value> {
            let string = match value {
                Value::Bool(v) => {
                    if v {
                        "1".into()
                    } else {
                        "0".into()
                    }
                }
                Value::Number(v) if v.to_string().contains(['.', 'e', 'E']) => {
                    format!("{:.0}", v.as_f64()?.trunc())
                }
                Value::Number(v) => v.to_string(),
                Value::String(v) => v,
                _ => return None,
            };
            let value = dispatcharr_ingest_core::source_integer_literal(&string)?;
            serde_json::from_str(&value).ok()
        };
        let Some(day) = integer(get("schedule_day_of_week", json!(0))) else {
            return Ok(reply(
                501,
                "Malformed backup schedule integer contract is not migrated",
            ));
        };
        let Some(retention) = integer(get("retention_count", json!(3))) else {
            return Ok(reply(
                501,
                "Malformed backup schedule integer contract is not migrated",
            ));
        };
        Ok(Json(json!({"enabled":crate::channels::truthy(Some(&get("schedule_enabled",json!(true)))),"frequency":text(get("schedule_frequency",json!("daily"))),"time":text(get("schedule_time",json!("03:00"))),"day_of_week":day,"retention_count":retention,"cron_expression":text(get("schedule_cron_expression",json!("")))})).into_response())
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
