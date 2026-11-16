//! Owned bounded Stream FileField multipart/storage operations.
//! Original Dispatcharr/Django/DRF authors retain all application credit.
use crate::{AuthService, StoreError};
use axum::{
    extract::{DefaultBodyLimit, FromRequest, Multipart, Request},
    http::HeaderMap,
};
use futures_util::FutureExt;
use rand::{distributions::Alphanumeric, Rng};
use serde_json::{json, Map, Value};
use std::{
    future::Future,
    io::Write,
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    },
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{Notify, OwnedSemaphorePermit};
const FILE: usize = 5 * 1024 * 1024;
const BODY: usize = 8 * 1024 * 1024;
fn unavailable() -> StoreError {
    StoreError::StatusBody(503, json!({"detail":"Stream file service unavailable."}))
}
fn parser() -> StoreError {
    StoreError::InvalidInput(json!({"detail":"Multipart form parse error"}))
}
fn field_error(message: String) -> StoreError {
    StoreError::InvalidInput(json!({"local_file":[message]}))
}
#[derive(Default)]
struct State {
    used: bool,
    closing: bool,
    fault: bool,
    active: usize,
}
#[derive(Clone)]
pub(crate) struct Runtime {
    state: Arc<Mutex<State>>,
    changed: Arc<Notify>,
    root: PathBuf,
}
pub(crate) struct Work {
    runtime: Runtime,
    permit: Arc<OwnedSemaphorePermit>,
}
impl Drop for Work {
    fn drop(&mut self) {
        if let Ok(mut state) = self.runtime.state.lock() {
            state.active -= 1;
        }
        self.runtime.changed.notify_waiters();
    }
}
impl Runtime {
    pub(crate) fn new() -> Self {
        Self {
            state: Default::default(),
            changed: Default::default(),
            root: PathBuf::from(
                std::env::var("RUST_EPG_MEDIA_ROOT").unwrap_or_else(|_| "/app/media".into()),
            )
            .join("uploads"),
        }
    }
    fn enter(&self, permit: Arc<OwnedSemaphorePermit>) -> Result<Arc<Work>, StoreError> {
        let mut state = self.state.lock().map_err(|_| unavailable())?;
        if state.closing || state.fault {
            return Err(unavailable());
        }
        state.active += 1;
        state.used = true;
        Ok(Arc::new(Work {
            runtime: self.clone(),
            permit,
        }))
    }
    pub(crate) fn stop(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.closing = true;
        }
    }
    pub(crate) fn ready(&self) -> bool {
        self.state.lock().is_ok_and(|s| !s.closing && !s.fault)
    }
    pub(crate) async fn shutdown(&self, budget: Duration) -> bool {
        self.stop();
        tokio::time::timeout(budget, async {
            loop {
                let notified = self.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if let Ok(state) = self.state.lock() {
                    if state.active == 0 {
                        return !state.fault;
                    }
                } else {
                    return false;
                }
                notified.await;
            }
        })
        .await
        .unwrap_or(false)
    }
}
impl AuthService {
    pub fn stream_files_were_used(&self) -> bool {
        self.stream_files
            .state
            .lock()
            .map_or(true, |state| state.used)
    }
    pub fn stream_files_ready(&self) -> bool {
        self.stream_files.ready()
    }
    pub fn stop_stream_file_admission(&self) {
        self.stream_files.stop()
    }
    pub async fn shutdown_stream_files(&self, budget: Duration) -> bool {
        self.stream_files.shutdown(budget).await
    }
}
pub(crate) async fn owned<T, F, U>(auth: AuthService, work: F) -> Result<T, StoreError>
where
    T: Send + 'static,
    F: FnOnce(AuthService, Arc<Work>) -> U + Send + 'static,
    U: Future<Output = Result<T, StoreError>> + Send + 'static,
{
    let guard = auth.stream_files.enter(auth.task_sql_writer()?)?;
    let auth = auth
        .with_request_writer_permit(guard.permit.clone())?
        .with_retained_scope(guard.clone());
    tokio::spawn(async move {
        let result = std::panic::AssertUnwindSafe(work(auth, guard.clone()))
            .catch_unwind()
            .await;
        match result {
            Ok(Err(StoreError::Database(sqlx::Error::Database(_)))) => {
                Err(StoreError::UnhandledRestFault)
            }
            Ok(Err(StoreError::Database(_))) => {
                if let Ok(mut state) = guard.runtime.state.lock() {
                    state.fault = true;
                    state.closing = true;
                }
                std::mem::forget(guard);
                Err(unavailable())
            }
            Ok(value) => value,
            Err(_) => {
                if let Ok(mut state) = guard.runtime.state.lock() {
                    state.fault = true;
                    state.closing = true;
                } // Unknown panic cannot release/recycle actual admitted work.
                std::mem::forget(guard);
                Err(StoreError::UnhandledRestFault)
            }
        }
    })
    .await
    .map_err(|_| StoreError::UnhandledRestFault)?
}
pub(crate) fn multipart(headers: &HeaderMap) -> bool {
    headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.split(';').next().unwrap_or("").trim() == "multipart/form-data")
}
pub(crate) struct Upload {
    name: String,
    bytes: Vec<u8>,
}
pub(crate) struct Input {
    pub fields: Map<String, Value>,
    pub file: Option<Upload>,
    pub error: Map<String, Value>,
}
fn filename(raw: &str) -> Result<String, StoreError> {
    // UploadedFile and multipart parsing strip path components before storage.
    // Non-ASCII/entity/control grammar stays explicit until its pinned vectors.
    if !raw.is_ascii() || raw.bytes().any(|b| b.is_ascii_control() || b == b'&') {
        return Err(StoreError::Unsupported(
            "Stream filename Unicode/entity/control grammar is not qualified",
        ));
    }
    let raw = raw.rsplit(['/', '\\']).next().unwrap_or("");
    if raw.is_empty() || raw == "." || raw == ".." {
        return Err(StoreError::Unsupported(
            "Empty/dot Stream upload filename is not qualified",
        ));
    }
    if raw.len() > 255 {
        return Err(StoreError::Unsupported(
            "Over255 Stream filename truncation is not qualified",
        ));
    }
    if raw.len() > 100 {
        return Err(field_error(format!(
            "Ensure this filename has at most 100 characters (it has {}).",
            raw.len()
        )));
    }
    Ok(raw.to_owned())
}
pub(crate) async fn parse(mut request: Request, partial: bool) -> Result<Input, StoreError> {
    DefaultBodyLimit::max(BODY).apply(&mut request);
    let mut multipart = Multipart::from_request(request, &())
        .await
        .map_err(|_| parser())?;
    let mut fields = Map::new();
    let mut file = None;
    let mut file_error = None;
    let mut had_file = false;
    let mut count = 0usize;
    let mut total = 0usize;
    while let Some(mut part) = multipart.next_field().await.map_err(|_| parser())? {
        count += 1;
        if count > 1000 {
            return Err(parser());
        }
        if part.headers().contains_key("content-transfer-encoding") {
            return Err(StoreError::Unsupported(
                "Stream transfer-encoded multipart is not qualified",
            ));
        }
        let name = part.name().unwrap_or("").to_owned();
        if name.len() > 1024 {
            return Err(parser());
        }
        let uploaded = part.file_name().map(str::to_owned);
        let mut bytes = Vec::new();
        while let Some(chunk) = part.chunk().await.map_err(|_| parser())? {
            total = total.checked_add(chunk.len()).ok_or_else(unavailable)?;
            if total > BODY
                || bytes
                    .len()
                    .checked_add(chunk.len())
                    .is_none_or(|v| v > if uploaded.is_some() { FILE } else { 65536 })
            {
                return Err(StoreError::StatusBody(
                    413,
                    json!({"detail":"Stream upload exceeds bounded service capacity."}),
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        if let Some(uploaded) = uploaded {
            if name == "local_file" {
                had_file = true;
                file = None;
                file_error = None;
                match filename(&uploaded) {
                    Ok(name) => {
                        if bytes.is_empty() {
                            file_error = Some("The submitted file is empty.".into());
                        } else {
                            file = Some(Upload { name, bytes });
                        }
                    }
                    Err(StoreError::InvalidInput(errors)) => {
                        file_error = if bytes.is_empty() {
                            Some("The submitted file is empty.".into())
                        } else {
                            errors["local_file"][0].as_str().map(str::to_owned)
                        };
                    }
                    Err(error) => return Err(error),
                }
            } else {
                fields.insert(name, json!({"file":true}));
            }
        } else {
            let value = String::from_utf8(bytes).map_err(|_| {
                StoreError::Unsupported("Stream form charset replacement is not qualified")
            })?;
            fields.insert(name, Value::String(value));
        }
    }
    if had_file {
        fields.remove("local_file");
    } else if fields.get("local_file") == Some(&json!("")) {
        fields.insert("local_file".into(), Value::Null);
    }
    for name in [
        "is_stale",
        "is_adult",
        "is_custom",
        "is_catchup",
        "is_radio",
    ] {
        if fields.get(name) == Some(&json!("")) {
            fields.remove(name);
        } else if !partial && !fields.contains_key(name) {
            fields.insert(name.into(), Value::Bool(false));
        }
    }
    for name in [
        "channel_group",
        "stream_profile_id",
        "m3u_account",
        "stream_id",
        "stream_chno",
        "stream_stats_updated_at",
        "stream_hash",
    ] {
        if fields.get(name) == Some(&json!("")) {
            fields.insert(name.into(), Value::Null);
        }
    }
    for name in ["name", "current_viewers", "catchup_days", "last_seen"] {
        if fields.get(name) == Some(&json!("")) {
            fields.remove(name);
        }
    }
    let mut errors = Map::new();
    if let Some(error) = file_error {
        errors.insert("local_file".into(), json!([error]));
    }
    if let Some(Value::String(raw)) = fields.get("stream_stats") {
        match serde_json::from_str::<Value>(raw) {
            Ok(value) => {
                fields.insert("stream_stats".into(), value);
            }
            Err(_) => {
                if raw.contains("NaN") || raw.contains("Infinity") {
                    return Err(StoreError::Unsupported(
                        "Nonfinite Stream HTML JSON is not qualified",
                    ));
                }
                errors.insert("stream_stats".into(), json!(["Value must be valid JSON."]));
                fields.remove("stream_stats");
            }
        }
    }
    Ok(Input {
        fields,
        file,
        error: errors,
    })
}
fn valid_name(raw: &str) -> Result<String, StoreError> {
    let name = raw
        .trim_matches(|c: char| c.is_ascii_whitespace())
        .replace(' ', "_")
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || "-_.".contains(*c))
        .collect::<String>();
    if name.is_empty() || name == "." || name == ".." {
        return Err(StoreError::Unsupported(
            "Stream filename storage rejection rendering is not qualified",
        ));
    }
    Ok(name)
}
fn candidate(root: &str, extension: &str) -> String {
    let suffix: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(7)
        .map(char::from)
        .collect();
    format!("{root}_{suffix}{extension}")
}
fn io(_: std::io::Error) -> StoreError {
    StoreError::UnhandledRestFault
}
fn suffixes(name: &str) -> (&str, &str) {
    let stripped = name.trim_start_matches('.');
    let offset = name.len() - stripped.len();
    let dot = if name.ends_with('.') {
        None
    } else {
        stripped.find('.').map(|n| offset + n)
    };
    dot.map_or((name, ""), |n| (&name[..n], &name[n..]))
}
fn available(
    directory: &std::fs::File,
    initial: &str,
    maximum: Option<usize>,
) -> Result<String, StoreError> {
    let (stem, extension) = suffixes(initial);
    let mut root = stem.to_owned();
    let mut selected = initial.to_owned();
    for _ in 0..256 {
        let raw = std::ffi::CString::new(selected.as_str()).map_err(|_| parser())?;
        let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
        let exists = unsafe {
            libc::fstatat(
                directory.as_raw_fd(),
                raw.as_ptr(),
                metadata.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        } == 0;
        if !exists && std::io::Error::last_os_error().kind() != std::io::ErrorKind::NotFound {
            return Err(io(std::io::Error::last_os_error()));
        }
        if !exists && maximum.is_none_or(|limit| selected.len() + 8 <= limit) {
            return Ok(selected);
        }
        selected = candidate(&root, extension);
        if let Some(limit) = maximum {
            let excess = (selected.len() + 8).saturating_sub(limit);
            if excess > 0 {
                if excess >= root.len() {
                    return Err(StoreError::Unsupported(
                        "Stream storage max-length rejection is not qualified",
                    ));
                }
                root.truncate(root.len() - excess);
                selected = candidate(&root, extension);
            }
        }
    }
    Err(unavailable())
}
fn save(root: PathBuf, upload: Upload) -> Result<String, StoreError> {
    if !root.is_absolute() {
        return Err(StoreError::Unsupported(
            "Relative Stream media root is not qualified",
        ));
    }
    let name = valid_name(&upload.name)?;
    let (stem, extension) = suffixes(&name);
    if name.len() + 8 > 100 && stem.len() + extension.len() + 16 - 100 >= stem.len() {
        return Err(StoreError::Unsupported(
            "Stream storage max-length rejection is not qualified",
        ));
    }
    std::fs::create_dir_all(&root).map_err(io)?;
    if root.is_symlink() {
        return Err(StoreError::Unsupported(
            "Symlinked Stream upload root is not qualified",
        ));
    }
    let directory = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(&root)
        .map_err(io)?;
    let mut selected = available(&directory, &name, Some(100))?;
    for _ in 0..256 {
        let raw = std::ffi::CString::new(selected.as_str()).map_err(|_| parser())?;
        let fd = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                raw.as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                0o666,
            )
        };
        if fd < 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                // Actual FileSystemStorage race retries without max_length.
                selected = available(&directory, &selected, None)?;
                continue;
            }
            return Err(io(error));
        }
        let mut file = unsafe { std::fs::File::from_raw_fd(fd) };
        if unsafe { libc::flock(fd, libc::LOCK_EX) } != 0 {
            return Err(io(std::io::Error::last_os_error()));
        }
        file.write_all(&upload.bytes).map_err(io)?;
        if unsafe { libc::flock(fd, libc::LOCK_UN) } != 0 {
            return Err(io(std::io::Error::last_os_error()));
        }
        file.set_permissions(std::fs::Permissions::from_mode(0o644))
            .map_err(io)?;
        let media_gid = std::fs::metadata(root.parent().ok_or_else(unavailable)?)
            .map_err(io)?
            .gid();
        if file.metadata().map_err(io)?.gid() != media_gid
            && unsafe { libc::fchown(fd, libc::uid_t::MAX, media_gid) } != 0
        {
            let error = std::io::Error::last_os_error();
            if error.kind() != std::io::ErrorKind::PermissionDenied {
                return Err(io(error));
            }
        }
        // Source does not delete this committed file on later DB failure, replace,
        // clear or model delete; preserve that orphan behavior deliberately.
        return Ok(format!("uploads/{selected}"));
    }
    Err(unavailable())
}
pub(crate) async fn store(file: Upload, owner: Arc<Work>) -> Result<String, StoreError> {
    let root = owner.runtime.root.clone();
    tokio::task::spawn_blocking(move || {
        let _owner = owner;
        save(root, file)
    })
    .await
    .map_err(|_| StoreError::UnhandledRestFault)?
}

pub(crate) fn combine(
    result: Result<Map<String, Value>, StoreError>,
    extra: Map<String, Value>,
) -> Result<Map<String, Value>, StoreError> {
    if extra.is_empty() {
        return result;
    }
    match result {
        Ok(_) => Err(StoreError::InvalidInput(Value::Object(extra))),
        Err(StoreError::InvalidInput(Value::Object(mut errors))) => {
            errors.extend(extra);
            Err(StoreError::InvalidInput(Value::Object(errors)))
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Directory(PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    #[test]
    fn pinned_storage_all_suffixes_collision_length_bytes_and_modes() {
        let parent = Directory(std::env::temp_dir().join(format!(
            "dispatcharr-owned-file-test-{}",
            uuid::Uuid::new_v4()
        )));
        let root = parent.0.join("uploads");
        let first = save(
            root.clone(),
            Upload {
                name: "owned file.tar.gz".into(),
                bytes: b"original bytes".to_vec(),
            },
        )
        .unwrap();
        assert_eq!(first, "uploads/owned_file.tar.gz");
        let second = save(
            root.clone(),
            Upload {
                name: "owned file.tar.gz".into(),
                bytes: b"replacement bytes".to_vec(),
            },
        )
        .unwrap();
        assert!(second.starts_with("uploads/owned_file_") && second.ends_with(".tar.gz"));
        assert_eq!(second.len(), "uploads/owned_file_.tar.gz".len() + 7);
        assert_eq!(
            std::fs::read(root.join("owned_file.tar.gz")).unwrap(),
            b"original bytes"
        );
        assert_eq!(
            std::fs::read(parent.0.join(&second)).unwrap(),
            b"replacement bytes"
        );
        let long = save(
            root.clone(),
            Upload {
                name: format!("{}.bin", "x".repeat(93)),
                bytes: vec![0, 255, 1],
            },
        )
        .unwrap();
        assert_eq!(long.len(), 100);
        assert_eq!(
            std::fs::metadata(parent.0.join(long))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o644
        );
        assert_eq!(suffixes(".hidden.tar.gz"), (".hidden", ".tar.gz"));
        assert_eq!(suffixes("trailing."), ("trailing.", ""));
    }
    #[tokio::test]
    async fn existing_request_grant_survives_clones_and_shutdown_waits() {
        let runtime = Runtime::new();
        let original = Arc::new(tokio::sync::Semaphore::new(4));
        let work = runtime
            .enter(Arc::new(original.clone().acquire_owned().await.unwrap()))
            .unwrap();
        let retained = work.clone();
        drop(work);
        assert_eq!(original.available_permits(), 3);
        assert!(!runtime.shutdown(Duration::ZERO).await);
        assert!(!runtime.ready());
        drop(retained);
        assert!(runtime.shutdown(Duration::from_secs(1)).await);
        assert_eq!(original.available_permits(), 4);
    }
    #[tokio::test]
    async fn multipart_file_and_html_empty_are_distinct_from_json_text() {
        let body="--OwnedBoundary\r\nContent-Disposition: form-data; name=\"name\"\r\n\r\nOwned upload\r\n--OwnedBoundary\r\nContent-Disposition: form-data; name=\"local_file\"; filename=\"owned.bin\"\r\n\r\nexact bytes\r\n--OwnedBoundary--\r\n";
        let mut request = Request::new(axum::body::Body::from(body));
        request.headers_mut().insert(
            "content-type",
            "multipart/form-data; boundary=OwnedBoundary"
                .parse()
                .unwrap(),
        );
        let input = parse(request, false).await.unwrap();
        assert!(input.error.is_empty());
        assert_eq!(input.file.unwrap().bytes, b"exact bytes");
        assert_eq!(input.fields["is_adult"], false);
        assert!(!input.fields.contains_key("local_file"));
    }
}
