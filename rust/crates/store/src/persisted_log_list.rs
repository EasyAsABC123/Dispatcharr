//! Original Dispatcharr's persisted log listing. Original authors retain credit.
use crate::{AuthService, StoreError};
use axum::{
    extract::{ConnectInfo, Path as RoutePath, RawQuery, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Json, Router,
};
use chrono::{DateTime, SecondsFormat, Utc};
use regex::Regex;
use serde::Serialize;
use serde_json::json;
use std::{
    env,
    fs::{self, File, OpenOptions},
    io::Read,
    net::SocketAddr,
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[path = "persisted_log_download.rs"]
mod log_download;
#[path = "persisted_log_tail.rs"]
mod log_tail;

const MAX_ENTRIES: usize = 4096;
const LOG_FILE_READERS: usize = 4;
const MAX_CMDLINE: u64 = 64 * 1024;

#[derive(Serialize)]
struct Entry {
    name: String,
    size: u64,
    modified: String,
}

#[derive(Serialize)]
struct Listing {
    files: Vec<Entry>,
    collector_running: bool,
}

fn family(name: &str) -> bool {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX
        .get_or_init(|| {
            Regex::new(r"\Adispatcharr\.log(-[A-Za-z0-9]{1,16})?(\.\d+)?\z")
                .expect("fixed Source log family")
        })
        .is_match(name)
}

struct LogDirectory(File);

impl LogDirectory {
    fn open(root: &Path) -> Option<Self> {
        let root = fs::canonicalize(root).ok()?;
        OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
            .open(root)
            .ok()
            .map(Self)
    }

    fn anchor(&self) -> PathBuf {
        PathBuf::from(format!("/proc/self/fd/{}", self.0.as_raw_fd()))
    }

    fn contained_path(&self, name: &str) -> Option<PathBuf> {
        let anchor = self.anchor();
        let root = fs::read_link(&anchor).ok()?;
        let target = fs::canonicalize(anchor.join(name)).ok()?;
        if target.parent() != Some(&root) {
            return None;
        }
        // Use the held directory plus the canonical leaf again. A replacement
        // symlink cannot redirect the subsequent no-follow operation.
        Some(anchor.join(target.file_name()?))
    }

    fn open_regular(&self, name: &str) -> Option<File> {
        let path = self.contained_path(name)?;
        if !fs::symlink_metadata(&path).ok()?.is_file() {
            return None;
        }
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)
            .ok()?;
        file.metadata().ok()?.is_file().then_some(file)
    }
}

fn modified(seconds: i64, nanoseconds: i64) -> Option<String> {
    // Source passes stat.st_mtime (a Python float) to datetime.fromtimestamp.
    let stamp = seconds as f64 + nanoseconds as f64 * 1e-9;
    let whole = stamp.floor();
    let micros = ((stamp - whole) * 1e6).round_ties_even() as u32;
    let seconds = whole as i64 + i64::from(micros / 1_000_000);
    let fraction = micros % 1_000_000;
    let value = DateTime::<Utc>::from_timestamp(seconds, fraction * 1000)?;
    let precision = if fraction == 0 {
        SecondsFormat::Secs
    } else {
        SecondsFormat::Micros
    };
    Some(value.to_rfc3339_opts(precision, false))
}

fn collector_pid_file(role: &str) -> String {
    let role: String = role
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .take(16)
        .collect();
    if role.is_empty() {
        "collector.pid".into()
    } else {
        format!("collector-{role}.pid")
    }
}

fn open_pid_file(directory: &LogDirectory, role: &str) -> Option<File> {
    directory.open_regular(&collector_pid_file(role))
}

fn missing_log() -> StoreError {
    StoreError::StatusBody(404, json!({"detail":"Log file not found"}))
}

fn collector_running(directory: &LogDirectory, role: &str) -> bool {
    fn running(directory: &LogDirectory, role: &str) -> Option<bool> {
        let mut text = String::new();
        open_pid_file(directory, role)?
            .take(128)
            .read_to_string(&mut text)
            .ok()?;
        let pid: u32 = text.trim().parse().ok()?;
        let mut command = Vec::new();
        File::open(format!("/proc/{pid}/cmdline"))
            .ok()?
            .take(MAX_CMDLINE)
            .read_to_end(&mut command)
            .ok()?;
        Some(
            [b"dispatcharr.log_collector".as_slice(), b"log_collector.py"]
                .iter()
                .any(|marker| command.windows(marker.len()).any(|part| part == *marker)),
        )
    }
    running(directory, role).unwrap_or_default()
}

fn listing(root: &Path, role: &str, native_running: bool) -> Result<Listing, StoreError> {
    let mut files = Vec::new();
    let Some(directory) = LogDirectory::open(root) else {
        return Ok(Listing {
            files,
            collector_running: false,
        });
    };
    if let Ok(names) = fs::read_dir(directory.anchor()) {
        for (index, entry) in names.enumerate() {
            if index >= MAX_ENTRIES {
                return Err(StoreError::StatusBody(
                    503,
                    json!({"detail": "Persisted log directory exceeds listing capacity."}),
                ));
            }
            let Some(name) = entry
                .ok()
                .and_then(|entry| entry.file_name().into_string().ok())
            else {
                continue;
            };
            if !family(&name) {
                continue;
            }
            let Some(stat) = directory
                .contained_path(&name)
                .and_then(|path| fs::symlink_metadata(path).ok())
                .filter(|stat| stat.is_file())
            else {
                continue;
            };
            let Some(modified) = modified(stat.mtime(), stat.mtime_nsec()) else {
                continue;
            };
            files.push(Entry {
                name,
                size: stat.len(),
                modified,
            });
        }
    }
    files.sort_unstable_by(|a, b| (&b.modified, &b.name).cmp(&(&a.modified, &a.name)));
    Ok(Listing {
        files,
        collector_running: native_running || collector_running(&directory, role),
    })
}

#[derive(Clone)]
struct Logs {
    auth: AuthService,
    file_readers: Arc<Semaphore>,
}

enum LogRead {
    Listing,
    Tail { name: String, query: Option<String> },
    Download(String),
}

pub(super) fn router(auth: AuthService) -> Router {
    Router::new()
        .route("/api/core/logs/", any(handle))
        .route("/api/core/logs/{name}/", any(tail))
        .route("/api/core/logs/{name}/download/", any(download))
        .with_state(Logs {
            auth,
            file_readers: Arc::new(Semaphore::new(LOG_FILE_READERS)),
        })
}

async fn handle(
    State(logs): State<Logs>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    dispatch(logs, peer, method, headers, LogRead::Listing).await
}

async fn tail(
    State(logs): State<Logs>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    RoutePath(name): RoutePath<String>,
    RawQuery(query): RawQuery,
    method: Method,
    headers: HeaderMap,
) -> Response {
    dispatch(logs, peer, method, headers, LogRead::Tail { name, query }).await
}

async fn download(
    State(logs): State<Logs>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    RoutePath(name): RoutePath<String>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    dispatch(logs, peer, method, headers, LogRead::Download(name)).await
}

fn file_slot(readers: Arc<Semaphore>) -> Result<OwnedSemaphorePermit, StoreError> {
    readers.try_acquire_owned().map_err(|_| {
        StoreError::StatusBody(
            503,
            json!({"detail": "Persisted log file reader capacity unavailable."}),
        )
    })
}

async fn dispatch(
    logs: Logs,
    peer: SocketAddr,
    method: Method,
    headers: HeaderMap,
    request: LogRead,
) -> Response {
    let Logs { auth, file_readers } = logs;
    let result = async {
        let permit = auth.task_sql_writer()?;
        let auth = auth.with_request_writer_permit(permit)?;
        auth.account_user(peer, headers, true).await?;
        if method == Method::OPTIONS {
            return Ok(Json(json!({
                "name": match request {
                    LogRead::Listing => "List Log Files",
                    LogRead::Tail { .. } => "Get Log File",
                    LogRead::Download(_) => "Download Log File",
                },
                "description": "",
                "renders": ["application/json", "text/html"],
                "parses": ["application/json", "application/x-www-form-urlencoded", "multipart/form-data"],
            })).into_response());
        }
        if method != Method::GET {
            return Err(StoreError::StatusBody(
                405,
                json!({"detail": format!("Method \"{method}\" not allowed.")}),
            ));
        }
        let root = env::var_os("DISPATCHARR_LOG_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/data/logs"));
        let role = env::var("DISPATCHARR_LOG_ROLE").unwrap_or_default();
        let worker = match request {
            LogRead::Listing => tokio::task::spawn_blocking(move || {
                // Preserve the listing's original bounded filesystem owner.
                let native_running = auth.native_log_liveness.as_ref().is_some_and(|owner| {
                    owner.load(std::sync::atomic::Ordering::Acquire)
                });
                let _owner = auth;
                listing(&root, &role, native_running).map(|value| Json(value).into_response())
            }),
            LogRead::Tail { name, query } => {
                drop(auth);
                let permit = file_slot(file_readers)?;
                let cursor = url::form_urlencoded::parse(query.as_deref().unwrap_or("").as_bytes())
                    .filter(|(key, _)| key == "cursor")
                    .map(|(_, value)| value.into_owned())
                    .last()
                    .unwrap_or_default();
                tokio::task::spawn_blocking(move || log_tail::serve(permit, &root, &name, &cursor))
            }
            LogRead::Download(name) => {
                drop(auth);
                let permit = file_slot(file_readers)?;
                tokio::task::spawn_blocking(move || log_download::serve(permit, &root, &name))
            }
        };
        worker.await.map_err(|_| StoreError::UnhandledRestFault)?
    }
    .await;
    let mut response = result.unwrap_or_else(StoreError::into_response);
    if response.status() != StatusCode::INTERNAL_SERVER_ERROR {
        response
            .headers_mut()
            .insert(header::ALLOW, HeaderValue::from_static("GET, OPTIONS"));
        response
            .headers_mut()
            .insert(header::VARY, HeaderValue::from_static("Accept"));
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_the_original_log_family_and_role_suffix() {
        for name in [
            "dispatcharr.log",
            "dispatcharr.log.2",
            "dispatcharr.log-web31.17",
            "dispatcharr.log.١",
        ] {
            assert!(family(name), "{name}");
        }
        for name in [
            "dispatcharr.log-copy-extra",
            "dispatcharr.log.1\n",
            ".dispatcharr.log",
            "dispatcharr.log-",
            "dispatcharr.log-parent/2",
            "collector.pid",
        ] {
            assert!(!family(name), "{name}");
        }
        assert_eq!(collector_pid_file(""), "collector.pid");
        assert_eq!(
            collector_pid_file("Worker_17-alpha"),
            "collector-Worker17alpha.pid"
        );
    }

    #[test]
    fn pid_probe_refuses_nonregular_files_without_waiting() {
        use std::{ffi::CString, os::unix::ffi::OsStrExt};

        let root = env::temp_dir().join(format!("dispatcharr-log-pid-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root).unwrap();
        let path = root.join("collector.pid");
        let raw = CString::new(path.as_os_str().as_bytes()).unwrap();
        // No writer opens the FIFO. The precheck rejects it without opening;
        // the no-follow nonblocking open and fstat cover a later replacement.
        assert_eq!(unsafe { libc::mkfifo(raw.as_ptr(), 0o600) }, 0);
        let directory = LogDirectory::open(&root).unwrap();
        assert!(open_pid_file(&directory, "").is_none());
        fs::remove_file(&path).unwrap();

        fs::write(root.join("owned.pid"), b"1\n").unwrap();
        std::os::unix::fs::symlink("owned.pid", &path).unwrap();
        assert!(open_pid_file(&directory, "").is_some());
        fs::remove_file(&path).unwrap();
        fs::create_dir(&path).unwrap();
        assert!(open_pid_file(&directory, "").is_none());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn matches_source_microsecond_format_and_rounding() {
        assert_eq!(modified(1, 0).unwrap(), "1970-01-01T00:00:01+00:00");
        assert_eq!(
            modified(1, 999_999_900).unwrap(),
            "1970-01-01T00:00:02+00:00"
        );
        assert_eq!(
            modified(1, 116_700).unwrap(),
            "1970-01-01T00:00:01.000117+00:00"
        );
    }
}
