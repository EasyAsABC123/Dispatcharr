//! Opt-in bounded diagnostics for the actual owned plugin host only.
//! No messages, arguments, payloads, SQL, caller IDs or private paths are logged.
//! Original Dispatcharr/CPython/dependency authors retain full code credit.
use super::{Error, Receipt};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    OnceLock,
};
const SCAN_BYTES: usize = 64 * 1024;
const LINE_BYTES: usize = 512;
const MAX_LOCATIONS: usize = 6;
const MAX_RECORD_BYTES: usize = 1024;
const MAX_RECORDS: usize = 64;
const MAX_PROCESS_BYTES: usize = MAX_RECORD_BYTES * MAX_RECORDS;
pub(crate) fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var("RUST_PLUGIN_HOST_DIAGNOSTICS").is_ok_and(|v| v == "1"))
}
fn emit(value: Value) {
    static RECORDS: AtomicUsize = AtomicUsize::new(0);
    static BYTES: AtomicUsize = AtomicUsize::new(0);
    if !enabled() {
        return;
    }
    let Ok(raw) = serde_json::to_string(&value) else {
        return;
    };
    let line = format!("dispatcharr_host_diag {raw}");
    let bytes = line.len() + 1;
    if !reserve(&RECORDS, &BYTES, bytes) {
        return;
    }
    use std::io::Write as _;
    // Diagnostic sink failure cannot panic/cancel the physical owner.
    let _ = writeln!(std::io::stderr().lock(), "{line}");
}
fn reserve(records: &AtomicUsize, bytes: &AtomicUsize, length: usize) -> bool {
    if length > MAX_RECORD_BYTES
        || records
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
                (v < MAX_RECORDS).then_some(v + 1)
            })
            .is_err()
    {
        return false;
    }
    bytes
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
            v.checked_add(length)
                .filter(|next| *next <= MAX_PROCESS_BYTES)
        })
        .is_ok()
}
fn error_kind(error: &Error) -> &'static str {
    match error {
        Error::Closed => "Closed",
        Error::Capacity => "Capacity",
        Error::Protocol => "Protocol",
        Error::Child => "Child",
        Error::Callback => "Callback",
        Error::Drain => "Drain",
        Error::Unavailable => "Unavailable",
        Error::Remote(_) => "Remote",
    }
}
pub(crate) fn operation(value: &str) -> &'static str {
    match value {
        "startup" => "startup",
        "discover" => "discover",
        "begin_native_task" => "begin_native_task",
        "append_native_trail" => "append_native_trail",
        "dispatch_plugin_event" => "dispatch_plugin_event",
        "begin_plugin_event" => "begin_plugin_event",
        "render_connect_template" => "render_connect_template",
        "end_plugin_event" => "end_plugin_event",
        "finish_native_task" => "finish_native_task",
        "abort_native_task" => "abort_native_task",
        "shutdown" => "shutdown",
        "run_action" => "run_action",
        "execute_plugin_task" => "execute_plugin_task",
        _ => "other",
    }
}
fn python_class(value: &str) -> Option<&'static str> {
    if value.len() > 128 {
        return None;
    }
    match value.rsplit('.').next()? {
        "ImportError" => Some("ImportError"),
        "ModuleNotFoundError" => Some("ModuleNotFoundError"),
        "RuntimeError" => Some("RuntimeError"),
        "CompatibilityUnavailable" => Some("CompatibilityUnavailable"),
        "ImproperlyConfigured" => Some("ImproperlyConfigured"),
        "AppRegistryNotReady" => Some("AppRegistryNotReady"),
        "OperationalError" => Some("OperationalError"),
        "ProgrammingError" => Some("ProgrammingError"),
        "DatabaseError" => Some("DatabaseError"),
        "InterfaceError" => Some("InterfaceError"),
        "PermissionError" => Some("PermissionError"),
        "FileNotFoundError" => Some("FileNotFoundError"),
        "OSError" => Some("OSError"),
        "ValueError" => Some("ValueError"),
        "TypeError" => Some("TypeError"),
        "AttributeError" => Some("AttributeError"),
        "KeyError" => Some("KeyError"),
        "AssertionError" => Some("AssertionError"),
        "SyntaxError" => Some("SyntaxError"),
        "NameError" => Some("NameError"),
        "ConnectionError" => Some("ConnectionError"),
        "TimeoutError" => Some("TimeoutError"),
        "SystemExit" => Some("SystemExit"),
        "EOFError" => Some("EOFError"),
        "BrokenPipeError" => Some("BrokenPipeError"),
        _ => None,
    }
}
fn framework_file(value: &str) -> Option<&'static str> {
    // Return ONLY literal public aliases; no part of an input path is copied.
    const FILES: &[&str] = &[
        "dispatcharr_native/bootstrap.py",
        "dispatcharr_native/settings.py",
        "dispatcharr_native/builtin_tasks.py",
        "dispatcharr_native/execution_guard.py",
        "dispatcharr_native/wire.py",
        "dispatcharr_native/transports.py",
        "dispatcharr_native/db/base.py",
        "dispatcharr_native/dbapi.py",
        "django/__init__.py",
        "django/apps/registry.py",
        "django/apps/config.py",
        "django/conf/__init__.py",
        "django/db/utils.py",
        "django/db/backends/base/base.py",
        "django/db/backends/postgresql/base.py",
        "django/utils/connection.py",
        "django/core/management/__init__.py",
        "celery/__init__.py",
        "celery/app/base.py",
        "celery/loaders/base.py",
        "celery/fixups/django.py",
        "kombu/connection.py",
        "dispatcharr/settings.py",
        "dispatcharr/celery.py",
        "apps/plugins/apps.py",
        "apps/plugins/loader.py",
    ];
    FILES.iter().copied().find(|name| {
        value == *name
            || value
                .strip_suffix(*name)
                .is_some_and(|prefix| prefix.ends_with('/'))
    })
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Location {
    file: &'static str,
    line: u32,
}
fn location(value: &str) -> Option<Location> {
    let (path, tail) = value.strip_prefix("  File \"")?.split_once("\", line ")?;
    let line = tail.split(',').next()?.parse::<u32>().ok()?;
    if line == 0 || line > 1_000_000 {
        return None;
    }
    Some(Location {
        file: framework_file(path)?,
        line,
    })
}
fn failure_projection(stage: &'static str, method: &str, error: &Error) -> Value {
    let class = match error {
        Error::Remote(value) => value
            .get("actual_class")
            .and_then(Value::as_str)
            .and_then(python_class),
        _ => None,
    };
    json!({"stage":stage,"operation":operation(method),"host_error":error_kind(error),"python_class":class})
}
pub(crate) fn failure(stage: &'static str, method: &str, error: &Error) {
    if enabled() {
        emit(failure_projection(stage, method, error));
    }
}
pub(crate) fn stage(stage: &'static str, category: &'static str) {
    if enabled() {
        emit(json!({"stage":stage,"category":category}));
    }
}
pub(crate) fn retired(receipt: &Receipt) {
    if !enabled() {
        return;
    }
    emit(
        json!({"stage":"physical_retire","actual_exit":receipt.actual_exit,"wait_observed":receipt.wait_observed,"stdout_eof":receipt.stdout_eof,"stderr_eof":receipt.stderr_eof,"stderr_bytes":receipt.stderr_bytes,"forced":receipt.forced,"descendants_absent":receipt.descendants_absent,"callbacks_settled":receipt.callbacks_settled}),
    );
}
/// Scan at most64KiB, retaining at most512 raw line bytes and six literal
/// locations. All later bytes are STILL drained/counted by the existing reader.
/// Observed stderr signatures are untrusted diagnostics, never claim authority.
struct Scanner {
    bytes: usize,
    line: Vec<u8>,
    oversized: bool,
    clipped: bool,
    locations: Vec<Location>,
    first_class: Option<&'static str>,
    last_class: Option<&'static str>,
    guard_signature: Option<&'static str>,
}
impl Scanner {
    fn new() -> Self {
        Self {
            bytes: 0,
            line: Vec::with_capacity(LINE_BYTES),
            oversized: false,
            clipped: false,
            locations: Vec::with_capacity(MAX_LOCATIONS),
            first_class: None,
            last_class: None,
            guard_signature: None,
        }
    }
    fn push(&mut self, raw: &[u8]) {
        for &byte in raw {
            if self.bytes == SCAN_BYTES {
                self.clipped = true;
                break;
            }
            self.bytes += 1;
            if byte == b'\n' {
                self.consume();
            } else if self.line.len() < LINE_BYTES && !self.oversized {
                self.line.push(byte);
            } else {
                self.oversized = true;
            }
        }
    }
    fn consume(&mut self) {
        if !self.oversized {
            if let Ok(value) = std::str::from_utf8(&self.line) {
                let guard = match value {
                    "Original backend task body guard: owned host retired before body" => {
                        Some("backend_body_guard")
                    }
                    "Original HTTP/management entry guard: owned host retired before body" => {
                        Some("http_management_guard")
                    }
                    "Original execution-origin audit capacity: owned host retired" => {
                        Some("execution_audit_capacity")
                    }
                    _ => None,
                };
                if guard.is_some() {
                    self.guard_signature = guard;
                }
                if let Some(found) = location(value) {
                    if self.locations.last() != Some(&found) {
                        if self.locations.len() == MAX_LOCATIONS {
                            self.locations.remove(0);
                        }
                        self.locations.push(found);
                    }
                } else if let Some((name, _)) = value.split_once(':') {
                    if let Some(class) = python_class(name) {
                        self.first_class.get_or_insert(class);
                        self.last_class = Some(class);
                    }
                }
            }
        }
        self.line.clear();
        self.oversized = false;
    }
    fn projection(&self) -> Value {
        json!({"stage":"stderr_signature","known_guard_signature":self.guard_signature,"first_python_class":self.first_class,"last_python_class":self.last_class,"framework_locations":self.locations.iter().map(|v|json!({"file":v.file,"line":v.line})).collect::<Vec<_>>(),"scanned_bytes":self.bytes,"scan_clipped":self.clipped})
    }
}
pub(crate) struct StderrScanner(Option<Scanner>);
impl StderrScanner {
    pub(crate) fn new() -> Self {
        Self(enabled().then(Scanner::new))
    }
    pub(crate) fn push(&mut self, raw: &[u8]) {
        if let Some(scanner) = &mut self.0 {
            scanner.push(raw);
        }
    }
    pub(crate) fn finish(mut self) {
        if let Some(scanner) = &mut self.0 {
            scanner.consume();
            emit(scanner.projection());
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hostile_fragmented_traceback_projects_only_public_alias_line_and_exception_type() {
        let raw=b"Traceback (most recent call last):\n  File \"/private/CANARY-PASSWORD/django/apps/registry.py\", line 138, in CANARY-SQL\n    SELECT CANARY-PASSWORD\n  File \"/private/CANARY-PASSWORD/user-secret.py\", line 7, in hidden\ndjango.core.exceptions.ImproperlyConfigured: CANARY-PASSWORD token=CANARY-TOKEN\n";
        let mut scanner = Scanner::new();
        for chunk in raw.chunks(3) {
            scanner.push(chunk);
        }
        scanner.consume();
        assert_eq!(
            scanner.locations,
            vec![Location {
                file: "django/apps/registry.py",
                line: 138
            }]
        );
        assert_eq!(scanner.last_class, Some("ImproperlyConfigured"));
        let safe = scanner.projection().to_string();
        for secret in ["CANARY", "SELECT", "/private", "user-secret.py"] {
            assert!(!safe.contains(secret));
        }
    }
    #[test]
    fn oversized_lines_and_scan_budget_cannot_smuggle_paths_or_retain_unbounded_stderr() {
        let mut scanner = Scanner::new();
        scanner.push(&vec![b'x'; LINE_BYTES + 1]);
        scanner.push(b"\nValueError: private-message\n");
        assert_eq!(scanner.last_class, Some("ValueError"));
        scanner.push(&vec![b'x'; SCAN_BYTES]);
        scanner.push(b"\nRuntimeError: later\n");
        assert_eq!(scanner.bytes, SCAN_BYTES);
        assert!(scanner.clipped);
        assert!(scanner.line.len() <= LINE_BYTES);
        assert_eq!(scanner.last_class, Some("ValueError"));
        for value in [
            "  File \"/tmp/private.py\", line 12, in x",
            "  File \"/tmp/django/apps/registry.py\", line 0, in x",
            "  File \"/tmp/django/apps/registry.py\", line secret, in x",
        ] {
            assert!(location(value).is_none());
        }
    }
    #[test]
    fn seven_concurrent_hosts_share_one_record_and_byte_budget() {
        let records = AtomicUsize::new(0);
        let bytes = AtomicUsize::new(0);
        assert!(!reserve(&records, &bytes, MAX_RECORD_BYTES + 1));
        assert_eq!(records.load(Ordering::Acquire), 0);
        let totals = std::thread::scope(|scope| {
            let mut hosts = Vec::new();
            for _ in 0..7 {
                hosts.push(scope.spawn(|| {
                    let mut accepted = 0;
                    let mut emitted = 0;
                    for _ in 0..128 {
                        if reserve(&records, &bytes, MAX_RECORD_BYTES) {
                            accepted += 1;
                            emitted += MAX_RECORD_BYTES;
                        }
                    }
                    (accepted, emitted)
                }));
            }
            hosts
                .into_iter()
                .map(|host| host.join().unwrap())
                .fold((0, 0), |(a, b), (c, d)| (a + c, b + d))
        });
        assert_eq!(totals.0, MAX_RECORDS);
        assert_eq!(totals.1, MAX_PROCESS_BYTES);
        assert_eq!(records.load(Ordering::Acquire), MAX_RECORDS);
        assert_eq!(bytes.load(Ordering::Acquire), MAX_PROCESS_BYTES);
    }
    #[test]
    fn only_six_literal_locations_survive_a_long_traceback_window() {
        let mut scanner = Scanner::new();
        for line in 1..=20 {
            scanner.push(
                format!("  File \"/CANARY/django/apps/registry.py\", line {line}, in hidden\n")
                    .as_bytes(),
            );
        }
        assert_eq!(scanner.locations.len(), MAX_LOCATIONS);
        assert_eq!(scanner.locations[0].line, 15);
        assert_eq!(scanner.locations[5].line, 20);
        assert!(!scanner.projection().to_string().contains("CANARY"));
    }
    #[test]
    fn remote_error_messages_and_unknown_operations_are_not_diagnostic_fields() {
        let value = failure_projection(
            "call_result",
            "CANARY-ARGS",
            &Error::Remote(
                json!({"actual_class":"django.core.exceptions.ImproperlyConfigured","message":"CANARY-SQL password=CANARY-SECRET","args":["CANARY-PAYLOAD"]}),
            ),
        );
        assert_eq!(value["python_class"], "ImproperlyConfigured");
        assert_eq!(value["host_error"], "Remote");
        assert!(!value.to_string().contains("CANARY"));
        let mut scanner = Scanner::new();
        scanner.push(b"Original HTTP/management entry guard: owned host retired before body\n");
        assert_eq!(scanner.guard_signature, Some("http_management_guard"));
        assert_eq!(python_class("custom.CANARY-TOKEN"), None);
        assert_eq!(operation("CANARY-ARGS"), "other");
        assert_eq!(
            python_class("django.core.exceptions.ImproperlyConfigured"),
            Some("ImproperlyConfigured")
        );
    }
}
