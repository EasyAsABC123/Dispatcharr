//! Opt-in bounded metadata-only GPG diagnostics. Original Dispatcharr and its
//! dependency authors retain full application/algorithm/license credit.
use serde_json::{Map, Value};
use std::{
    fmt,
    io::{self, Write},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};
use tracing::{
    field::{Field, Visit},
    span::{Attributes, Id, Record},
    Event, Metadata, Subscriber,
};

const TARGET: &str = "dispatcharr::plugin_gpg";
const MAX_EVENT: usize = 4096;
const FIELDS: &[&str] = &[
    "event",
    "pid",
    "start_generation",
    "pid_namespace",
    "role",
    "home_id",
    "stdin_bytes",
    "stdin_complete",
    "stdout_eof",
    "stderr_eof",
    "stdout_bytes",
    "stderr_bytes",
    "wait_observed",
    "wait_exit_code",
    "wait_signal",
    "kill_ok",
    "pipe_eof_observed",
    "remove_ok",
    "home_absent",
    "error_kind",
];

#[derive(Default)]
struct Fields {
    values: Map<String, Value>,
    invalid: bool,
}
impl Fields {
    fn push(&mut self, name: &str, value: Value) {
        if !FIELDS.contains(&name) || value.as_str().is_some_and(|s| s.len() > 256) {
            self.invalid = true;
        } else {
            self.values.insert(name.into(), value);
        }
    }
}
impl Visit for Fields {
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.push(field.name(), value.into());
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.push(field.name(), value.into());
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.push(field.name(), value.into());
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        if !FIELDS.contains(&field.name()) || value.len() > 256 {
            self.invalid = true;
            return;
        }
        self.push(field.name(), value.into());
    }
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if !FIELDS.contains(&field.name()) {
            self.invalid = true;
            return;
        }
        struct Limited(String);
        impl fmt::Write for Limited {
            fn write_str(&mut self, value: &str) -> fmt::Result {
                if self.0.len().saturating_add(value.len()) > 256 {
                    return Err(fmt::Error);
                }
                self.0.push_str(value);
                Ok(())
            }
        }
        let mut text = Limited(String::new());
        if fmt::write(&mut text, format_args!("{value:?}")).is_err() {
            self.invalid = true;
            return;
        }
        self.push(field.name(), text.0.into());
    }
}
struct Bounded(Vec<u8>);
impl Write for Bounded {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.0.len().saturating_add(bytes.len()) >= MAX_EVENT {
            return Err(io::Error::other("GPG trace event capacity"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn render(fields: Fields, epoch_seconds: f64) -> Vec<u8> {
    if fields.invalid || !epoch_seconds.is_finite() {
        return b"{\"target\":\"dispatcharr::plugin_gpg\",\"trace_error\":\"invalid_field_or_clock\"}\n".to_vec();
    }
    let row =
        serde_json::json!({"target":TARGET,"epoch_seconds":epoch_seconds,"fields":fields.values});
    let mut output = Bounded(Vec::with_capacity(MAX_EVENT));
    if serde_json::to_writer(&mut output, &row).is_err() {
        return b"{\"target\":\"dispatcharr::plugin_gpg\",\"trace_error\":\"event_capacity\"}\n"
            .to_vec();
    }
    output.0.push(b'\n');
    output.0
}
struct GpgTrace {
    spans: AtomicU64,
}
impl Subscriber for GpgTrace {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.target() == TARGET && *metadata.level() == tracing::Level::DEBUG
    }
    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(self.spans.fetch_add(1, Ordering::Relaxed).saturating_add(1))
    }
    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
    fn event(&self, event: &Event<'_>) {
        if !self.enabled(event.metadata()) {
            return;
        }
        let mut fields = Fields::default();
        event.record(&mut fields);
        let epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(f64::NAN, |d| d.as_secs_f64());
        let bytes = render(fields, epoch);
        // A single locked write keeps actual events intact across request actors.
        // I/O failure leaves missing evidence; no semantic receipt is fabricated.
        let _ = io::stderr().lock().write_all(&bytes);
    }
}
pub(crate) fn install_from_environment() -> Result<(), &'static str> {
    match std::env::var("RUST_PLUGIN_GPG_TRACE").as_deref() {
        Ok("0") | Err(std::env::VarError::NotPresent) => Ok(()),
        Ok("1") => tracing::subscriber::set_global_default(GpgTrace {
            spans: AtomicU64::new(0),
        })
        .map_err(|_| "GPG diagnostic subscriber initialization failed"),
        _ => Err("RUST_PLUGIN_GPG_TRACE must be 0 or 1"),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unrecognized_fields_never_emit_their_values() {
        let mut fields = Fields::default();
        fields.push(
            "signature",
            Value::String("unretained-fixture-value".into()),
        );
        let raw = render(fields, 1.0);
        let value: Value = serde_json::from_slice(&raw).unwrap();
        assert_eq!(value["trace_error"], "invalid_field_or_clock");
        assert!(!String::from_utf8(raw)
            .unwrap()
            .contains("unretained-fixture-value"));
    }
    #[test]
    fn escaped_fields_cannot_exceed_event_budget() {
        let mut fields = Fields::default();
        for name in FIELDS {
            fields.push(name, Value::String("\u{1}".repeat(256)));
        }
        let raw = render(fields, 1.0);
        assert!(raw.len() <= MAX_EVENT);
        let value: Value = serde_json::from_slice(&raw).unwrap();
        assert_eq!(value["trace_error"], "event_capacity");
    }
}
