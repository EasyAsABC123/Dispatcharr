//! Opt-in bounded typed send-failure observations for the EPG caller only.
//! Original Dispatcharr and Requests authors retain application credit.
use std::{
    error::Error as _,
    fmt::{self, Write as _},
    io::Write as _,
    sync::atomic::{AtomicUsize, Ordering},
};

const MAX_RECORDS: usize = 8;
const MAX_RECORD_BYTES: usize = 512;
// At most eight attempted writes of 512 bytes: 4 KiB for the whole process.
static RECORDS: AtomicUsize = AtomicUsize::new(0);

struct Record {
    bytes: [u8; MAX_RECORD_BYTES],
    used: usize,
}
impl fmt::Write for Record {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        let remaining = self.bytes.len().saturating_sub(self.used);
        if text.len() > remaining {
            return Err(fmt::Error);
        }
        self.bytes[self.used..self.used + text.len()].copy_from_slice(text.as_bytes());
        self.used += text.len();
        Ok(())
    }
}

fn io_kind(kind: std::io::ErrorKind) -> &'static str {
    use std::io::ErrorKind;
    match kind {
        ErrorKind::ConnectionRefused => "ConnectionRefused",
        ErrorKind::ConnectionReset => "ConnectionReset",
        ErrorKind::ConnectionAborted => "ConnectionAborted",
        ErrorKind::NotConnected => "NotConnected",
        ErrorKind::BrokenPipe => "BrokenPipe",
        ErrorKind::TimedOut => "TimedOut",
        ErrorKind::UnexpectedEof => "UnexpectedEof",
        ErrorKind::InvalidData => "InvalidData",
        ErrorKind::InvalidInput => "InvalidInput",
        ErrorKind::WouldBlock => "WouldBlock",
        ErrorKind::Interrupted => "Interrupted",
        ErrorKind::Unsupported => "Unsupported",
        ErrorKind::PermissionDenied => "PermissionDenied",
        ErrorKind::NotFound => "NotFound",
        _ => "Other",
    }
}

pub(super) fn send_error(error: &reqwest::Error) {
    if std::env::var_os("RUST_EPG_HTTP_DIAGNOSTICS").as_deref() != Some(std::ffi::OsStr::new("1")) {
        return;
    }
    if RECORDS
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
            (count < MAX_RECORDS).then_some(count + 1)
        })
        .is_err()
    {
        return;
    }
    let mut source = error.source();
    let mut depth = 0u8;
    let mut kind = "none";
    while let Some(current) = source {
        if depth == 8 {
            break;
        }
        depth += 1;
        if kind == "none" {
            if let Some(value) = current.downcast_ref::<std::io::Error>() {
                kind = io_kind(value.kind());
            }
        }
        source = current.source();
    }
    let mut record = Record {
        bytes: [0; MAX_RECORD_BYTES],
        used: 0,
    };
    // Only fixed literals, booleans, a u16 status and a whitelisted ErrorKind.
    // Never format an error/source, URL, header, identifier or response body.
    let status = error.status().map(|value| value.as_u16()).unwrap_or(0);
    if writeln!(
        record,
        "epg_http_failure {{\"stage\":\"send\",\"builder\":{},\"redirect\":{},\"status_error\":{},\"status_code\":{},\"timeout\":{},\"connect\":{},\"request\":{},\"body\":{},\"decode\":{},\"io_kind\":\"{}\",\"source_depth\":{}}}",
        error.is_builder(), error.is_redirect(), error.is_status(), status,
        error.is_timeout(), error.is_connect(), error.is_request(),
        error.is_body(), error.is_decode(), kind, depth,
    )
    .is_err()
    {
        return;
    }
    // Diagnostic output is deliberately best effort; failure handling below
    // the caller remains authoritative even when stderr is closed/unwritable.
    let _ = std::io::stderr()
        .lock()
        .write_all(&record.bytes[..record.used]);
}
