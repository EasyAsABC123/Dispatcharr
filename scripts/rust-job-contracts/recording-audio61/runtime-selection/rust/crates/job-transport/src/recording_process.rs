//! PRIVATE UNCOMPILED inert owned-FFmpeg draft. No worker/ACK attachment.
//! Source algorithm/arguments credit: Original Dispatcharr channels/tasks.py.
//! Direct pinned libc dependency provides the confined owned-child SIGINT boundary.
use super::recording_normal::NormalPaths;
use chrono::{DateTime, Utc};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::{
    collections::VecDeque,
    fs,
    io::{self, Read},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use uuid::Uuid;

const STDERR_TAIL: usize = 65_536;
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identity {
    pub pid: u32,
    pub start_ticks: u64,
    pub pgid: u32,
}
#[derive(Clone, Debug)]
pub struct StderrReceipt {
    pub total_bytes: u64,
    pub tail: Vec<u8>,
    pub read_error: Option<String>,
}
#[derive(Clone, Debug)]
pub struct ProcessReceipt {
    pub argv: Vec<String>,
    pub identity: Identity,
    pub started_utc: DateTime<Utc>,
    pub settled_utc: DateTime<Utc>,
    pub code: Option<i32>,
    pub signal: Option<i32>,
    pub stderr: StderrReceipt,
    pub sigint_requested: bool,
    pub forced_kill: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct ProcessRecord {
    pub argv: Vec<String>,
    pub started_utc: DateTime<Utc>,
    pub settled_utc: Option<DateTime<Utc>>,
    pub pid: Option<u32>,
    pub identity: Option<Identity>,
    pub code: Option<i32>,
    pub signal: Option<i32>,
    pub stderr: Option<StderrReceipt>,
    pub reader_error: Option<String>,
    pub command_error: Option<String>,
    pub wait_error: Option<String>,
    pub kill_error: Option<String>,
    pub emergency: bool,
}
impl ProcessRecord {
    pub(crate) fn receipt(&self) -> serde_json::Value {
        serde_json::json!({"argv":self.argv,"started_utc":self.started_utc,"settled_utc":self.settled_utc,
            "pid":self.pid,"identity":self.identity.as_ref().map(|value|serde_json::json!({"pid":value.pid,"start_ticks":value.start_ticks,"pgid":value.pgid})),
            "code":self.code,"signal":self.signal,"stderr":self.stderr.as_ref().map(|value|serde_json::json!({"total_bytes":value.total_bytes,"tail":value.tail,"read_error":value.read_error})),
            "reader_error":self.reader_error,"command_error":self.command_error,"wait_error":self.wait_error,"kill_error":self.kill_error,"emergency":self.emergency})
    }
}
struct Records {
    rows: Vec<ProcessRecord>,
    poisoned: bool,
    pending_owner: Option<Arc<dyn Send + Sync>>,
}
pub(crate) struct Transcript {
    records: Mutex<Records>,
}
impl Transcript {
    pub(crate) fn new() -> Self {
        Self {
            records: Mutex::new(Records {
                rows: Vec::new(),
                poisoned: false,
                pending_owner: None,
            }),
        }
    }
    fn change<T>(&self, operation: impl FnOnce(&mut Records) -> T) -> T {
        match self.records.lock() {
            Ok(mut state) => operation(&mut state),
            Err(error) => {
                let mut state = error.into_inner();
                state.poisoned = true;
                operation(&mut state)
            }
        }
    }
    fn begin(&self, argv: Vec<String>, started_utc: DateTime<Utc>) -> Result<usize, Error> {
        self.change(|state| {
            if state.poisoned || state.rows.len() >= 2 {
                return Err(Error::Unsupported);
            }
            let index = state.rows.len();
            state.rows.push(ProcessRecord {
                argv,
                started_utc,
                settled_utc: None,
                pid: None,
                identity: None,
                code: None,
                signal: None,
                stderr: None,
                reader_error: None,
                command_error: None,
                wait_error: None,
                kill_error: None,
                emergency: false,
            });
            Ok(index)
        })
    }
    fn row(&self, index: usize, operation: impl FnOnce(&mut ProcessRecord)) {
        self.change(|state| {
            if let Some(row) = state.rows.get_mut(index) {
                operation(row)
            } else {
                state.poisoned = true;
            }
        });
    }
    pub(crate) fn snapshot(&self) -> (Vec<ProcessRecord>, bool, bool) {
        self.change(|state| {
            (
                state.rows.clone(),
                state.poisoned,
                state.pending_owner.is_some(),
            )
        })
    }
}
fn diagnostic(error: impl std::fmt::Display) -> String {
    error.to_string().chars().take(2048).collect()
}
#[derive(Debug)]
pub enum Error {
    Unsupported,
    Spawn(io::Error),
    Identity(io::Error),
    Wait(io::Error),
    Signal(io::Error),
    ReaderPanic,
    ReaderMissing,
    CallerShutdown(Box<ProcessReceipt>),
    EarlyExit(Box<ProcessReceipt>),
    UnexpectedExit(Box<ProcessReceipt>),
}

/// Explicit configured Source override, never inferred from the task. Only the
/// actually observed local-origin URL is currently qualified; normal AIO/web
/// defaults and arbitrary providers remain separate domains.
pub(crate) struct ObservedLocalOrigin;
impl ObservedLocalOrigin {
    pub(crate) fn from_explicit_setting(value: &str) -> Result<Self, Error> {
        if value.trim_end_matches('/') == "http://origin:8080" {
            Ok(Self)
        } else {
            Err(Error::Unsupported)
        }
    }
}

fn proc_identity(pid: u32) -> io::Result<Identity> {
    let raw = fs::read_to_string(format!("/proc/{pid}/stat"))?;
    parse_stat(pid, &raw)
}
fn parse_stat(pid: u32, raw: &str) -> io::Result<Identity> {
    let error = || io::Error::new(io::ErrorKind::InvalidData, "owned process identity");
    let (prefix, fields) = raw.rsplit_once(") ").ok_or_else(error)?;
    let actual: u32 = prefix
        .split_once(" (")
        .ok_or_else(error)?
        .0
        .parse()
        .map_err(|_| error())?;
    let fields: Vec<_> = fields.split_ascii_whitespace().collect();
    if actual != pid || fields.len() <= 19 {
        return Err(error());
    }
    let pgid = fields[2].parse::<u32>().map_err(|_| error())?;
    let start_ticks = fields[19].parse::<u64>().map_err(|_| error())?;
    if start_ticks == 0 || pgid != pid {
        return Err(error());
    }
    Ok(Identity {
        pid,
        start_ticks,
        pgid,
    })
}

fn drain(mut pipe: impl Read) -> StderrReceipt {
    let mut tail = VecDeque::with_capacity(STDERR_TAIL);
    let mut total = 0_u64;
    let mut buffer = [0_u8; 8192];
    loop {
        match pipe.read(&mut buffer) {
            Ok(0) => {
                return StderrReceipt {
                    total_bytes: total,
                    tail: tail.into(),
                    read_error: None,
                }
            }
            Ok(count) => {
                total = total.saturating_add(count as u64);
                for byte in &buffer[..count] {
                    if tail.len() == STDERR_TAIL {
                        tail.pop_front();
                    }
                    tail.push_back(*byte);
                }
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => {
                return StderrReceipt {
                    total_bytes: total,
                    tail: tail.into(),
                    read_error: Some(error.to_string()),
                }
            }
        }
    }
}

/// Retains an unreaped Child, not an arbitrary envelope PID. PID cannot be reused
/// while that actual Child is alive/zombie and has not been reaped. Caller must
/// never invoke signal after a successful try_wait()/wait().
fn signal_int(child: &mut Child, identity: &Identity) -> Result<(), Error> {
    if child.id() != identity.pid
        || proc_identity(child.id()).map_err(Error::Identity)? != *identity
    {
        return Err(Error::Unsupported);
    }
    // SAFETY: signal only the actual unreaped owned Child's validated positive
    // PID. No process ID/root/name from a Celery body reaches this boundary.
    let result = unsafe { libc::kill(identity.pid as libc::pid_t, libc::SIGINT) };
    if result == 0 {
        Ok(())
    } else {
        Err(Error::Signal(io::Error::last_os_error()))
    }
}

struct OwnedChild {
    child: Child,
    reader: Option<JoinHandle<StderrReceipt>>,
    reaped: bool,
    transcript: Arc<Transcript>,
    record: usize,
    _caller: Arc<dyn Send + Sync>,
}
impl Drop for OwnedChild {
    fn drop(&mut self) {
        // Emergency unwind containment only, never normal completion evidence.
        // Explicit paths below retain structured wait/reader receipts. Admission
        // cannot escape while this owned OS resource/reader is still settling.
        if self.reaped && self.reader.is_none() {
            return;
        }
        self.transcript.row(self.record, |row| row.emergency = true);
        if !self.reaped {
            if let Err(error) = self.child.kill() {
                self.transcript
                    .row(self.record, |row| row.kill_error = Some(diagnostic(error)));
            }
            match self.child.wait() {
                Ok(status) => {
                    self.reaped = true;
                    self.transcript.row(self.record, |row| {
                        row.code = status.code();
                        row.signal = status.signal();
                    });
                }
                Err(error) => {
                    self.transcript
                        .row(self.record, |row| row.wait_error = Some(diagnostic(error)));
                    self.transcript
                        .change(|state| state.pending_owner = Some(self._caller.clone()));
                }
            }
        }
        if let Some(reader) = self.reader.take() {
            match reader.join() {
                Ok(stderr) => self
                    .transcript
                    .row(self.record, |row| row.stderr = Some(stderr)),
                Err(_) => self.transcript.row(self.record, |row| {
                    row.reader_error = Some("stderr reader panic".into())
                }),
            }
        }
        if self.reaped {
            self.transcript
                .row(self.record, |row| row.settled_utc = Some(Utc::now()));
        }
    }
}
impl OwnedChild {
    fn spawn(
        args: &[String],
        transcript: Arc<Transcript>,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<(Self, DateTime<Utc>, Vec<String>), Error> {
        let mut argv = vec!["ffmpeg".to_owned()];
        argv.extend_from_slice(args);
        let started = Utc::now();
        let record = transcript.begin(argv.clone(), started)?;
        let mut command = Command::new("ffmpeg");
        command
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .process_group(0);
        let child = match command.spawn() {
            Ok(child) => child,
            Err(error) => {
                transcript.row(record, |row| {
                    row.command_error = Some(diagnostic(&error));
                    row.settled_utc = Some(Utc::now());
                });
                return Err(Error::Spawn(error));
            }
        };
        transcript.row(record, |row| row.pid = Some(child.id()));
        let mut owned = Self {
            child,
            reader: None,
            reaped: false,
            transcript,
            record,
            _caller: caller,
        };
        let pipe = match owned.child.stderr.take() {
            Some(pipe) => pipe,
            None => {
                owned.transcript.row(record, |row| {
                    row.reader_error = Some("stderr pipe missing".into())
                });
                return Err(Error::ReaderMissing);
            }
        };
        owned.reader = Some(
            match thread::Builder::new()
                .name("owned-dvr-stderr".into())
                .spawn(move || drain(pipe))
            {
                Ok(reader) => reader,
                Err(error) => {
                    owned
                        .transcript
                        .row(record, |row| row.reader_error = Some(diagnostic(&error)));
                    return Err(Error::Spawn(error));
                }
            },
        );
        Ok((owned, started, argv))
    }
    fn identity(&self) -> Result<Identity, Error> {
        match proc_identity(self.child.id()) {
            Ok(value) => {
                self.transcript
                    .row(self.record, |row| row.identity = Some(value.clone()));
                Ok(value)
            }
            Err(error) => {
                self.transcript.row(self.record, |row| {
                    row.command_error = Some(diagnostic(&error))
                });
                Err(Error::Identity(error))
            }
        }
    }
    fn try_wait(&mut self) -> Result<Option<ExitStatus>, Error> {
        self.child.try_wait().map_err(|error| {
            self.transcript.row(self.record, |row| {
                row.command_error = Some(diagnostic(&error))
            });
            Error::Wait(error)
        })
    }
    fn kill(&mut self) -> Result<(), Error> {
        self.child.kill().map_err(|error| {
            self.transcript.row(self.record, |row| {
                row.command_error = Some(diagnostic(&error))
            });
            Error::Wait(error)
        })
    }
    fn wait(&mut self) -> Result<ExitStatus, Error> {
        self.child.wait().map_err(|error| {
            self.transcript.row(self.record, |row| {
                row.command_error = Some(diagnostic(&error))
            });
            Error::Wait(error)
        })
    }
    fn receipt(
        &mut self,
        status: ExitStatus,
        identity: Identity,
        argv: Vec<String>,
        started_utc: DateTime<Utc>,
        sigint_requested: bool,
        forced_kill: bool,
    ) -> Result<ProcessReceipt, Error> {
        self.reaped = true;
        self.transcript.row(self.record, |row| {
            row.code = status.code();
            row.signal = status.signal();
        });
        let reader = self.reader.take().ok_or(Error::ReaderMissing)?;
        let stderr = match reader.join() {
            Ok(stderr) => stderr,
            Err(_) => {
                self.transcript.row(self.record, |row| {
                    row.reader_error = Some("stderr reader panic".into());
                    row.settled_utc = Some(Utc::now());
                });
                return Err(Error::ReaderPanic);
            }
        };
        self.transcript.row(self.record, |row| {
            row.stderr = Some(stderr.clone());
            row.settled_utc = Some(Utc::now());
        });
        Ok(ProcessReceipt {
            argv,
            identity,
            started_utc,
            settled_utc: Utc::now(),
            code: status.code(),
            signal: status.signal(),
            stderr,
            sigint_requested,
            forced_kill,
        })
    }
}

fn capture_argv(
    channel: Uuid,
    recording: i64,
    paths: &NormalPaths,
    _origin: &ObservedLocalOrigin,
) -> Result<Vec<String>, Error> {
    if !(1..=i64::from(i32::MAX)).contains(&recording) {
        return Err(Error::Unsupported);
    }
    let playlist = paths.playlist.to_str().ok_or(Error::Unsupported)?;
    let segments = paths.segments.to_str().ok_or(Error::Unsupported)?;
    // Only the captured owned local origin is in this first draft's domain.
    let mut args: Vec<String> = [
        "-y",
        "-reconnect",
        "1",
        "-reconnect_streamed",
        "1",
        "-reconnect_delay_max",
        "5",
        "-user_agent",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    args.push(format!("Dispatcharr-DVR/recording-{recording}"));
    args.extend(
        ["-fflags", "+genpts", "-err_detect", "ignore_err", "-i"]
            .into_iter()
            .map(str::to_owned),
    );
    args.push(format!("http://origin:8080/proxy/ts/stream/{channel}"));
    args.extend(
        [
            "-map",
            "0:v?",
            "-map",
            "0:a?",
            "-c",
            "copy",
            "-avoid_negative_ts",
            "make_zero",
            "-f",
            "hls",
            "-hls_time",
            "4",
            "-hls_list_size",
            "0",
            "-hls_playlist_type",
            "event",
            "-hls_flags",
            "append_list+omit_endlist+independent_segments",
            "-start_number",
            "0",
            "-hls_segment_filename",
        ]
        .into_iter()
        .map(str::to_owned),
    );
    args.push(segments.to_owned());
    args.push(playlist.to_owned());
    Ok(args)
}

/// Blocking supervisor must run inside a retained blocking task. Dropping its
/// async observation future does NOT request stop: only explicit worker shutdown
/// sets shutdown. This does not grant due/lease/worker authority.
// Keep each physical owner and explicit cancellation input visible at the call boundary.
#[allow(clippy::too_many_arguments)]
pub(crate) fn capture(
    channel: Uuid,
    recording: i64,
    paths: NormalPaths,
    origin: ObservedLocalOrigin,
    end: DateTime<Utc>,
    shutdown: Arc<AtomicBool>,
    original_owner: Arc<dyn Send + Sync>,
    transcript: Arc<Transcript>,
) -> Result<ProcessReceipt, Error> {
    capture_inner(
        channel,
        recording,
        paths,
        origin,
        end,
        shutdown,
        original_owner,
        transcript,
        Duration::from_secs(30),
    )
}
#[allow(clippy::too_many_arguments)]
pub(crate) fn capture_captured61(
    channel: Uuid,
    recording: i64,
    paths: NormalPaths,
    origin: ObservedLocalOrigin,
    end: DateTime<Utc>,
    shutdown: Arc<AtomicBool>,
    original_owner: Arc<dyn Send + Sync>,
    transcript: Arc<Transcript>,
) -> Result<ProcessReceipt, Error> {
    capture_inner(
        channel,
        recording,
        paths,
        origin,
        end,
        shutdown,
        original_owner,
        transcript,
        Duration::from_secs(61),
    )
}
#[allow(clippy::too_many_arguments)]
fn capture_inner(
    channel: Uuid,
    recording: i64,
    paths: NormalPaths,
    origin: ObservedLocalOrigin,
    end: DateTime<Utc>,
    shutdown: Arc<AtomicBool>,
    original_owner: Arc<dyn Send + Sync>,
    transcript: Arc<Transcript>,
    maximum_remaining: Duration,
) -> Result<ProcessReceipt, Error> {
    let _owner = original_owner.clone();
    let now = Utc::now();
    // Only the observed short normal window; no hours-long claim from Source3.
    let remaining = (end - now).to_std().map_err(|_| Error::Unsupported)?;
    if remaining.is_zero() || remaining > maximum_remaining {
        return Err(Error::Unsupported);
    }
    let args = capture_argv(channel, recording, &paths, &origin)?;
    let deadline = Instant::now() + remaining;
    let (mut owned, started, argv) = OwnedChild::spawn(&args, transcript, original_owner)?;
    let identity = owned.identity()?;
    loop {
        if let Some(status) = owned.try_wait()? {
            let receipt = owned.receipt(status, identity, argv, started, false, false)?;
            return Err(Error::EarlyExit(Box::new(receipt)));
        }
        if shutdown.load(Ordering::Acquire) || Instant::now() >= deadline {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    let shutdown_requested = shutdown.load(Ordering::Acquire);
    // If the process exits between try_wait and kill, an ESRCH is not terminal
    // success evidence. Drop still waits/joins before owner is released.
    if let Err(error) = signal_int(&mut owned.child, &identity) {
        owned.transcript.row(owned.record, |row| {
            row.command_error = Some(format!("{error:?}").chars().take(2048).collect())
        });
        return Err(error);
    }
    let stop_deadline = Instant::now() + Duration::from_secs(10);
    let (status, forced) = loop {
        if let Some(status) = owned.try_wait()? {
            break (status, false);
        }
        if Instant::now() >= stop_deadline {
            owned.kill()?;
            break (owned.wait()?, true);
        }
        thread::sleep(Duration::from_millis(25));
    };
    let receipt = owned.receipt(status, identity, argv, started, true, forced)?;
    if shutdown_requested {
        return Err(Error::CallerShutdown(Box::new(receipt)));
    }
    if receipt.forced_kill || receipt.code != Some(255) || receipt.stderr.read_error.is_some() {
        return Err(Error::UnexpectedExit(Box::new(receipt)));
    }
    Ok(receipt)
}

/// Source's first normal HLS→MKV attempt, without retry/repair/comskip guesses.
/// Caller must have already appended ENDLIST under the same owned filesystem
/// scope; output is not terminal authority until real file validation/SQL/events.
pub(crate) fn remux(
    paths: NormalPaths,
    shutdown: Arc<AtomicBool>,
    original_owner: Arc<dyn Send + Sync>,
    transcript: Arc<Transcript>,
) -> Result<ProcessReceipt, Error> {
    let _owner = original_owner.clone();
    let playlist = paths.playlist.to_str().ok_or(Error::Unsupported)?;
    let output = paths.final_path.to_str().ok_or(Error::Unsupported)?;
    let args: Vec<String> = [
        "-y",
        "-fflags",
        "+genpts+discardcorrupt",
        "-err_detect",
        "ignore_err",
        "-i",
        playlist,
        "-map",
        "0:v?",
        "-map",
        "0:a?",
        "-c",
        "copy",
        "-avoid_negative_ts",
        "make_zero",
        output,
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    let (mut owned, started, argv) = OwnedChild::spawn(&args, transcript, original_owner)?;
    let identity = owned.identity()?;
    let deadline = Instant::now() + Duration::from_secs(30 * 60);
    loop {
        if let Some(status) = owned.try_wait()? {
            let receipt = owned.receipt(status, identity, argv, started, false, false)?;
            return if receipt.code == Some(0) && receipt.stderr.read_error.is_none() {
                Ok(receipt)
            } else {
                Err(Error::UnexpectedExit(Box::new(receipt)))
            };
        }
        let requested = shutdown.load(Ordering::Acquire);
        if requested || Instant::now() >= deadline {
            owned.kill()?;
            let status = owned.wait()?;
            let receipt = owned.receipt(status, identity, argv, started, false, true)?;
            return if requested {
                Err(Error::CallerShutdown(Box::new(receipt)))
            } else {
                Err(Error::UnexpectedExit(Box::new(receipt)))
            };
        }
        thread::sleep(Duration::from_millis(25));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stat_identity_handles_comm_parentheses_and_rejects_wrong_owner() {
        let raw = "42 (ffmpeg (owned)) S 1 42 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 12345 0";
        assert_eq!(
            parse_stat(42, raw).expect("actual identity shape"),
            Identity {
                pid: 42,
                start_ticks: 12345,
                pgid: 42
            }
        );
        assert!(parse_stat(43, raw).is_err());
        assert!(parse_stat(42, &raw.replace("S 1 42 ", "S 1 99 ")).is_err());
        assert!(parse_stat(42, &raw.replace("12345", "0")).is_err());
    }
    #[test]
    fn diagnostic_limit_drains_all_bytes_while_retaining_bounded_tail() {
        let bytes = vec![b'x'; STDERR_TAIL + 8193];
        let receipt = drain(bytes.as_slice());
        assert_eq!(receipt.total_bytes, bytes.len() as u64);
        assert_eq!(receipt.tail.len(), STDERR_TAIL);
        assert!(receipt.read_error.is_none());
    }
    #[test]
    fn origin_must_come_from_exact_observed_explicit_configuration() {
        assert!(ObservedLocalOrigin::from_explicit_setting("http://origin:8080/").is_ok());
        for value in [
            "",
            "http://127.0.0.1:5656",
            "http://provider:8080",
            "http://origin:8080?x=1",
            "http://origin:8080@remote",
        ] {
            assert!(ObservedLocalOrigin::from_explicit_setting(value).is_err());
        }
    }
}

#[cfg(test)]
#[path = "recording_process_owned_tests.rs"]
mod owned_process_controls;
