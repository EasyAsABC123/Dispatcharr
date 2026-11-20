//! Managed application logs. Original Dispatcharr and its collector authors retain all credit.
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use serde_json::Value;
use std::{
    collections::VecDeque,
    env,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd, RawFd},
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

const RECORD: usize = 16 * 1024;
const QUEUE: usize = 2 * 1024 * 1024;
const BATCH: usize = 128 * 1024;
const TICK: Duration = Duration::from_millis(250);
const MAX_KEEP: usize = 50;
const MAX_ENTRIES: usize = 4096;
const CONFIG_LIMIT: u64 = 4096;

#[derive(Clone, Copy)]
struct Settings {
    persist: bool,
    maximum: u64,
    keep: usize,
    zone: Tz,
}

impl Settings {
    fn defaults(zone: Tz) -> Self {
        Self {
            persist: true,
            maximum: 5 * 1024 * 1024,
            keep: 5,
            zone,
        }
    }

    fn normalized(persist: bool, maximum: i64, keep: i64, zone: Tz) -> Self {
        Self {
            persist,
            maximum: maximum.clamp(1, 20) as u64 * 1024 * 1024,
            keep: keep.clamp(2, MAX_KEEP as i64) as usize,
            zone,
        }
    }

    fn from_system(value: &Value, zone: Tz) -> Self {
        let number = |key: &str| {
            value
                .get(key)
                .and_then(|x| x.as_i64().or_else(|| x.as_str()?.trim().parse().ok()))
                .filter(|number| *number != 0)
                .unwrap_or(5)
        };
        let zone = value
            .get("time_zone")
            .and_then(Value::as_str)
            .and_then(|name| name.trim().parse().ok())
            .unwrap_or(zone);
        Self::normalized(
            value.get("log_persist") != Some(&Value::Bool(false)),
            number("log_max_mb"),
            number("log_keep"),
            zone,
        )
    }

    fn from_config(text: &str, boot_zone: Tz) -> Self {
        let (mut persist, mut maximum, mut keep, mut zone) = (true, 5, 5, boot_zone);
        for (key, value) in text.lines().filter_map(|line| line.trim().split_once('=')) {
            match key {
                "persist" => persist = !matches!(value.trim(), "0" | "false" | ""),
                "max_mb" => maximum = value.trim().parse().unwrap_or(5),
                "keep" => keep = value.trim().parse().unwrap_or(5),
                "time_zone" => zone = value.trim().parse().unwrap_or(boot_zone),
                _ => {}
            }
        }
        Self::normalized(persist, maximum, keep, zone)
    }
}

struct Record {
    stamp: DateTime<Utc>,
    zone: Tz,
    body: String,
}

#[derive(Default)]
struct Pending {
    records: VecDeque<Record>,
    bytes: usize,
    dropped: u64,
    waiting_for_role: bool,
}

impl Pending {
    fn suspend_for_role(&mut self) {
        if !self.waiting_for_role {
            *self = Self {
                waiting_for_role: true,
                ..Self::default()
            };
        }
    }

    fn push(&mut self, record: Record) {
        let size = record.body.capacity().max(record.body.len()) + std::mem::size_of::<Record>();
        while self.bytes + size > QUEUE {
            let Some(old) = self.records.pop_front() else {
                return;
            };
            self.bytes -= old.body.capacity() + std::mem::size_of::<Record>();
            self.dropped = self.dropped.saturating_add(1);
        }
        self.bytes += size;
        self.records.push_back(record);
    }

    fn take(&mut self) -> (u64, Vec<Record>) {
        let mut batch = Vec::new();
        let mut bytes = 0;
        while bytes < BATCH {
            let Some(record) = self.records.pop_front() else {
                break;
            };
            let size = record.body.capacity() + std::mem::size_of::<Record>();
            self.bytes -= size;
            bytes += size;
            batch.push(record);
        }
        (std::mem::take(&mut self.dropped), batch)
    }
}

struct State {
    pending: Mutex<Pending>,
    seed: Mutex<Option<Settings>>,
    effective: Mutex<Settings>,
    readers_done: AtomicUsize,
    closing: AtomicBool,
    ready: Arc<AtomicBool>,
}

impl State {
    fn new(zone: Tz) -> Self {
        Self {
            pending: Mutex::new(Pending::default()),
            seed: Mutex::new(None),
            effective: Mutex::new(Settings::defaults(zone)),
            readers_done: AtomicUsize::new(0),
            closing: AtomicBool::new(false),
            ready: Arc::new(AtomicBool::new(false)),
        }
    }
}

struct Capture {
    reader: File,
    writer: File,
    console: Arc<File>,
    target: RawFd,
}

impl Capture {
    fn prepare(target: RawFd) -> io::Result<Self> {
        // SAFETY: fcntl returns a distinct owned descriptor when successful.
        let preserved = unsafe { libc::fcntl(target, libc::F_DUPFD_CLOEXEC, 3) };
        if preserved < 0 {
            return Err(io::Error::last_os_error());
        }
        let console = Arc::new(unsafe { File::from_raw_fd(preserved) });
        let mut pair = [-1; 2];
        // Only the read end is made nonblocking, so producer console writes keep
        // their previous blocking semantics even when stderr and stdout interleave.
        if unsafe { libc::pipe2(pair.as_mut_ptr(), libc::O_CLOEXEC) } < 0 {
            return Err(io::Error::last_os_error());
        }
        let reader = unsafe { File::from_raw_fd(pair[0]) };
        let writer = unsafe { File::from_raw_fd(pair[1]) };
        if unsafe { libc::fcntl(reader.as_raw_fd(), libc::F_SETFL, libc::O_NONBLOCK) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            reader,
            writer,
            console,
            target,
        })
    }
}

fn replace_fd(from: RawFd, to: RawFd) -> io::Result<()> {
    // SAFETY: both descriptors refer to process-owned stdio or private pipes.
    loop {
        if unsafe { libc::dup2(from, to) } >= 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

struct Forwarder {
    target: RawFd,
    console: Arc<File>,
    thread: JoinHandle<()>,
}

pub(crate) struct ManagedLogs {
    state: Arc<State>,
    writer: Option<JoinHandle<()>>,
    readers: Vec<Forwarder>,
    boot_zone: Tz,
}

fn log_directory() -> PathBuf {
    env::var_os("DISPATCHARR_LOG_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/data/logs"))
}

fn role_suffix() -> String {
    let role: String = env::var("DISPATCHARR_LOG_ROLE")
        .unwrap_or_default()
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .take(16)
        .collect();
    if role.is_empty() {
        role
    } else {
        format!("-{role}")
    }
}

impl ManagedLogs {
    pub(crate) fn start() -> Self {
        let boot_zone = env::var("DISPATCHARR_TIME_ZONE")
            .ok()
            .and_then(|name| name.trim().parse().ok())
            .unwrap_or(chrono_tz::UTC);
        let state = Arc::new(State::new(boot_zone));
        let mut logs = Self {
            state,
            writer: None,
            readers: Vec::with_capacity(2),
            boot_zone,
        };
        if logs.install(log_directory(), role_suffix()).is_err() {
            logs.close_captures();
        }
        logs
    }

    fn install(&mut self, directory: PathBuf, role: String) -> io::Result<()> {
        let captures = [Capture::prepare(1)?, Capture::prepare(2)?];
        let mut writers = Vec::with_capacity(2);
        for capture in captures {
            let target = capture.target;
            let console = capture.console.clone();
            let state = self.state.clone();
            let worker = thread::Builder::new()
                .name(format!("dispatcharr-log-fd-{target}"))
                .spawn(move || {
                    forward(capture.reader, &capture.console, target, &state);
                    state.readers_done.fetch_add(1, Ordering::Release);
                })?;
            self.readers.push(Forwarder {
                target,
                console,
                thread: worker,
            });
            writers.push((target, capture.writer));
        }
        for (target, writer) in &writers {
            replace_fd(writer.as_raw_fd(), *target)?;
        }
        let state = self.state.clone();
        let zone = self.boot_zone;
        let writer = thread::Builder::new()
            .name("dispatcharr-log-file".into())
            .spawn(move || {
                write_logs(directory, role, zone, &state);
                state.ready.store(false, Ordering::Release);
            })?;
        self.writer = Some(writer);
        Ok(())
    }

    pub(crate) fn liveness(&self) -> Arc<AtomicBool> {
        self.state.ready.clone()
    }

    pub(crate) fn seed(&self, value: &Value) {
        if let Ok(mut seed) = self.state.seed.lock() {
            *seed = Some(Settings::from_system(value, self.boot_zone));
        }
    }

    fn close_captures(&mut self) {
        for reader in &self.readers {
            let _ = replace_fd(reader.console.as_raw_fd(), reader.target);
        }
        self.state.closing.store(true, Ordering::Release);
    }
}

impl Drop for ManagedLogs {
    fn drop(&mut self) {
        let _ = io::stdout().flush();
        let _ = io::stderr().flush();
        self.close_captures();
        let deadline = Instant::now() + TICK * 3;
        while Instant::now() < deadline {
            if self
                .readers
                .iter()
                .all(|reader| reader.thread.is_finished())
                && self.writer.as_ref().is_none_or(JoinHandle::is_finished)
            {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}

fn forward(mut pipe: File, mut console: &File, target: RawFd, state: &State) {
    let mut scratch = [0; 8192];
    let mut record = Vec::with_capacity(RECORD);
    let mut cut = false;
    loop {
        match pipe.read(&mut scratch) {
            Ok(0) => break,
            Ok(n) => {
                let _ = console.write_all(&scratch[..n]);
                for &byte in &scratch[..n] {
                    if byte == b'\n' {
                        enqueue(state, &record, cut);
                        record.clear();
                        cut = false;
                    } else if record.len() < RECORD {
                        record.push(byte);
                    } else {
                        cut = true;
                    }
                }
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                if state.closing.load(Ordering::Acquire) {
                    break;
                }
                let mut poll = libc::pollfd {
                    fd: pipe.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                };
                // SAFETY: poll is given exactly one initialized pollfd.
                unsafe { libc::poll(&mut poll, 1, 75) };
            }
            Err(_) => break,
        }
    }
    if !record.is_empty() {
        enqueue(state, &record, cut);
    }
    if !state.closing.load(Ordering::Acquire) {
        let _ = replace_fd(console.as_raw_fd(), target);
    }
}

fn enqueue(state: &State, raw: &[u8], cut: bool) {
    let Ok(settings) = state.effective.lock().map(|effective| *effective) else {
        return;
    };
    if settings.persist {
        if let Ok(mut pending) = state.pending.lock() {
            if !pending.waiting_for_role {
                pending.push(Record {
                    stamp: Utc::now(),
                    zone: settings.zone,
                    body: file_body(raw, cut),
                });
            }
        }
    }
}

fn file_body(raw: &[u8], cut: bool) -> String {
    let mut content = String::from_utf8_lossy(raw).into_owned();
    if cut || content.len() > RECORD {
        let mut end = content.len().min(RECORD);
        while !content.is_char_boundary(end) {
            end -= 1;
        }
        content.truncate(end);
        content.push_str(" ... [log_collector truncated this record at 16384 bytes]");
    }
    content
}

fn directory(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY)
        .open(path)
}

fn anchor(directory: &File) -> PathBuf {
    PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd()))
}

fn create_directory(path: &Path) -> io::Result<File> {
    match fs::create_dir(path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error),
    }
    directory(path)
}

fn regular(file: File) -> io::Result<File> {
    if file.metadata()?.is_file() {
        Ok(file)
    } else {
        Err(io::Error::other("managed log entry is not a regular file"))
    }
}

fn append(path: &Path) -> io::Result<File> {
    regular(
        OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o644)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)?,
    )
}

struct Files {
    root: File,
    config: File,
    live: String,
    _lock: File,
    stamp: Option<(u64, i64, i64, u64)>,
    opened: Option<File>,
}

impl Files {
    fn open(path: &Path, role: &str) -> io::Result<Option<Self>> {
        fs::create_dir_all(path)?;
        let root = directory(&fs::canonicalize(path)?)?;
        let config = create_directory(&anchor(&root).join("config"))?;
        let lock = append(&anchor(&config).join(format!("collector{role}.lock")))?;
        // SAFETY: the lifetime of the owned file retains this nonblocking lease.
        if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::WouldBlock {
                return Ok(None);
            }
            return Err(error);
        }
        Ok(Some(Self {
            root,
            config,
            live: format!("dispatcharr.log{role}"),
            _lock: lock,
            stamp: None,
            opened: None,
        }))
    }

    fn acquire(
        root: &Path,
        role: &str,
        zone: Tz,
        settings: &mut Settings,
        state: &State,
    ) -> io::Result<Option<Self>> {
        let Some(mut files) = Self::open(root, role)? else {
            if let Ok(mut pending) = state.pending.lock() {
                pending.suspend_for_role();
            }
            return Ok(None);
        };
        if let Ok(Some(next)) = files.read_settings(zone) {
            *settings = next;
        }
        files.rotate(0, settings.keep)?;
        if let Ok(mut effective) = state.effective.lock() {
            *effective = *settings;
        }
        if let Ok(mut pending) = state.pending.lock() {
            pending.waiting_for_role = false;
        }
        Ok(Some(files))
    }

    fn read_settings(&mut self, zone: Tz) -> io::Result<Option<Settings>> {
        let path = anchor(&self.config).join("collector.conf");
        let file = match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)
        {
            Ok(file) => regular(file)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(self.stamp.take().map(|_| Settings::defaults(zone)));
            }
            Err(error) => return Err(error),
        };
        let metadata = file.metadata()?;
        let stamp = (
            metadata.ino(),
            metadata.mtime(),
            metadata.mtime_nsec(),
            metadata.len(),
        );
        if self.stamp == Some(stamp) {
            return Ok(None);
        }
        let mut input = String::new();
        file.take(CONFIG_LIMIT + 1).read_to_string(&mut input)?;
        if input.len() as u64 > CONFIG_LIMIT {
            return Err(io::Error::other(
                "managed log settings exceed the read bound",
            ));
        }
        self.stamp = Some(stamp);
        Ok(Some(Settings::from_config(&input, zone)))
    }

    fn live_path(&self) -> PathBuf {
        anchor(&self.root).join(&self.live)
    }

    fn archives(&self) -> io::Result<Vec<(usize, PathBuf)>> {
        let prefix = format!("{}.", self.live);
        let mut entries = Vec::new();
        for (index, entry) in fs::read_dir(anchor(&self.root))?.enumerate() {
            if index == MAX_ENTRIES {
                return Err(io::Error::other(
                    "managed log directory exceeds the entry bound",
                ));
            }
            let entry = entry?;
            let name = entry.file_name();
            if let Some(number) = name
                .to_str()
                .and_then(|value| value.strip_prefix(&prefix))
                .filter(|value| value.bytes().all(|byte| byte.is_ascii_digit()))
                .and_then(|value| value.parse::<usize>().ok())
                .filter(|number| *number > 0)
            {
                if entry.file_type()?.is_file() {
                    entries.push((number, entry.path()));
                }
            }
        }
        entries.sort_unstable_by_key(|entry| std::cmp::Reverse(entry.0));
        Ok(entries)
    }

    fn rotate(&mut self, threshold: u64, keep: usize) -> io::Result<bool> {
        let live = self.live_path();
        match fs::symlink_metadata(&live) {
            Ok(metadata) if metadata.is_file() && metadata.len() > threshold => {}
            Ok(_) => return Ok(false),
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error),
        }
        self.opened.take();
        let root = anchor(&self.root);
        for (number, path) in self.archives()? {
            if number >= keep - 1 {
                fs::remove_file(path)?;
            } else {
                fs::rename(path, root.join(format!("{}.{}", self.live, number + 1)))?;
            }
        }
        fs::rename(live, root.join(format!("{}.1", self.live)))?;
        self.opened = Some(append(&self.live_path())?);
        Ok(true)
    }

    fn prune(&self, keep: usize) -> io::Result<()> {
        for (_, path) in self
            .archives()?
            .into_iter()
            .filter(|(number, _)| *number >= keep)
        {
            fs::remove_file(path)?;
        }
        Ok(())
    }

    fn write(&mut self, bytes: &[u8], settings: Settings) -> io::Result<()> {
        let before_append = settings
            .maximum
            .checked_sub(bytes.len() as u64)
            .ok_or_else(|| {
                io::Error::other("managed log batch exceeds the configured file size")
            })?;
        let live = self.live_path();
        if self.opened.is_none() || !live.exists() {
            self.opened = Some(append(&live)?);
        }
        let Some(opened) = &mut self.opened else {
            return Err(io::Error::other("managed log file is unavailable"));
        };
        let current = opened.metadata()?;
        if current.len() <= before_append {
            return opened.write_all(bytes);
        }

        // The crossing batch belongs to the preceding inode. Move that inode
        // before writing so a failed rotation cannot grow the active file.
        let mut crossing = opened.try_clone()?;
        if self.rotate(before_append, settings.keep)? {
            let archive = anchor(&self.root).join(format!("{}.1", self.live));
            let archived = fs::symlink_metadata(archive)?;
            if archived.dev() == current.dev() && archived.ino() == current.ino() {
                return crossing.write_all(bytes);
            }
        }
        Err(io::Error::other(
            "managed log rotation did not archive the live inode",
        ))
    }

    fn write_managed_batch(
        &mut self,
        bytes: &[u8],
        settings: Settings,
        ready: &AtomicBool,
    ) -> bool {
        let written = self.write(bytes, settings).is_ok();
        if !written {
            self.opened.take();
        }
        ready.store(written, Ordering::Release);
        written
    }
}

fn stamp(time: DateTime<Utc>, zone: Tz) -> String {
    let local = time.with_timezone(&zone);
    let offset = local.format("%z").to_string();
    let text = local.format("%Y-%m-%d %H:%M:%S,%3f");
    if offset == "+0000" {
        text.to_string()
    } else {
        format!("{text} {offset}")
    }
}

fn write_logs(root: PathBuf, role: String, zone: Tz, state: &State) {
    let mut files = None;
    let mut settings = Settings::defaults(zone);
    let mut dropped = 0u64;
    loop {
        if files.is_none() {
            if let Ok(Some(opened)) = Files::acquire(&root, &role, zone, &mut settings, state) {
                files = Some(opened);
                state.ready.store(true, Ordering::Release);
            }
        }
        if let Some(opened) = &mut files {
            if let Some(next) = state.seed.lock().ok().and_then(|mut seed| seed.take()) {
                settings = next;
                let _ = opened.prune(settings.keep);
            }
            if let Ok(Some(next)) = opened.read_settings(zone) {
                settings = next;
                let _ = opened.prune(settings.keep);
            }
            if let Ok(mut effective) = state.effective.lock() {
                *effective = settings;
            }
            let (discarded, batch) = state
                .pending
                .lock()
                .map(|mut queue| queue.take())
                .unwrap_or_default();
            if !batch.is_empty() || discarded > 0 {
                dropped = dropped.saturating_add(discarded);
                let mut output = String::new();
                use std::fmt::Write as _;
                if dropped > 0 {
                    let _ = writeln!(output, "{} WARNING dispatcharr.log_collector {dropped} log lines dropped (buffer full - slow disk or log burst)", stamp(Utc::now(), settings.zone));
                }
                for record in &batch {
                    let _ = writeln!(
                        output,
                        "{} INFO stdout {}",
                        stamp(record.stamp, record.zone),
                        record.body
                    );
                }
                if opened.write_managed_batch(output.as_bytes(), settings, &state.ready) {
                    dropped = 0;
                } else {
                    dropped = dropped.saturating_add(batch.len() as u64);
                }
            }
        }
        let closing = state.closing.load(Ordering::Acquire);
        if closing && state.readers_done.load(Ordering::Acquire) == 2 {
            if files.is_none()
                || state
                    .pending
                    .lock()
                    .map_or(true, |queue| queue.records.is_empty())
            {
                break;
            }
            continue;
        }
        thread::sleep(TICK);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    fn fixture() -> PathBuf {
        static SEQUENCE: AtomicUsize = AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "dispatcharr-owned-log-test-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        root
    }

    #[test]
    fn owned_role_reloads_the_shared_atomic_config_and_rotates_only_its_logs() {
        let root = fixture();
        fs::write(root.join("dispatcharr.log"), b"last generation\n").unwrap();
        fs::write(root.join("dispatcharr.log-celery"), b"independent\n").unwrap();
        let mut main = Files::open(&root, "").unwrap().unwrap();
        assert!(Files::open(&root, "").unwrap().is_none());
        let other = Files::open(&root, "-celery").unwrap().unwrap();
        let new_config = root.join("config/collector.next");
        fs::write(
            &new_config,
            b"persist=0\nmax_mb=30\nkeep=1\ntime_zone=America/New_York\n",
        )
        .unwrap();
        fs::rename(new_config, root.join("config/collector.conf")).unwrap();
        let settings = main.read_settings(chrono_tz::UTC).unwrap().unwrap();
        assert!(!settings.persist);
        assert_eq!(settings.maximum, 20 * 1024 * 1024);
        assert_eq!(settings.keep, 2);
        assert_eq!(settings.zone, chrono_tz::America::New_York);
        assert!(main.read_settings(chrono_tz::UTC).unwrap().is_none());
        main.rotate(0, settings.keep).unwrap();
        assert_eq!(
            fs::read(root.join("dispatcharr.log.1")).unwrap(),
            b"last generation\n"
        );
        assert_eq!(
            fs::read(root.join("dispatcharr.log-celery")).unwrap(),
            b"independent\n"
        );
        drop(main);
        drop(other);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rotation_failure_blocks_append_until_owned_file_can_rotate() {
        let root = fixture();
        let live = root.join("dispatcharr.log");
        let archive = root.join("dispatcharr.log.1");
        let mut files = Files::open(&root, "").unwrap().unwrap();
        let settings = Settings::normalized(true, 1, 2, chrono_tz::UTC);
        let fd = append(&live).unwrap();
        fd.set_len(settings.maximum - 1).unwrap();
        fs::create_dir(&archive).unwrap();
        let ready = AtomicBool::new(true);
        assert!(!files.write_managed_batch(b"next\n", settings, &ready));
        assert!(!ready.load(Ordering::Acquire));
        assert_eq!(fs::metadata(&live).unwrap().len(), settings.maximum - 1);

        fs::remove_dir(&archive).unwrap();
        assert!(files.write_managed_batch(b"next\n", settings, &ready));
        assert!(ready.load(Ordering::Acquire));
        let bytes = fs::read(&archive).unwrap();
        assert_eq!(bytes.len() as u64, settings.maximum + 4);
        assert!(bytes.ends_with(b"next\n"));
        assert!(fs::read(&live).unwrap().is_empty());
        assert!(files
            .write(&vec![b'x'; settings.maximum as usize + 1], settings)
            .is_err());
        assert!(fs::read(&live).unwrap().is_empty());
        drop(files);
        drop(fd);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn exact_cap_stays_live_and_crossing_batch_finishes_archive_one() {
        let root = fixture();
        let live = root.join("dispatcharr.log");
        let archive = root.join("dispatcharr.log.1");
        let mut files = Files::open(&root, "").unwrap().unwrap();
        let settings = Settings::normalized(true, 1, 2, chrono_tz::UTC);
        let fd = append(&live).unwrap();
        fd.set_len(settings.maximum - 6).unwrap();
        let inode = fd.metadata().unwrap().ino();

        files.write(b"exact\n", settings).unwrap();
        assert_eq!(fs::metadata(&live).unwrap().len(), settings.maximum);
        assert!(!archive.exists());
        files.write(b"crossing\n", settings).unwrap();
        let before = fs::read(&archive).unwrap();
        assert_eq!(fs::metadata(&archive).unwrap().ino(), inode);
        assert!(before.ends_with(b"exact\ncrossing\n"));
        assert_eq!(before.len() as u64, settings.maximum + 9);
        assert!(fs::read(&live).unwrap().is_empty());
        files.write(b"following\n", settings).unwrap();
        assert_eq!(fs::read(live).unwrap(), b"following\n");
        assert_eq!(fs::read(archive).unwrap(), before);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn crossing_write_failure_leaves_the_rotated_prefix_and_reports_unready() {
        let root = fixture();
        let live = root.join("dispatcharr.log");
        let archive = root.join("dispatcharr.log.1");
        let mut files = Files::open(&root, "").unwrap().unwrap();
        let settings = Settings::normalized(true, 1, 2, chrono_tz::UTC);
        append(&live).unwrap().set_len(settings.maximum).unwrap();
        // A read-only descriptor makes the crossing write fail after rename.
        files.opened = Some(File::open(&live).unwrap());
        let ready = AtomicBool::new(true);

        assert!(!files.write_managed_batch(b"crossing\n", settings, &ready));
        assert!(!ready.load(Ordering::Acquire));
        assert_eq!(fs::metadata(&archive).unwrap().len(), settings.maximum);
        assert!(fs::read(&live).unwrap().is_empty());
        assert!(files.write_managed_batch(b"following\n", settings, &ready));
        assert!(ready.load(Ordering::Acquire));
        assert_eq!(fs::read(&live).unwrap(), b"following\n");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn oversized_batch_leaves_empty_active_and_backup_role_rotates_only_its_archives() {
        let root = fixture();
        let live = root.join("dispatcharr.log-backup");
        let archive = root.join("dispatcharr.log-backup.1");
        let mut files = Files::open(&root, "-backup").unwrap().unwrap();
        let settings = Settings::normalized(true, 1, 3, chrono_tz::UTC);
        let fd = append(&live).unwrap();
        assert!(files
            .write(&vec![b'x'; settings.maximum as usize + 1], settings)
            .is_err());
        assert!(fs::read(&live).unwrap().is_empty());
        assert!(!archive.exists());

        fd.set_len(settings.maximum).unwrap();
        fs::write(&archive, b"previous backup\n").unwrap();
        let second = root.join("dispatcharr.log-backup.2");
        fs::write(&second, b"expired backup\n").unwrap();
        let unrelated = root.join("dispatcharr.log.1");
        fs::write(&unrelated, b"main archive\n").unwrap();
        files.write(b"backup crossing\n", settings).unwrap();
        assert!(fs::read(archive).unwrap().ends_with(b"backup crossing\n"));
        assert_eq!(fs::read(second).unwrap(), b"previous backup\n");
        assert_eq!(fs::read(unrelated).unwrap(), b"main archive\n");
        assert!(fs::read(live).unwrap().is_empty());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn confirmed_same_role_standby_has_no_file_backlog_on_takeover() {
        let root = fixture();
        let zone = chrono_tz::UTC;
        let first = Files::open(&root, "").unwrap().unwrap();
        let other = Files::open(&root, "-celery").unwrap().unwrap();
        let state = State::new(zone);
        let mut settings = Settings::defaults(zone);
        enqueue(&state, b"before role admission", false);
        assert!(Files::acquire(&root, "", zone, &mut settings, &state)
            .unwrap()
            .is_none());
        enqueue(&state, b"during confirmed standby", false);
        {
            let pending = state.pending.lock().unwrap();
            assert!(pending.waiting_for_role);
            assert!(pending.records.is_empty());
            assert_eq!((pending.bytes, pending.dropped), (0, 0));
        }

        drop(first);
        let next = Files::acquire(&root, "", zone, &mut settings, &state)
            .unwrap()
            .unwrap();
        enqueue(&state, b"after takeover", false);
        {
            let mut pending = state.pending.lock().unwrap();
            assert!(!pending.waiting_for_role);
            let (dropped, mut batch) = pending.take();
            assert_eq!(dropped, 0);
            assert_eq!(batch.len(), 1);
            assert_eq!(batch.pop().unwrap().body, "after takeover");
        }
        drop(next);
        drop(other);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn bounded_file_records_keep_valid_utf8_and_charge_queue_storage() {
        let raw = "通".repeat(RECORD).into_bytes();
        let body = file_body(&raw[..RECORD], true);
        assert!(body.ends_with("[log_collector truncated this record at 16384 bytes]"));
        let mut queue = Pending::default();
        for _ in 0..(QUEUE / RECORD + 10) {
            queue.push(Record {
                stamp: Utc::now(),
                zone: chrono_tz::UTC,
                body: body.clone(),
            });
        }
        assert!(queue.bytes <= QUEUE);
        assert!(queue.dropped > 0);
        let (dropped, records) = queue.take();
        assert!(dropped > 0 && !records.is_empty());
        assert!(queue.bytes < QUEUE);
    }

    #[test]
    fn canonical_clock_omits_the_zero_offset() {
        let utc = DateTime::parse_from_rfc3339("2026-01-02T03:04:05.006Z")
            .unwrap()
            .to_utc();
        assert_eq!(stamp(utc, chrono_tz::UTC), "2026-01-02 03:04:05,006");
        assert_eq!(
            stamp(utc, chrono_tz::America::New_York),
            "2026-01-01 22:04:05,006 -0500"
        );
    }
}
