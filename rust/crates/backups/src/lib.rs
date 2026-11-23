//! Bounded original v2 PostgreSQL backup/archive primitives.
//! Original Dispatcharr authors retain full application credit and AGPL license.
//! Normal HTTP/jobs compose these operations in job-transport; no Source task
//! bodies or migration engine execute here. Disposable restore remains guarded;
//! application restore additionally requires actual current-schema SQL authority.
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Component, Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant, SystemTime},
};
use zip::{write::SimpleFileOptions, ZipArchive, ZipWriter};

#[cfg(target_os = "linux")]
mod guarded_ready;
mod restore_plugin_copy;
mod taskless_archive_rows;
#[cfg(target_os = "linux")]
pub use guarded_ready::{
    GuardedRestoreIdentity, PendingGuardedCommit, ReadyGuardedRestore, VerifiedGuardedHelper,
};

#[derive(Clone)]
pub struct Limits {
    pub database_bytes: u64,
    pub archive_bytes: u64,
    pub central_directory_bytes: u64,
    pub entries: u64,
    pub metadata_bytes: u64,
    pub diagnostic_bytes: usize,
    pub command_timeout: Duration,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            database_bytes: 128 << 30,
            archive_bytes: 128 << 30,
            central_directory_bytes: 8 << 20,
            entries: 128,
            metadata_bytes: 64 << 10,
            diagnostic_bytes: 64 << 10,
            command_timeout: Duration::from_secs(3600),
        }
    }
}
impl Limits {
    fn validate(&self) -> Result<(), Error> {
        if self.database_bytes == 0
            || self.archive_bytes == 0
            || self.metadata_bytes == 0
            || self.metadata_bytes > 1 << 20
            || self.central_directory_bytes == 0
            || self.central_directory_bytes > 16 << 20
            || self.entries == 0
            || self.entries > 1024
            || self.diagnostic_bytes > 1 << 20
            || self.command_timeout.is_zero()
            || self.command_timeout > Duration::from_secs(86400)
            || self.database_bytes > i64::MAX as u64
            || self.archive_bytes > i64::MAX as u64
        {
            return Err(Error::Capacity);
        }
        Ok(())
    }
}
#[derive(Clone)]
pub struct PgConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub database: String,
    pub password: Option<String>,
    /// Allowed keys: sslmode, sslrootcert, sslcert, sslkey. No secret Debug output.
    pub options: BTreeMap<String, String>,
}
#[derive(Clone)]
pub struct Runtime {
    pub pg_dump: PathBuf,
    pub pg_restore: PathBuf,
    pub psql: PathBuf,
}
#[derive(Debug)]
pub enum Error {
    Io,
    Zip,
    MissingMetadata,
    Metadata,
    Capacity,
    Busy,
    Cancelled,
    Timeout,
    Command {
        stage: &'static str,
        code: Option<i32>,
    },
    WrongDatabaseType,
    SqliteDatabaseTypeMismatch,
    UnsafeMember,
    FixtureAuthorization,
    WorkerPanic,
}
impl From<io::Error> for Error {
    fn from(_: io::Error) -> Self {
        Self::Io
    }
}
impl From<zip::result::ZipError> for Error {
    fn from(_: zip::result::ZipError) -> Self {
        Self::Zip
    }
}
#[derive(Serialize, Deserialize)]
struct Metadata {
    format: String,
    version: u32,
    database_type: String,
    database_file: String,
    created_at: String,
}
/// All clocks are supplied by a future Source-compatible timezone resolver.
/// The archive component does not pretend host localtime reproduces pytz.
pub trait SourceClock {
    fn filename_timestamp(&self) -> Result<String, Error>;
    fn created_at_utc(&self) -> Result<String, Error>;
    fn zip_timestamp(&self, time: SystemTime) -> Result<zip::DateTime, Error>;
}
#[derive(Clone, Default)]
pub struct Cancellation(Arc<AtomicBool>);
impl Cancellation {
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release)
    }
    fn check(&self) -> Result<(), Error> {
        if self.0.load(Ordering::Acquire) {
            Err(Error::Cancelled)
        } else {
            Ok(())
        }
    }
}
/// One owned worker per service. Dropping its join handle does not cancel it;
/// explicit cancellation does. Durable job persistence is a separate component.
#[derive(Clone, Default)]
pub struct Admission(Arc<AtomicBool>);
struct Permit(Arc<AtomicBool>);
impl Drop for Permit {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release)
    }
}
impl Admission {
    pub fn submit<T: Send + 'static>(
        &self,
        job: impl FnOnce() -> Result<T, Error> + Send + 'static,
    ) -> Result<thread::JoinHandle<Result<T, Error>>, Error> {
        self.0
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| Error::Busy)?;
        let permit = Permit(self.0.clone());
        thread::Builder::new()
            .name("dispatcharr-backup".into())
            .spawn(move || {
                let _permit = permit;
                job()
            })
            .map_err(|_| Error::Io)
    }
}
/// A draft restore may only destroy its explicitly owned disposable database.
/// Promotion to a production restore authority requires separate API/job gates.
pub struct DisposableRestore {
    database: String,
}
impl DisposableRestore {
    pub fn authorize(database: &str) -> Result<Self, Error> {
        if !database.starts_with("rust_probe_backup_")
            || database.len() > 63
            || !database
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        {
            return Err(Error::FixtureAuthorization);
        }
        Ok(Self {
            database: database.into(),
        })
    }
}
struct OwnedDirectory(PathBuf);
impl OwnedDirectory {
    fn new(root: &Path) -> Result<Self, Error> {
        fs::create_dir_all(root)?;
        for _ in 0..8 {
            let path = root.join(format!("dispatcharr-backup-{}", uuid::Uuid::new_v4()));
            let mut directory = fs::DirBuilder::new();
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                directory.mode(0o700);
            }
            match directory.create(&path) {
                Ok(()) => return Ok(Self(path)),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(_) => return Err(Error::Io),
            }
        }
        Err(Error::Io)
    }
}
impl Drop for OwnedDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// Diagnostics are retained privately and bounded; public summaries expose only
/// stage/status/byte counts. Never serialize stderr or DB config into evidence.
struct Diagnostic {
    prefix: Vec<u8>,
    bytes: u64,
    critical: bool,
    utf8_valid: bool,
}
fn drain(mut reader: impl Read, limit: usize) -> io::Result<Diagnostic> {
    let mut output = Diagnostic {
        prefix: Vec::with_capacity(limit.min(8192)),
        bytes: 0,
        critical: false,
        utf8_valid: true,
    };
    let mut buffer = [0u8; 8192];
    let mut search_tail = Vec::new();
    let mut utf8_tail = Vec::new();
    loop {
        let count = match reader.read(&mut buffer) {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            other => other?,
        };
        if count == 0 {
            break;
        }
        let chunk = &buffer[..count];
        output.bytes = output.bytes.saturating_add(count as u64);
        output.prefix.extend_from_slice(
            &chunk[..chunk.len().min(limit.saturating_sub(output.prefix.len()))],
        );
        search_tail.extend(chunk.iter().map(u8::to_ascii_lowercase));
        output.critical |= search_tail.windows(5).any(|v| v == b"fatal")
            || search_tail.windows(17).any(|v| v == b"could not connect");
        if search_tail.len() > 16 {
            search_tail.drain(..search_tail.len() - 16);
        }
        if output.utf8_valid {
            utf8_tail.extend_from_slice(chunk);
            match std::str::from_utf8(&utf8_tail) {
                Ok(_) => utf8_tail.clear(),
                Err(error) if error.error_len().is_none() => {
                    utf8_tail.drain(..error.valid_up_to());
                }
                Err(_) => {
                    output.utf8_valid = false;
                    utf8_tail.clear();
                }
            }
        }
    }
    output.utf8_valid &= utf8_tail.is_empty();
    Ok(output)
}
struct ChildOwner {
    child: Child,
    cleaned: bool,
}
impl ChildOwner {
    fn kill_and_wait(&mut self) {
        #[cfg(unix)]
        unsafe {
            libc::kill(-(self.child.id() as i32), libc::SIGKILL);
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.cleaned = true;
    }
}
impl Drop for ChildOwner {
    fn drop(&mut self) {
        if !self.cleaned {
            self.kill_and_wait()
        }
    }
}
struct CommandOutcome {
    status: ExitStatus,
    stderr: Diagnostic,
}
fn connection_args(config: &PgConfig) -> Vec<String> {
    vec![
        "-h".into(),
        config.host.clone(),
        "-p".into(),
        config.port.to_string(),
        "-U".into(),
        config.user.clone(),
        "-d".into(),
        config.database.clone(),
    ]
}
fn prepare_command(binary: &Path, config: &PgConfig, args: &[String], file_limit: u64) -> Command {
    let mut command = Command::new(binary);
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for key in [
        "PGPASSWORD",
        "PGSSLMODE",
        "PGSSLROOTCERT",
        "PGSSLCERT",
        "PGSSLKEY",
    ] {
        command.env_remove(key);
    }
    if let Some(password) = config.password.as_ref().filter(|v| !v.is_empty()) {
        command.env("PGPASSWORD", password);
    }
    for (key, env) in [
        ("sslmode", "PGSSLMODE"),
        ("sslrootcert", "PGSSLROOTCERT"),
        ("sslcert", "PGSSLCERT"),
        ("sslkey", "PGSSLKEY"),
    ] {
        if let Some(value) = config.options.get(key).filter(|v| !v.is_empty()) {
            command.env(env, value);
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
        // Only async-signal-safe libc call in the child before exec. Child file
        // growth is hard-limited rather than checked after a large write.
        unsafe {
            command.pre_exec(move || {
                let limit = libc::rlimit {
                    rlim_cur: file_limit as libc::rlim_t,
                    rlim_max: file_limit as libc::rlim_t,
                };
                if libc::setrlimit(libc::RLIMIT_FSIZE, &limit) != 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }
    command
}
fn run(
    mut command: Command,
    limits: &Limits,
    cancel: &Cancellation,
) -> Result<CommandOutcome, Error> {
    cancel.check()?;
    let mut child = ChildOwner {
        child: command.spawn()?,
        cleaned: false,
    };
    let stdout = child.child.stdout.take().ok_or(Error::Io)?;
    let stderr = child.child.stderr.take().ok_or(Error::Io)?;
    let cap = limits.diagnostic_bytes;
    let out = thread::spawn(move || drain(stdout, cap));
    let err = thread::spawn(move || drain(stderr, cap));
    let deadline = Instant::now() + limits.command_timeout;
    let status = loop {
        if let Err(error) = cancel.check() {
            child.kill_and_wait();
            let _ = out.join();
            let _ = err.join();
            return Err(error);
        }
        if Instant::now() >= deadline {
            child.kill_and_wait();
            let _ = out.join();
            let _ = err.join();
            return Err(Error::Timeout);
        }
        match child.child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => thread::sleep(Duration::from_millis(20)),
            Err(_) => {
                child.kill_and_wait();
                let _ = out.join();
                let _ = err.join();
                return Err(Error::Io);
            }
        }
    };
    // Reap/kill the owned process group before joining pipe readers, including
    // any descendant that inherited a pipe after the leader exited.
    child.kill_and_wait();
    let stdout = out.join().map_err(|_| Error::WorkerPanic)??;
    let stderr = err.join().map_err(|_| Error::WorkerPanic)??;
    if !stdout.utf8_valid || !stderr.utf8_valid {
        return Err(Error::Io);
    }
    Ok(CommandOutcome { status, stderr })
}
fn checked(outcome: CommandOutcome, stage: &'static str) -> Result<(), Error> {
    if outcome.status.success() {
        Ok(())
    } else {
        Err(Error::Command {
            stage,
            code: outcome.status.code(),
        })
    }
}
struct LimitedFile {
    file: File,
    position: u64,
    limit: u64,
}
impl Write for LimitedFile {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() as u64 > self.limit.saturating_sub(self.position) {
            return Err(io::Error::other("archive capacity"));
        }
        let count = self.file.write(bytes)?;
        self.position += count as u64;
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}
impl Seek for LimitedFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let offset = self.file.seek(pos)?;
        if offset > self.limit {
            return Err(io::Error::other("archive capacity"));
        }
        self.position = offset;
        Ok(offset)
    }
}
fn copy(
    mut reader: impl Read,
    mut writer: impl Write,
    limit: u64,
    cancel: &Cancellation,
) -> Result<u64, Error> {
    let mut buffer = [0u8; 8192];
    let mut bytes = 0_u64;
    loop {
        cancel.check()?;
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            return Ok(bytes);
        }
        bytes = bytes.checked_add(count as u64).ok_or(Error::Capacity)?;
        if bytes > limit {
            return Err(Error::Capacity);
        }
        writer.write_all(&buffer[..count])?;
    }
}
/// Successful archives remain DB-only v2; compressed ZIP bytes need not match
/// Python's deflater. Current bounded/malformed input limits are explicit deltas.
pub fn create_backup(
    config: &PgConfig,
    runtime: &Runtime,
    backup_root: &Path,
    scratch_root: &Path,
    clock: &dyn SourceClock,
    limits: &Limits,
    cancel: &Cancellation,
) -> Result<PathBuf, Error> {
    limits.validate()?;
    cancel.check()?;
    let timestamp = clock.filename_timestamp()?;
    if timestamp.len() != 19 || !timestamp.bytes().all(|v| v.is_ascii_digit() || v == b'.') {
        return Err(Error::Metadata);
    }
    fs::create_dir_all(backup_root)?;
    let scratch = OwnedDirectory::new(scratch_root)?;
    let dump = scratch.0.join("database.dump");
    let mut args = connection_args(config);
    args.extend([
        "-Fc".into(),
        "-v".into(),
        "-f".into(),
        dump.to_string_lossy().into_owned(),
    ]);
    checked(
        run(
            prepare_command(&runtime.pg_dump, config, &args, limits.database_bytes),
            limits,
            cancel,
        )?,
        "pg_dump",
    )?;
    if fs::metadata(&dump)?.len() > limits.database_bytes {
        return Err(Error::Capacity);
    }
    let destination = backup_root.join(format!("dispatcharr-backup-{timestamp}.zip"));
    // Source opens directly with truncate semantics; no atomic rename invented.
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&destination)?;
    let mut zip = ZipWriter::new(LimitedFile {
        file,
        position: 0,
        limit: limits.archive_bytes,
    });
    let options = SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated)
        .large_file(true)
        .last_modified_time(clock.zip_timestamp(fs::metadata(&dump)?.modified()?)?);
    zip.start_file("database.dump", options)?;
    copy(File::open(dump)?, &mut zip, limits.database_bytes, cancel)?;
    let created_at = clock.created_at_utc()?;
    if created_at.len() > 128 {
        return Err(Error::Metadata);
    }
    let metadata = Metadata {
        format: "dispatcharr-backup".into(),
        version: 2,
        database_type: "postgresql".into(),
        database_file: "database.dump".into(),
        created_at,
    };
    let data = serde_json::to_vec_pretty(&metadata).map_err(|_| Error::Metadata)?;
    if data.len() as u64 > limits.metadata_bytes {
        return Err(Error::Capacity);
    }
    zip.start_file(
        "metadata.json",
        options.last_modified_time(clock.zip_timestamp(SystemTime::now())?),
    )?;
    zip.write_all(&data)?;
    zip.finish()?.file.sync_all()?;
    Ok(destination)
}
/// Check the final ZIP directory before a library can allocate from it.
/// ZIP64 supported; split archives and oversized metadata deliberately reject.
fn preflight(file: &mut File, limits: &Limits) -> Result<(), Error> {
    let size = file.metadata()?.len();
    if size > limits.archive_bytes {
        return Err(Error::Capacity);
    }
    let tail_len = size.min(65557) as usize;
    file.seek(SeekFrom::End(-(tail_len as i64)))?;
    let mut tail = vec![0; tail_len];
    file.read_exact(&mut tail)?;
    let index = (0..tail.len().saturating_sub(21))
        .rev()
        .find(|&i| {
            tail[i..].starts_with(b"PK\x05\x06")
                && i + 22 + u16::from_le_bytes([tail[i + 20], tail[i + 21]]) as usize == tail.len()
        })
        .ok_or(Error::Zip)?;
    let end = &tail[index..];
    let u16_at = |at| u16::from_le_bytes([end[at], end[at + 1]]);
    let u32_at = |at| u32::from_le_bytes(end[at..at + 4].try_into().unwrap());
    if u16_at(4) != 0 || u16_at(6) != 0 || u16_at(8) != u16_at(10) {
        return Err(Error::Zip);
    }
    let (mut entries, mut directory_size, mut offset) = (
        u64::from(u16_at(10)),
        u64::from(u32_at(12)),
        u64::from(u32_at(16)),
    );
    if entries == 65535 || directory_size == u32::MAX as u64 || offset == u32::MAX as u64 {
        let absolute = size - tail_len as u64 + index as u64;
        if absolute < 20 {
            return Err(Error::Zip);
        }
        file.seek(SeekFrom::Start(absolute - 20))?;
        let mut locator = [0; 20];
        file.read_exact(&mut locator)?;
        if &locator[..4] != b"PK\x06\x07"
            || locator[4..8] != [0; 4]
            || u32::from_le_bytes(locator[16..20].try_into().unwrap()) != 1
        {
            return Err(Error::Zip);
        }
        file.seek(SeekFrom::Start(u64::from_le_bytes(
            locator[8..16].try_into().unwrap(),
        )))?;
        let mut record = [0; 56];
        file.read_exact(&mut record)?;
        if &record[..4] != b"PK\x06\x06"
            || record[16..24] != [0; 8]
            || record[24..32] != record[32..40]
        {
            return Err(Error::Zip);
        }
        entries = u64::from_le_bytes(record[32..40].try_into().unwrap());
        directory_size = u64::from_le_bytes(record[40..48].try_into().unwrap());
        offset = u64::from_le_bytes(record[48..56].try_into().unwrap());
    }
    if entries > limits.entries
        || directory_size > limits.central_directory_bytes
        || offset
            .checked_add(directory_size)
            .is_none_or(|end| end > size)
    {
        return Err(Error::Capacity);
    }
    file.rewind()?;
    Ok(())
}
/// Compiled application port backed by an actual current-database SQL fence.
/// Implementations must derive this from the admitted Main session, not HTTP IDs.
pub trait CurrentSchemaRestoreAuthority: Send + Sync {
    fn current_database(&self) -> &str;
    fn server_started_micros(&self) -> i64;
    fn expected_migrations(&self) -> &std::collections::BTreeSet<(String, String)>;
    /// Called before the FIRST possibly destructive command submission.
    /// Default preserves existing unobserved callers. An observer must retain
    /// this per-execution fact through failures; it is not proof of SQL success.
    fn before_destructive_submission(&self) {}
}
/// Application restore is separate from the unchanged disposable-only helper.
/// Only the currently pinned migration graph is supported; inspection occurs
/// BEFORE schema deletion and migration/post_migrate reconciliation is required
/// by the real worker after this physical archive/PG command phase completes.
pub fn restore_current_schema(
    config: &PgConfig,
    runtime: &Runtime,
    archive: &Path,
    scratch_root: &Path,
    limits: &Limits,
    cancel: &Cancellation,
    authority: &dyn CurrentSchemaRestoreAuthority,
) -> Result<(), Error> {
    if authority.current_database() != config.database {
        return Err(Error::FixtureAuthorization);
    }
    restore_archive(
        config,
        runtime,
        archive,
        scratch_root,
        limits,
        cancel,
        Some(authority),
    )
}
pub fn restore_fixture(
    config: &PgConfig,
    runtime: &Runtime,
    archive: &Path,
    scratch_root: &Path,
    limits: &Limits,
    cancel: &Cancellation,
    authority: &DisposableRestore,
) -> Result<(), Error> {
    limits.validate()?;
    cancel.check()?;
    if authority.database != config.database {
        return Err(Error::FixtureAuthorization);
    }
    restore_archive(config, runtime, archive, scratch_root, limits, cancel, None)
}
struct PreparedDump {
    scratch: OwnedDirectory,
    dump: PathBuf,
}
fn stage_restore_dump(
    archive: &Path,
    scratch_root: &Path,
    limits: &Limits,
    cancel: &Cancellation,
) -> Result<PreparedDump, Error> {
    limits.validate()?;
    cancel.check()?;
    let scratch = OwnedDirectory::new(scratch_root)?;
    let mut file = File::open(archive)?;
    preflight(&mut file, limits)?;
    let mut zip = ZipArchive::new(file)?;
    let mut metadata = Vec::new();
    let metadata_member = zip.by_name("metadata.json").map_err(|error| match error {
        zip::result::ZipError::FileNotFound => Error::MissingMetadata,
        other => Error::from(other),
    })?;
    copy(
        metadata_member,
        &mut metadata,
        limits.metadata_bytes,
        cancel,
    )?;
    // Preserve Source's missing database_type/database_file defaults, and do
    // not suddenly require metadata.format/version on legacy valid archives.
    let value: serde_json::Value =
        serde_json::from_slice(&metadata).map_err(|_| Error::Metadata)?;
    let object = value.as_object().ok_or(Error::Metadata)?;
    let member = object
        .get("database_file")
        .map_or(Some("database.dump"), serde_json::Value::as_str)
        .ok_or(Error::Metadata)?;
    if member.is_empty()
        || Path::new(member)
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err(Error::UnsafeMember);
    }
    let dump = scratch.0.join("database.dump");
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&dump)?;
    copy(
        zip.by_name(member)?,
        &mut output,
        limits.database_bytes,
        cancel,
    )?;
    output.sync_all()?;
    let kind = object
        .get("database_type")
        .map_or(Some("postgresql"), serde_json::Value::as_str)
        .ok_or(Error::Metadata)?;
    match kind {
        "postgresql" => {}
        "sqlite" => return Err(Error::SqliteDatabaseTypeMismatch),
        _ => return Err(Error::WrongDatabaseType),
    }
    Ok(PreparedDump { scratch, dump })
}

// The callback precedes even a spawn/IO failure: once invoked the caller must
// conservatively treat physical submission as possible, never infer no writes.
fn destructive_submission<T>(
    authority: Option<&dyn CurrentSchemaRestoreAuthority>,
    submit: impl FnOnce() -> Result<T, Error>,
) -> Result<T, Error> {
    if let Some(authority) = authority {
        authority.before_destructive_submission();
    }
    submit()
}
fn restore_archive(
    config: &PgConfig,
    runtime: &Runtime,
    archive: &Path,
    scratch_root: &Path,
    limits: &Limits,
    cancel: &Cancellation,
    authority: Option<&dyn CurrentSchemaRestoreAuthority>,
) -> Result<(), Error> {
    let PreparedDump { scratch, dump } = stage_restore_dump(archive, scratch_root, limits, cancel)?;
    if let Some(authority) = authority {
        inspect_current_migrations(
            config,
            runtime,
            &scratch,
            InspectedDump::Path(&dump),
            limits,
            cancel,
            authority.expected_migrations(),
        )?;
        let plugin_sql = scratch.0.join("restore-plugin-policy.sql");
        let args = vec![
            "--data-only".into(),
            "--schema=public".into(),
            "--table=plugins_pluginconfig".into(),
            "--file".into(),
            plugin_sql.to_string_lossy().into_owned(),
            dump.to_string_lossy().into_owned(),
        ];
        checked(
            run(
                prepare_command(&runtime.pg_restore, config, &args, 1 << 20),
                limits,
                cancel,
            )?,
            "inspect restore plugin policy",
        )?;
        let mut bytes = Vec::new();
        File::open(plugin_sql)?
            .take((1 << 20) + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > 1 << 20 {
            return Err(Error::Metadata);
        }
        restore_plugin_copy::require_disabled(&bytes).map_err(|_| Error::Metadata)?;
        // Prove the configured PG tool reaches the SAME observed Main server
        // lifetime/database, not merely a different server with an equal DB name.
        let identity_file = scratch.0.join("restore-target.json");
        let mut args = connection_args(config);
        args.extend(["-A".into(),"-t".into(),"--output".into(),identity_file.to_string_lossy().into_owned(),"-c".into(),"SELECT json_build_object('database',current_database(),'postmaster_micros',(extract(epoch FROM pg_postmaster_start_time())*1000000)::bigint)::text".into()]);
        checked(
            run(
                prepare_command(&runtime.psql, config, &args, 65536),
                limits,
                cancel,
            )?,
            "verify restore target",
        )?;
        let mut raw = Vec::new();
        File::open(identity_file)?
            .take(65537)
            .read_to_end(&mut raw)?;
        if raw.len() > 65536 {
            return Err(Error::FixtureAuthorization);
        }
        let identity: serde_json::Value =
            serde_json::from_slice(&raw).map_err(|_| Error::FixtureAuthorization)?;
        if identity["database"].as_str() != Some(authority.current_database())
            || identity["postmaster_micros"].as_i64() != Some(authority.server_started_micros())
        {
            return Err(Error::FixtureAuthorization);
        }
    }
    let mut args = connection_args(config);
    args.extend(["-c".into(),"DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public; GRANT ALL ON SCHEMA public TO public;".into()]);
    checked(
        destructive_submission(authority, || {
            run(
                prepare_command(&runtime.psql, config, &args, limits.database_bytes),
                limits,
                cancel,
            )
        })?,
        "psql schema cleanup",
    )?;
    let mut args = vec!["--no-owner".into()];
    args.extend(connection_args(config));
    args.extend(["-v".into(), dump.to_string_lossy().into_owned()]);
    let restored = run(
        prepare_command(&runtime.pg_restore, config, &args, limits.database_bytes),
        limits,
        cancel,
    )?;
    if !restored.status.success() && restored.stderr.critical {
        return Err(Error::Command {
            stage: "pg_restore",
            code: restored.status.code(),
        });
    }
    // Other nonzero pg_restore exits are Source-compatible warnings, not proof
    // of a complete DB restore. Real copied-data manifests remain mandatory.
    Ok(())
}

fn inspect_current_migrations(
    config: &PgConfig,
    runtime: &Runtime,
    scratch: &OwnedDirectory,
    dump: InspectedDump<'_>,
    limits: &Limits,
    cancel: &Cancellation,
    expected: &std::collections::BTreeSet<(String, String)>,
) -> Result<(), Error> {
    let bytes = inspect_copy(
        config,
        runtime,
        scratch,
        dump,
        limits,
        cancel,
        "django_migrations",
        "inspect restore migration graph",
    )?;
    if &parse_migration_copy(&bytes, expected.len())? != expected {
        return Err(Error::Metadata);
    }
    Ok(())
}

enum InspectedDump<'a> {
    Path(&'a Path),
    #[cfg(target_os = "linux")]
    Descriptor(&'a File),
}

#[allow(clippy::too_many_arguments)]
fn inspect_copy(
    config: &PgConfig,
    runtime: &Runtime,
    scratch: &OwnedDirectory,
    dump: InspectedDump<'_>,
    limits: &Limits,
    cancel: &Cancellation,
    table: &'static str,
    step: &'static str,
) -> Result<Vec<u8>, Error> {
    let output = scratch.0.join(format!("{table}.sql"));
    #[cfg(target_os = "linux")]
    let inherited = match &dump {
        InspectedDump::Path(_) => None,
        InspectedDump::Descriptor(file) => {
            use std::os::fd::AsRawFd;
            Some(guarded_ready::duplicate(file.as_raw_fd())?)
        }
    };
    let file_argument = match &dump {
        InspectedDump::Path(path) => path.to_string_lossy().into_owned(),
        #[cfg(target_os = "linux")]
        InspectedDump::Descriptor(_) => {
            use std::os::fd::AsRawFd;
            format!(
                "/proc/self/fd/{}",
                inherited.as_ref().ok_or(Error::Io)?.as_raw_fd()
            )
        }
    };
    let args = vec![
        "--data-only".into(),
        "--schema=public".into(),
        format!("--table={table}"),
        "--file".into(),
        output.to_string_lossy().into_owned(),
        file_argument,
    ];
    let mut command = prepare_command(&runtime.pg_restore, config, &args, 1 << 20);
    #[cfg(target_os = "linux")]
    if let Some(fd) = inherited {
        guarded_ready::inherit(&mut command, fd);
    }
    checked(run(command, limits, cancel)?, step)?;
    let mut bytes = Vec::new();
    File::open(output)?
        .take((1 << 20) + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 1 << 20 {
        return Err(Error::Metadata);
    }
    Ok(bytes)
}

/// Read-only staged projection for the taskless restore worker. Keeping this
/// object alive keeps the exact dump that was inspected alive as well.
pub struct PreparedTasklessArchive {
    _dump: PreparedDump,
    #[cfg(target_os = "linux")]
    descriptor: File,
    plugin: serde_json::Value,
    periodic: Vec<serde_json::Value>,
}
impl PreparedTasklessArchive {
    /// Park the private helper at the validated bound connection without
    /// sending schema apply permission.
    #[cfg(target_os = "linux")]
    pub fn wait_guarded_ready(
        &self,
        helper: &Path,
        config: &PgConfig,
        target: GuardedRestoreIdentity<'_>,
        timeout: Duration,
    ) -> Result<ReadyGuardedRestore, Error> {
        guarded_ready::wait(helper, config, target, &self.descriptor, timeout)
    }

    pub fn matches_post_restore(
        &self,
        plugin: &serde_json::Value,
        periodic: &[serde_json::Value],
    ) -> bool {
        taskless_archive_rows::matches_post_restore(&self.plugin, &self.periodic, plugin, periodic)
    }
}
/// Inspect only. It neither connects to the database nor applies the staged dump.
#[allow(clippy::too_many_arguments)]
pub fn prepare_taskless_archive(
    config: &PgConfig,
    runtime: &Runtime,
    archive: &Path,
    scratch_root: &Path,
    limits: &Limits,
    cancel: &Cancellation,
    expected: &std::collections::BTreeSet<(String, String)>,
    current_plugin: &serde_json::Value,
    current_periodic: &[serde_json::Value],
    builtin_tasks: &std::collections::BTreeSet<String>,
) -> Result<PreparedTasklessArchive, Error> {
    let owned = stage_restore_dump(archive, scratch_root, limits, cancel)?;
    #[cfg(target_os = "linux")]
    let descriptor = guarded_ready::open_prepared(&owned.dump)?;
    let inspection = || {
        #[cfg(target_os = "linux")]
        {
            InspectedDump::Descriptor(&descriptor)
        }
        #[cfg(not(target_os = "linux"))]
        {
            InspectedDump::Path(&owned.dump)
        }
    };
    inspect_current_migrations(
        config,
        runtime,
        &owned.scratch,
        inspection(),
        limits,
        cancel,
        expected,
    )?;
    let plugin = inspect_copy(
        config,
        runtime,
        &owned.scratch,
        inspection(),
        limits,
        cancel,
        "plugins_pluginconfig",
        "inspect taskless restore registry",
    )?;
    let periodic = inspect_copy(
        config,
        runtime,
        &owned.scratch,
        inspection(),
        limits,
        cancel,
        "django_celery_beat_periodictask",
        "inspect taskless restore schedules",
    )?;
    let (plugin, periodic) = taskless_archive_rows::taskless_projection(
        &plugin,
        &periodic,
        current_plugin,
        current_periodic,
        builtin_tasks,
    )
    .map_err(|_| Error::Metadata)?;
    Ok(PreparedTasklessArchive {
        _dump: owned,
        #[cfg(target_os = "linux")]
        descriptor,
        plugin,
        periodic,
    })
}

/// Parse only the real pg_restore COPY projection, never execute its SQL output.
fn parse_migration_copy(
    bytes: &[u8],
    maximum_rows: usize,
) -> Result<std::collections::BTreeSet<(String, String)>, Error> {
    let text = std::str::from_utf8(bytes).map_err(|_| Error::Metadata)?;
    let mut found = false;
    let mut ended = false;
    let mut result = std::collections::BTreeSet::new();
    for line in text.lines() {
        if !found {
            if line == "COPY public.django_migrations (id, app, name, applied) FROM stdin;" {
                found = true;
            }
            continue;
        }
        if line == "\\." {
            ended = true;
            break;
        }
        let fields = line.splitn(5, '\t').collect::<Vec<_>>();
        if fields.len() != 4
            || fields[0].parse::<i64>().is_err()
            || !fields[1..3]
                .iter()
                .all(|v| !v.is_empty() && v.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'))
            || !result.insert((fields[1].to_string(), fields[2].to_string()))
        {
            return Err(Error::Metadata);
        }
        if result.len() > maximum_rows {
            return Err(Error::Metadata);
        }
    }
    if !found || !ended || result.is_empty() {
        return Err(Error::Metadata);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct EffectAuthority {
        count: std::sync::atomic::AtomicUsize,
        migrations: std::collections::BTreeSet<(String, String)>,
    }
    impl EffectAuthority {
        fn new() -> Self {
            Self {
                count: std::sync::atomic::AtomicUsize::new(0),
                migrations: Default::default(),
            }
        }
        fn calls(&self) -> usize {
            self.count.load(std::sync::atomic::Ordering::Acquire)
        }
    }
    impl CurrentSchemaRestoreAuthority for EffectAuthority {
        fn current_database(&self) -> &str {
            "owned_effect_fixture"
        }
        fn server_started_micros(&self) -> i64 {
            1
        }
        fn expected_migrations(&self) -> &std::collections::BTreeSet<(String, String)> {
            &self.migrations
        }
        fn before_destructive_submission(&self) {
            self.count.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        }
    }
    fn effect_ports(root: &Path) -> (PgConfig, Runtime) {
        (
            PgConfig {
                host: "owned-inert-no-network".into(),
                port: 1,
                user: "owned-fixture".into(),
                database: "owned_effect_fixture".into(),
                password: None,
                options: Default::default(),
            },
            Runtime {
                pg_dump: root.join("never-exec-dump"),
                pg_restore: root.join("never-exec-restore"),
                psql: root.join("never-exec-psql"),
            },
        )
    }
    #[test]
    fn bad_archive_never_announces_destructive_submission() {
        let root = OwnedDirectory::new(&std::env::temp_dir()).unwrap();
        let archive = root.0.join("owned.zip");
        File::create(&archive)
            .unwrap()
            .write_all(b"inert invalid archive")
            .unwrap();
        let (config, runtime) = effect_ports(&root.0);
        let authority = EffectAuthority::new();
        assert!(matches!(
            restore_current_schema(
                &config,
                &runtime,
                &archive,
                &root.0.join("scratch"),
                &Limits::default(),
                &Cancellation::default(),
                &authority
            ),
            Err(Error::Zip)
        ));
        assert_eq!(authority.calls(), 0);
    }
    #[test]
    fn wrong_engine_never_announces_destructive_submission() {
        let root = OwnedDirectory::new(&std::env::temp_dir()).unwrap();
        let archive = root.0.join("owned.zip");
        let mut zip = ZipWriter::new(File::create(&archive).unwrap());
        zip.start_file("metadata.json", SimpleFileOptions::default())
            .unwrap();
        zip.write_all(br#"{"database_type":"sqlite","database_file":"database.sqlite3"}"#)
            .unwrap();
        zip.start_file("database.sqlite3", SimpleFileOptions::default())
            .unwrap();
        zip.write_all(b"inert owned fixture").unwrap();
        zip.finish().unwrap();
        let (config, runtime) = effect_ports(&root.0);
        let authority = EffectAuthority::new();
        assert!(matches!(
            restore_current_schema(
                &config,
                &runtime,
                &archive,
                &root.0.join("scratch"),
                &Limits::default(),
                &Cancellation::default(),
                &authority
            ),
            Err(Error::SqliteDatabaseTypeMismatch)
        ));
        assert_eq!(authority.calls(), 0);
    }
    #[test]
    fn destructive_boundary_precedes_submission_error() {
        let authority = EffectAuthority::new();
        let result: Result<(), Error> = destructive_submission(Some(&authority), || {
            assert_eq!(authority.calls(), 1);
            Err(Error::Io)
        });
        assert!(matches!(result, Err(Error::Io)));
        assert_eq!(authority.calls(), 1);
    }
    #[test]
    fn unobserved_submission_keeps_existing_result() {
        let result: Result<(), Error> = destructive_submission(None, || Err(Error::Timeout));
        assert!(matches!(result, Err(Error::Timeout)));
    }

    #[test]
    fn restore_distinguishes_missing_metadata_and_sqlite_before_database_dispatch() {
        let fixture = OwnedDirectory::new(&std::env::temp_dir()).unwrap();
        let archive = fixture.0.join("owned-fixture.zip");
        let scratch = fixture.0.join("scratch");
        let stage = |members: &[(&str, &[u8])]| {
            let mut writer = ZipWriter::new(File::create(&archive).unwrap());
            for &(name, value) in members {
                writer
                    .start_file(name, SimpleFileOptions::default())
                    .unwrap();
                writer.write_all(value).unwrap();
            }
            writer.finish().unwrap();
            stage_restore_dump(
                &archive,
                &scratch,
                &Limits::default(),
                &Cancellation::default(),
            )
        };

        assert!(matches!(
            stage(&[("owned.txt", b"owned fixture")]),
            Err(Error::MissingMetadata)
        ));
        assert!(matches!(
            stage(&[
                (
                    "metadata.json",
                    br#"{"database_type":"sqlite","database_file":"database.sqlite3"}"#,
                ),
                ("database.sqlite3", b"owned inert fixture"),
            ]),
            Err(Error::SqliteDatabaseTypeMismatch)
        ));
        assert!(matches!(
            stage(&[
                (
                    "metadata.json",
                    br#"{"database_type":"unknown","database_file":"database.dump"}"#,
                ),
                ("database.dump", b"owned inert fixture"),
            ]),
            Err(Error::WrongDatabaseType)
        ));
    }

    #[test]
    fn current_restore_graph_uses_real_copy_projection_not_executable_sql() {
        let raw=b"-- pg_restore projection\nSET statement_timeout = 0;\nCOPY public.django_migrations (id, app, name, applied) FROM stdin;\n1\taccounts\t0001_initial\t2026-01-01 00:00:00+00\n2\tauth\t0001_initial\t2026-01-01 00:00:00+00\n\\.\nSELECT irrelevant();\n";
        let graph = parse_migration_copy(raw, 2).unwrap();
        assert_eq!(graph.len(), 2);
        assert!(graph.contains(&("accounts".into(), "0001_initial".into())));
        let mut duplicate = raw.to_vec();
        let offset = duplicate.windows(3).position(|w| w == b"\\.\n").unwrap();
        drop(
            duplicate.splice(
                offset..offset,
                b"3\taccounts\t0001_initial\t2026-01-01 00:00:00+00\n"
                    .iter()
                    .copied(),
            ),
        );
        assert!(parse_migration_copy(&duplicate, 2).is_err());
        assert!(parse_migration_copy(b"SELECT 'COPY public.django_migrations';", 2).is_err());
        assert!(parse_migration_copy(b"COPY public.django_migrations (id, app, name, applied) FROM stdin;\n1\taccounts\t0001_initial\tdate\n",2).is_err());
    }
    #[test]
    fn current_restore_plugin_guard_rejects_active_or_history_before_any_drop() {
        let header="COPY public.plugins_pluginconfig (key, enabled, owned_tasks, suspended_schedules, ever_enabled) FROM stdin;\n";
        let body = |row: &str| format!("{header}{row}\n\\.\n");
        assert!(
            restore_plugin_copy::require_disabled(body("owned\tf\t[]\t[]\tf").as_bytes()).is_ok()
        );
        for row in [
            "owned\tt\t[]\t[]\tf",
            "owned\tf\t[]\t[]\tt",
            "owned\tf\t[1]\t[]\tf",
            "owned\tf\t[]\t[1]\tf",
        ] {
            assert!(restore_plugin_copy::require_disabled(body(row).as_bytes()).is_err());
        }
        assert!(restore_plugin_copy::require_disabled(
            b"COPY public.plugins_pluginconfig (enabled) FROM stdin;\nf\n\\.\n"
        )
        .is_err());
    }

    use std::sync::mpsc;
    #[test]
    fn owned_admission_survives_observer_drop() {
        let admission = Admission::default();
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let observer = admission
            .submit(move || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok(())
            })
            .unwrap();
        started_rx.recv().unwrap();
        drop(observer);
        assert!(matches!(admission.submit(|| Ok(())), Err(Error::Busy)));
        release_tx.send(()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while admission.0.load(Ordering::Acquire) {
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
        admission
            .submit(|| Ok(()))
            .unwrap()
            .join()
            .unwrap()
            .unwrap();
    }
    #[test]
    fn diagnostics_bound_utf8_and_critical_text_beyond_retained_prefix() {
        let mut data = vec![b'a'; 8190];
        data.extend_from_slice(b"FATAL");
        data.extend_from_slice("café".as_bytes());
        let result = drain(data.as_slice(), 7).unwrap();
        assert_eq!(result.prefix.len(), 7);
        assert_eq!(result.bytes, data.len() as u64);
        assert!(result.critical);
        assert!(result.utf8_valid);
        let result = drain(&b"\xff"[..], 0).unwrap();
        assert!(!result.utf8_valid);
        assert!(result.prefix.is_empty());
    }
    #[cfg(unix)]
    #[test]
    fn running_command_cancel_and_timeout_reap_owned_process_group() {
        fn command() -> Command {
            let mut c = Command::new("/bin/sh");
            c.args(["-c", "sleep 30 & wait"])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            use std::os::unix::process::CommandExt;
            c.process_group(0);
            c
        }
        let cancel = Cancellation::default();
        let trigger = cancel.clone();
        let thread = thread::spawn(move || {
            thread::sleep(Duration::from_millis(70));
            trigger.cancel();
        });
        let began = Instant::now();
        assert!(matches!(
            run(command(), &Limits::default(), &cancel),
            Err(Error::Cancelled)
        ));
        thread.join().unwrap();
        assert!(began.elapsed() < Duration::from_secs(2));
        let began = Instant::now();
        let limits = Limits {
            command_timeout: Duration::from_millis(60),
            ..Limits::default()
        };
        assert!(matches!(
            run(command(), &limits, &Cancellation::default()),
            Err(Error::Timeout)
        ));
        assert!(began.elapsed() < Duration::from_secs(2));
    }
}
