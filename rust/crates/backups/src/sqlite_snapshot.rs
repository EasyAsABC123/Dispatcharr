//! SQLite online backup primitive. Original Dispatcharr and SQLite authors retain credit.
//! This is not a SQLite Main/router/restore implementation. The caller must hold
//! its owned AppData/path admission and account this synchronous work in its role.
use libsqlite3_sys::{
    sqlite3 as Db, sqlite3_backup as Copy, sqlite3_backup_finish, sqlite3_backup_init,
    sqlite3_backup_step, sqlite3_close, sqlite3_errcode, sqlite3_exec, sqlite3_open_v2,
    SQLITE_BUSY, SQLITE_DONE, SQLITE_LOCKED, SQLITE_OK, SQLITE_OPEN_READONLY,
    SQLITE_OPEN_READWRITE,
};
use std::{
    ffi::CString,
    fs::{self, OpenOptions},
    io,
    marker::PhantomData,
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
    ptr, thread,
    time::{Duration, Instant},
};
#[derive(Debug)]
pub enum SnapshotError {
    Io(io::Error),
    InvalidInput,
    Cancelled,
    Timeout,
    ByteLimit,
    Sqlite(i32),
}
impl From<io::Error> for SnapshotError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}
#[derive(Clone, Copy)]
pub struct SnapshotLimits {
    pub bytes: u64,
    pub timeout: Duration,
}
struct Database(*mut Db);
impl Database {
    fn open(path: &Path, flags: i32) -> Result<Self, SnapshotError> {
        let name =
            CString::new(path.as_os_str().as_bytes()).map_err(|_| SnapshotError::InvalidInput)?;
        let mut db = ptr::null_mut();
        // SAFETY: name is NUL-terminated for the call; sqlite owns a returned handle.
        let code = unsafe { sqlite3_open_v2(name.as_ptr(), &mut db, flags, ptr::null()) };
        if code != SQLITE_OK || db.is_null() {
            if !db.is_null() {
                unsafe { sqlite3_close(db) };
            }
            return Err(SnapshotError::Sqlite(code));
        }
        Ok(Self(db))
    }
}
impl Drop for Database {
    fn drop(&mut self) {
        unsafe { sqlite3_close(self.0) };
    }
}
struct Backup<'a> {
    raw: *mut Copy,
    _owners: PhantomData<(&'a Database, &'a Database)>,
}
impl<'a> Backup<'a> {
    fn begin(to: &'a Database, from: &'a Database) -> Result<Self, SnapshotError> {
        // SAFETY: both connections remain alive until Backup is finished/dropped.
        let raw = unsafe { sqlite3_backup_init(to.0, c"main".as_ptr(), from.0, c"main".as_ptr()) };
        if raw.is_null() {
            return Err(SnapshotError::Sqlite(unsafe { sqlite3_errcode(to.0) }));
        }
        Ok(Self {
            raw,
            _owners: PhantomData,
        })
    }
    fn finish(mut self) -> Result<(), SnapshotError> {
        let code = unsafe { sqlite3_backup_finish(self.raw) };
        self.raw = ptr::null_mut();
        if code == SQLITE_OK {
            Ok(())
        } else {
            Err(SnapshotError::Sqlite(code))
        }
    }
}
impl Drop for Backup<'_> {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            unsafe { sqlite3_backup_finish(self.raw) };
        }
    }
}
struct Output {
    path: PathBuf,
    keep: bool,
}
impl Drop for Output {
    fn drop(&mut self) {
        if !self.keep {
            let _ = fs::remove_file(&self.path);
        }
    }
}

/// Copy committed SQLite content, including WAL, into a fresh self-contained DB.
/// No raw copying of a live main file, pool replacement or migration is performed.
/// Paths must already have an AppData owner/path lease held by the caller; these
/// canonical checks alone are not a substitute for that application authority.
pub fn snapshot_sqlite(
    source: &Path,
    destination: &Path,
    limits: SnapshotLimits,
    cancelled: impl Fn() -> bool,
) -> Result<u64, SnapshotError> {
    let began = Instant::now();
    if cancelled() {
        return Err(SnapshotError::Cancelled);
    }
    if limits.bytes < 16
        || limits.timeout.is_zero()
        || !source.is_absolute()
        || !destination.is_absolute()
        || fs::canonicalize(source)? != source
        || !fs::metadata(source)?.is_file()
        || destination
            .parent()
            .is_none_or(|p| fs::canonicalize(p).map_or(true, |c| c != p))
    {
        return Err(SnapshotError::InvalidInput);
    }
    if fs::metadata(source)?.len() > limits.bytes {
        return Err(SnapshotError::ByteLimit);
    }
    // create_new refuses an existing database/symlink; never truncate another file.
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)?;
    drop(file);
    let mut output = Output {
        path: destination.to_owned(),
        keep: false,
    };
    let result = (|| -> Result<u64, SnapshotError> {
        let src = Database::open(source, SQLITE_OPEN_READONLY)?;
        let dst = Database::open(destination, SQLITE_OPEN_READWRITE)?;
        let backup = Backup::begin(&dst, &src)?;
        loop {
            if cancelled() {
                return Err(SnapshotError::Cancelled);
            }
            if began.elapsed() >= limits.timeout {
                return Err(SnapshotError::Timeout);
            }
            // Bounded batches; the byte budget can transiently overshoot by <=128 pages.
            let code = unsafe { sqlite3_backup_step(backup.raw, 128) };
            if fs::metadata(destination)?.len() > limits.bytes {
                return Err(SnapshotError::ByteLimit);
            }
            match code {
                SQLITE_DONE => break,
                SQLITE_OK => {}
                SQLITE_BUSY | SQLITE_LOCKED => thread::sleep(
                    Duration::from_millis(5).min(limits.timeout.saturating_sub(began.elapsed())),
                ),
                other => return Err(SnapshotError::Sqlite(other)),
            }
        }
        backup.finish()?;
        drop(dst);
        drop(src);
        let length = fs::metadata(destination)?.len();
        if length > limits.bytes {
            return Err(SnapshotError::ByteLimit);
        }
        fs::File::open(destination)?.sync_all()?;
        Ok(length)
    })();
    match result {
        Ok(length) => {
            output.keep = true;
            Ok(length)
        }
        Err(cause) => {
            // A cleanup IO error remains an error; never report disposal from cancellation alone.
            fs::remove_file(destination)?;
            output.keep = true;
            Err(cause)
        }
    }
}

pub(super) fn verify_catalogue(path: &Path) -> Result<(), SnapshotError> {
    // Like Original, open the extracted copy read/write so SQLite may create
    // private WAL companions before opening its catalogue. No current DB is used.
    let database = Database::open(path, SQLITE_OPEN_READWRITE)?;
    let code = unsafe {
        sqlite3_exec(
            database.0,
            c"SELECT name FROM sqlite_master WHERE type='table'".as_ptr(),
            None,
            ptr::null_mut(),
            ptr::null_mut(),
        )
    };
    if code == SQLITE_OK {
        Ok(())
    } else {
        Err(SnapshotError::Sqlite(code))
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use libsqlite3_sys::SQLITE_OPEN_CREATE;
    use std::{
        ffi::{c_char, c_int, c_void, CStr},
        sync::atomic::{AtomicUsize, Ordering},
        time::SystemTime,
    };

    struct Temp(PathBuf);
    impl Temp {
        fn new() -> Self {
            let n = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let p = std::env::temp_dir()
                .join(format!("owned-sqlite-snapshot-{}-{n}", std::process::id()));
            fs::create_dir(&p).unwrap();
            Self(fs::canonicalize(p).unwrap())
        }
    }
    impl Drop for Temp {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
    fn exec(db: &Database, sql: &str) {
        let s = CString::new(sql).unwrap();
        assert_eq!(
            unsafe { sqlite3_exec(db.0, s.as_ptr(), None, ptr::null_mut(), ptr::null_mut()) },
            0
        );
    }
    unsafe extern "C" fn scalar(
        ctx: *mut c_void,
        _n: c_int,
        values: *mut *mut c_char,
        _names: *mut *mut c_char,
    ) -> c_int {
        let text = unsafe { CStr::from_ptr(*values) }.to_bytes();
        let value = std::str::from_utf8(text)
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(-1);
        unsafe {
            *(ctx as *mut i64) = value;
        }
        0
    }
    fn query_one(db: &Database, sql: &std::ffi::CStr) -> i64 {
        let mut result = -1;
        assert_eq!(
            unsafe {
                sqlite3_exec(
                    db.0,
                    sql.as_ptr(),
                    Some(scalar),
                    (&mut result as *mut i64).cast(),
                    ptr::null_mut(),
                )
            },
            0
        );
        result
    }
    fn count(db: &Database) -> i64 {
        query_one(db, c"SELECT count(*) FROM owned")
    }

    pub(crate) fn create_fixture(path: &Path, sql: &str) {
        let database = Database::open(path, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE).unwrap();
        exec(&database, sql);
    }

    pub(crate) fn read_fixture_value(path: &Path, sql: &std::ffi::CStr) -> i64 {
        let database = Database::open(path, SQLITE_OPEN_READWRITE).unwrap();
        query_one(&database, sql)
    }

    fn limits() -> SnapshotLimits {
        SnapshotLimits {
            bytes: 1 << 20,
            timeout: Duration::from_secs(2),
        }
    }
    #[test]
    fn online_snapshot_includes_committed_wal_and_survives_source_change() {
        let t = Temp::new();
        let source = t.0.join("source.db");
        let out = t.0.join("snapshot.db");
        let db = Database::open(&source, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE).unwrap();
        exec(
            &db,
            "PRAGMA journal_mode=WAL;PRAGMA wal_autocheckpoint=0;CREATE TABLE owned(id INTEGER);INSERT INTO owned VALUES(1),(2);",
        );
        assert!(source.with_extension("db-wal").is_file());
        assert!(snapshot_sqlite(&source, &out, limits(), || false).unwrap() > 0);
        exec(&db, "INSERT INTO owned VALUES(3)");
        // Source restore verifies through sqlite3.connect(), a read/write connection.
        // A WAL-mode snapshot need not pre-create read-only -wal/-shm companions.
        let saved = Database::open(&out, SQLITE_OPEN_READWRITE).unwrap();
        assert_eq!(count(&saved), 2);
        assert_eq!(count(&db), 3);
    }
    #[test]
    fn existing_destination_is_never_overwritten() {
        let t = Temp::new();
        let source = t.0.join("source.db");
        let out = t.0.join("keep.db");
        let db = Database::open(&source, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE).unwrap();
        exec(&db, "CREATE TABLE owned(id INTEGER)");
        fs::write(&out, b"owned sentinel").unwrap();
        assert!(snapshot_sqlite(&source, &out, limits(), || false).is_err());
        assert_eq!(fs::read(out).unwrap(), b"owned sentinel");
    }
    #[test]
    fn cancellation_after_admission_removes_partial_output() {
        let t = Temp::new();
        let source = t.0.join("source.db");
        let out = t.0.join("cancelled.db");
        let db = Database::open(&source, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE).unwrap();
        exec(&db, "CREATE TABLE owned(id INTEGER)");
        let calls = AtomicUsize::new(0);
        assert!(matches!(
            snapshot_sqlite(&source, &out, limits(), || calls
                .fetch_add(1, Ordering::SeqCst)
                > 0),
            Err(SnapshotError::Cancelled)
        ));
        assert!(!out.exists());
        assert_eq!(count(&db), 0);
    }
    #[test]
    fn timeout_removes_output_without_mutating_source() {
        let t = Temp::new();
        let source = t.0.join("source.db");
        let out = t.0.join("timeout.db");
        let db = Database::open(&source, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE).unwrap();
        exec(&db, "CREATE TABLE owned(id INTEGER)");
        let short = SnapshotLimits {
            timeout: Duration::from_nanos(1),
            ..limits()
        };
        assert!(matches!(
            snapshot_sqlite(&source, &out, short, || false),
            Err(SnapshotError::Timeout)
        ));
        assert!(!out.exists());
        assert_eq!(count(&db), 0);
    }
    #[test]
    fn bounded_size_failure_never_publishes_output() {
        let t = Temp::new();
        let source = t.0.join("source.db");
        let out = t.0.join("small.db");
        let db = Database::open(&source, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE).unwrap();
        exec(&db, "CREATE TABLE owned(id INTEGER)");
        let small = SnapshotLimits {
            bytes: 16,
            ..limits()
        };
        assert!(matches!(
            snapshot_sqlite(&source, &out, small, || false),
            Err(SnapshotError::ByteLimit)
        ));
        assert!(!out.exists());
    }
}
