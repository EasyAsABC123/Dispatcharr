//! Private, pre-effect guard handshake for the isolated Linux helper.
use super::{connection_args, prepare_command, ChildOwner, Error, PgConfig};
use std::{
    fs::{File, OpenOptions, Permissions},
    io::{Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd},
        unix::{
            fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
            net::UnixStream,
            process::CommandExt,
        },
    },
    path::Path,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

const PACKAGED_HELPER: &str =
    "/opt/dispatcharr-pg-guarded-restore/bin/dispatcharr-pg-guarded-restore";
const MAX_HELPER_BYTES: u64 = 32 << 20;

#[derive(Clone)]
pub struct VerifiedGuardedHelper {
    expected_sha256: String,
    timeout: Duration,
}

impl VerifiedGuardedHelper {
    /// Bind an immutable deployed helper to the digest embedded in the image.
    pub fn verify(expected_sha256: &str, timeout: Duration) -> Result<Self, Error> {
        let valid = Self {
            expected_sha256: expected_sha256.to_owned(),
            timeout,
        };
        if timeout.is_zero() || timeout > Duration::from_secs(120) {
            return Err(Error::Capacity);
        }
        valid.path()?;
        Ok(valid)
    }

    /// Recheck the immutable regular ELF before admitting each helper launch.
    pub fn path(&self) -> Result<&'static Path, Error> {
        let path = Path::new(PACKAGED_HELPER);
        if path.canonicalize()? != path {
            return Err(Error::FixtureAuthorization);
        }
        for directory in path.ancestors().skip(1) {
            let metadata = directory.metadata()?;
            if !metadata.is_dir() || metadata.uid() != 0 || metadata.mode() & 0o022 != 0 {
                return Err(Error::FixtureAuthorization);
            }
        }
        let executable = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)?;
        verify_executable(executable, 0, &self.expected_sha256)?;
        Ok(path)
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }
}

fn verify_executable(mut executable: File, uid: u32, expected_sha256: &str) -> Result<(), Error> {
    use sha2::{Digest, Sha256};

    let info = executable.metadata()?;
    if !info.is_file()
        || info.uid() != uid
        || info.mode() & 0o022 != 0
        || info.mode() & 0o111 == 0
        || info.len() > MAX_HELPER_BYTES
    {
        return Err(Error::FixtureAuthorization);
    }
    let mut magic = [0; 4];
    executable.read_exact(&mut magic)?;
    if magic != *b"\x7fELF" {
        return Err(Error::FixtureAuthorization);
    }
    let mut hash = Sha256::new();
    hash.update(magic);
    let count = std::io::copy(&mut executable.take(MAX_HELPER_BYTES), &mut hash)?;
    if count + 4 != info.len() || format!("{:x}", hash.finalize()) != expected_sha256 {
        return Err(Error::FixtureAuthorization);
    }
    Ok(())
}

/// Values come from the same retained Main connection via the typed worker.
pub struct GuardedRestoreIdentity<'a> {
    pub database: &'a str,
    pub database_oid: i64,
    pub server_started_micros: i64,
    pub fence_pid: i32,
    pub fence_started_micros: i64,
}

/// A running private helper parked before SQL effects.
pub struct ReadyGuardedRestore(Helper);

/// The helper has applied the archive inside the still-open transaction.
pub struct PendingGuardedCommit(Helper);

struct Helper {
    control: UnixStream,
    child: ChildOwner,
    _prepared: File,
    deadline: Instant,
}

fn send(control: &mut UnixStream, message: &[u8], deadline: Instant) -> Result<(), Error> {
    let limit = deadline.saturating_duration_since(Instant::now());
    if limit.is_zero() {
        return Err(Error::Timeout);
    }
    control.set_write_timeout(Some(limit.min(Duration::from_secs(30))))?;
    control.write_all(message)?;
    Ok(())
}

impl ReadyGuardedRestore {
    pub fn apply(mut self) -> Result<PendingGuardedCommit, Error> {
        let helper = &mut self.0;
        send(&mut helper.control, b"APPLY\n", helper.deadline)?;
        read_expected(&helper.control, helper.deadline, b"PRECOMMIT\n")?;
        Ok(PendingGuardedCommit(self.0))
    }
}

impl PendingGuardedCommit {
    /// An error after entering this method leaves commit status unknown.
    pub fn commit(mut self) -> Result<(), Error> {
        let helper = &mut self.0;
        send(&mut helper.control, b"COMMIT\n", helper.deadline)?;
        read_expected(&helper.control, helper.deadline, b"DONE\n")?;
        loop {
            if let Some(status) = helper.child.child.try_wait()? {
                helper.child.cleaned = true;
                return if status.success() {
                    Ok(())
                } else {
                    Err(Error::Command {
                        stage: "guarded restore commit",
                        code: status.code(),
                    })
                };
            }
            if Instant::now() >= helper.deadline {
                return Err(Error::Timeout);
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}

pub(super) fn open_prepared(path: &Path) -> Result<File, Error> {
    let fd = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let info = fd.metadata()?;
    if !info.is_file() || info.len() == 0 || info.nlink() != 1 {
        return Err(Error::Metadata);
    }
    fd.set_permissions(Permissions::from_mode(0o600))?;
    Ok(fd)
}

pub(super) fn duplicate(fd: i32) -> std::io::Result<OwnedFd> {
    // Keep originals above the well-known child slots and close them at exec.
    let safe = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 10) };
    if safe < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(safe) })
}

pub(super) fn inherit(command: &mut Command, descriptor: OwnedFd) {
    // The allocated descriptor is already held in this Command before spawn.
    // No destination fd can overlap the child launcher or another live file.
    unsafe {
        command.pre_exec(move || {
            let fd = descriptor.as_raw_fd();
            let flags = libc::fcntl(fd, libc::F_GETFD);
            if flags < 0 || libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

fn read_expected(
    mut control: &UnixStream,
    deadline: Instant,
    mut read: &[u8],
) -> Result<(), Error> {
    // Use one absolute deadline even if a peer sends the short frame in parts.
    let mut bytes = [0; 16];
    while !read.is_empty() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(Error::Timeout);
        }
        control.set_read_timeout(Some(remaining))?;
        match control.read(&mut bytes[..read.len()]) {
            Ok(0) => return Err(Error::FixtureAuthorization),
            Ok(count) if bytes[..count] == read[..count] => read = &read[count..],
            Ok(_) => return Err(Error::FixtureAuthorization),
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
                ) =>
            {
                return Err(Error::Timeout);
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

pub(super) fn wait(
    helper: &Path,
    config: &PgConfig,
    target: GuardedRestoreIdentity<'_>,
    prepared: &File,
    timeout: Duration,
) -> Result<ReadyGuardedRestore, Error> {
    if !helper.is_absolute()
        || target.database != config.database
        || target.database.is_empty()
        || target.database.len() > 63
        || target.database.bytes().any(|byte| byte < 32 || byte == 127)
        || target.database_oid <= 0
        || target.fence_pid <= 0
        || target.fence_started_micros < target.server_started_micros
        || timeout.is_zero()
    {
        return Err(Error::FixtureAuthorization);
    }
    let timeout = timeout.min(Duration::from_secs(3600));
    let deadline = Instant::now() + timeout;
    let ready_deadline = deadline.min(Instant::now() + Duration::from_secs(30));
    let (mut parent, socket) = UnixStream::pair()?;
    let archived = duplicate(prepared.as_raw_fd())?;
    let channel = duplicate(socket.as_raw_fd())?;
    let mut args = vec![
        "--dispatcharr-guard".into(),
        format!("--dispatcharr-control-fd={}", channel.as_raw_fd()),
        format!("--dispatcharr-archive-fd={}", archived.as_raw_fd()),
    ];
    args.extend(connection_args(config));
    let mut command = prepare_command(helper, config, &args, 1 << 20);
    command.stdout(Stdio::null()).stderr(Stdio::null());
    inherit(&mut command, archived);
    inherit(&mut command, channel);
    let child = ChildOwner {
        child: command.spawn()?,
        cleaned: false,
    };
    drop(command);
    drop(socket);
    let binding = format!(
        "DPR1\n{}\n{}\n{}\n{}\n{}\n",
        target.database,
        target.database_oid,
        target.server_started_micros,
        target.fence_pid,
        target.fence_started_micros
    );
    send(&mut parent, binding.as_bytes(), ready_deadline)?;
    read_expected(&parent, ready_deadline, b"READY\n")?;
    Ok(ReadyGuardedRestore(Helper {
        control: parent,
        child,
        _prepared: prepared.try_clone()?,
        deadline,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn helper_admission_uses_exact_elf_bytes_and_rejects_writable_modes() {
        use sha2::{Digest, Sha256};

        let working = super::super::OwnedDirectory::new(&std::env::temp_dir()).unwrap();
        let path = working.0.join("fixture");
        let marker = b"\x7fELF test-only private helper fingerprint";
        std::fs::write(&path, marker).unwrap();
        std::fs::set_permissions(&path, Permissions::from_mode(0o500)).unwrap();
        let as_owner = path.metadata().unwrap().uid();
        let expected = format!("{:x}", Sha256::digest(marker));
        assert!(verify_executable(File::open(&path).unwrap(), as_owner, &expected).is_ok());
        assert!(matches!(
            verify_executable(File::open(&path).unwrap(), as_owner, &"0".repeat(64)),
            Err(Error::FixtureAuthorization)
        ));
        std::fs::set_permissions(&path, Permissions::from_mode(0o520)).unwrap();
        assert!(matches!(
            verify_executable(File::open(&path).unwrap(), as_owner, &expected),
            Err(Error::FixtureAuthorization)
        ));
    }

    #[test]
    fn exact_ready_frame_is_the_only_accepted_reply() {
        let check = |reply: &[u8]| {
            let (owner, mut peer) = UnixStream::pair().unwrap();
            peer.write_all(reply).unwrap();
            drop(peer);
            read_expected(&owner, Instant::now() + Duration::from_secs(1), b"READY\n")
        };
        assert!(check(b"READY\n").is_ok());
        assert!(matches!(
            check(b"PRECOMMIT\n"),
            Err(Error::FixtureAuthorization)
        ));
        assert!(matches!(check(b"READ"), Err(Error::FixtureAuthorization)));
        let (owner, _peer) = UnixStream::pair().unwrap();
        assert!(matches!(
            read_expected(&owner, Instant::now(), b"READY\n"),
            Err(Error::Timeout)
        ));
    }
    #[test]
    #[ignore = "requires an owned disposable PostgreSQL Unix socket and exact private helper"]
    fn actual_disposable_guard_helper_commit() {
        let values: Vec<String> = serde_json::from_str(
            &std::env::var("DISPATCHARR_OWNED_GUARDED_RESTORE_FIXTURE").unwrap(),
        )
        .unwrap();
        assert_eq!(values.len(), 10);
        let (helper, archive, socket) = (
            Path::new(&values[0]),
            Path::new(&values[1]),
            Path::new(&values[2]),
        );
        assert!(socket.is_absolute() && socket.starts_with("/tmp") && socket.is_dir());
        let database = &values[5];
        super::super::DisposableRestore::authorize(database).unwrap();
        let config = PgConfig {
            host: values[2].clone(),
            port: values[3].parse().unwrap(),
            user: values[4].clone(),
            database: database.clone(),
            password: None,
            options: std::collections::BTreeMap::new(),
        };
        let target = GuardedRestoreIdentity {
            database,
            database_oid: values[6].parse().unwrap(),
            server_started_micros: values[7].parse().unwrap(),
            fence_pid: values[8].parse().unwrap(),
            fence_started_micros: values[9].parse().unwrap(),
        };
        let held = open_prepared(archive).unwrap();
        let ready = wait(helper, &config, target, &held, Duration::from_secs(10)).unwrap();
        let pending = ready.apply().unwrap();
        pending.commit().unwrap();
        assert!(held.metadata().unwrap().len() > 0);
    }
}
