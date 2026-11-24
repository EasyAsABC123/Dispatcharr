//! Owned SQLite file restore. Original Dispatcharr and SQLite authors retain credit.
use super::*;

fn present_regular(path: &Path) -> Result<bool, Error> {
    match fs::symlink_metadata(path) {
        Ok(info) if info.is_file() => Ok(true),
        Ok(_) => Err(Error::FixtureAuthorization),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn require_closed_sidecars(database: &Path) -> Result<(), Error> {
    for suffix in ["-wal", "-shm", "-journal"] {
        let mut name = database.as_os_str().to_owned();
        name.push(suffix);
        match fs::symlink_metadata(PathBuf::from(name)) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => (),
            Err(error) => return Err(error.into()),
            Ok(_) => return Err(Error::Busy),
        }
    }
    Ok(())
}

fn durable_copy(
    source: &Path,
    output: &Path,
    bytes_limit: u64,
    cancel: &Cancellation,
) -> Result<(), Error> {
    let input = File::open(source)?;
    let mut out = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)?;
    copy(&input, &mut out, bytes_limit, cancel)?;
    out.set_permissions(input.metadata()?.permissions())?;
    out.sync_all()?;
    Ok(())
}

pub(super) fn install(
    payload: &Path,
    bytes_limit: u64,
    authority: &dyn CurrentSqliteRestoreAuthority,
    cancel: &Cancellation,
) -> Result<(), Error> {
    cancel.check()?;
    let database = authority.database();
    let parent = database
        .parent()
        .filter(|path| path.is_absolute())
        .ok_or(Error::FixtureAuthorization)?;
    if parent.canonicalize()? != parent || database.file_name().is_none() {
        return Err(Error::FixtureAuthorization);
    }
    require_closed_sidecars(database)?;
    let had_database = present_regular(database)?;
    let backup = database.with_extension("db.bak");
    if backup == database {
        return Err(Error::FixtureAuthorization);
    }
    present_regular(&backup)?;

    let scratch = OwnedDirectory::new(parent)?;
    let candidate = scratch.0.join("new.sqlite3");
    durable_copy(payload, &candidate, bytes_limit, cancel)?;
    super::sqlite_snapshot::verify_catalogue(&candidate).map_err(|_| Error::Metadata)?;
    let saved_previous = scratch.0.join("previous.sqlite3");
    if had_database {
        durable_copy(database, &saved_previous, bytes_limit, cancel)?;
    }
    let directory = File::open(parent)?;
    cancel.check()?;
    require_closed_sidecars(database)?;
    if had_database {
        fs::rename(saved_previous, &backup)?;
        directory.sync_all()?;
    }

    authority.before_destructive_submission();
    fs::rename(candidate, database)?;
    if let Err(error) = directory.sync_all().map_err(Error::from).and_then(|()| {
        super::sqlite_snapshot::verify_catalogue(database).map_err(|_| Error::Metadata)
    }) {
        // A failed installation keeps the detached backup even if the rollback fails.
        if had_database {
            let rollback = scratch.0.join("rollback.sqlite3");
            durable_copy(&backup, &rollback, bytes_limit, &Cancellation::default())?;
            fs::rename(rollback, database)?;
        } else {
            fs::remove_file(database)?;
        }
        directory.sync_all()?;
        return Err(error);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqlite_snapshot::tests::{create_fixture, read_fixture_value};
    use std::cell::Cell;

    struct Owner {
        database: PathBuf,
        submitted: Cell<bool>,
    }

    impl CurrentSqliteRestoreAuthority for Owner {
        fn database(&self) -> &Path {
            &self.database
        }

        fn before_destructive_submission(&self) {
            self.submitted.set(true);
        }
    }

    #[test]
    fn restore_installs_an_owned_database_and_retains_the_previous_data() {
        let root = OwnedDirectory::new(&std::env::temp_dir()).unwrap();
        let database = root.0.join("current.sqlite3");
        let payload = root.0.join("staged.sqlite3");
        create_fixture(
            &database,
            "CREATE TABLE t (value INTEGER); INSERT INTO t VALUES (21);",
        );
        create_fixture(
            &payload,
            "CREATE TABLE t (value INTEGER); INSERT INTO t VALUES (47);",
        );
        let owner = Owner {
            database: database.clone(),
            submitted: Cell::new(false),
        };
        install(&payload, 1 << 20, &owner, &Cancellation::default()).unwrap();
        assert!(owner.submitted.get());
        assert_eq!(read_fixture_value(&database, c"SELECT value FROM t"), 47);
        assert_eq!(
            read_fixture_value(&database.with_extension("db.bak"), c"SELECT value FROM t"),
            21
        );
    }

    #[test]
    fn restore_declines_unclosed_sqlite_sidecars_before_file_replacement() {
        let root = OwnedDirectory::new(&std::env::temp_dir()).unwrap();
        let database = root.0.join("current.sqlite3");
        let sidecar = root.0.join("current.sqlite3-wal");
        fs::write(&database, b"existing database bytes").unwrap();
        fs::write(&sidecar, b"existing WAL bytes").unwrap();
        let owner = Owner {
            database: database.clone(),
            submitted: Cell::new(false),
        };
        assert!(matches!(
            install(
                Path::new("irrelevant"),
                1 << 20,
                &owner,
                &Cancellation::default()
            ),
            Err(Error::Busy)
        ));
        assert!(!owner.submitted.get());
        assert!(!database.with_extension("db.bak").exists());
        assert_eq!(fs::read(&database).unwrap(), b"existing database bytes");
        assert_eq!(fs::read(&sidecar).unwrap(), b"existing WAL bytes");
    }
}
