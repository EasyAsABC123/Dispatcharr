//! Source-compatible SQLite creation using the existing bounded v2 archive recipe.
//! Original Dispatcharr authors retain full application credit and AGPL license.
//! The ordinary datastore owner must hold its path lease through this call.
use super::sqlite_snapshot::{snapshot_sqlite, SnapshotError, SnapshotLimits};
use super::*;

fn snapshot_error(error: SnapshotError) -> Error {
    match error {
        SnapshotError::Cancelled => Error::Cancelled,
        SnapshotError::Timeout => Error::Timeout,
        SnapshotError::ByteLimit => Error::Capacity,
        SnapshotError::InvalidInput => Error::FixtureAuthorization,
        SnapshotError::Io(error) => error.into(),
        SnapshotError::Sqlite(code) => Error::Command {
            stage: "SQLite snapshot",
            code: Some(code),
        },
    }
}

pub(super) fn create_sqlite_backup(
    database: &Path,
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
    let dump = scratch.0.join("database.sqlite3");
    snapshot_sqlite(
        database,
        &dump,
        SnapshotLimits {
            bytes: limits.database_bytes,
            timeout: limits.command_timeout,
        },
        || cancel.check().is_err(),
    )
    .map_err(snapshot_error)?;
    let destination = backup_root.join(format!("dispatcharr-backup-{timestamp}.zip"));
    write_archive(
        destination,
        &dump,
        BackupEngine::Sqlite,
        clock,
        limits,
        cancel,
    )
}

#[cfg(test)]
mod tests {
    use super::super::sqlite_snapshot::tests::{create_fixture, read_fixture_value};
    use super::*;
    struct Clock;
    impl SourceClock for Clock {
        fn filename_timestamp(&self) -> Result<String, Error> {
            Ok("2026.11.23.12.30.00".into())
        }
        fn created_at_utc(&self) -> Result<String, Error> {
            Ok("2026-11-23T12:30:00+00:00".into())
        }
        fn zip_timestamp(&self, _: SystemTime) -> Result<zip::DateTime, Error> {
            Ok(zip::DateTime::default())
        }
    }
    fn source(root: &Path) -> PathBuf {
        let path = root.join("source.sqlite3");
        create_fixture(
            &path,
            "CREATE TABLE owned (id INTEGER); INSERT INTO owned VALUES (17);",
        );
        path
    }
    #[test]
    fn sqlite_archive_has_exact_source_v2_members_and_real_database() {
        let root = OwnedDirectory::new(&std::env::temp_dir()).unwrap();
        let database = source(&root.0);
        let result = create_backup_for_datastore(
            BackupDatastore::Sqlite {
                database: &database,
            },
            &root.0.join("backups"),
            &root.0.join("scratch"),
            &Clock,
            &Limits::default(),
            &Cancellation::default(),
        )
        .unwrap();
        let mut archive = ZipArchive::new(File::open(result).unwrap()).unwrap();
        assert_eq!(archive.len(), 2);
        let metadata: serde_json::Value =
            serde_json::from_reader(archive.by_name("metadata.json").unwrap()).unwrap();
        assert_eq!(
            metadata,
            serde_json::json!({"format":"dispatcharr-backup","version":2,"database_type":"sqlite","database_file":"database.sqlite3","created_at":"2026-11-23T12:30:00+00:00"})
        );
        let mut bytes = Vec::new();
        archive
            .by_name("database.sqlite3")
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();
        assert!(bytes.starts_with(b"SQLite format 3\0"));
        let restored = root.0.join("independent-readback.sqlite3");
        fs::write(&restored, bytes).unwrap();
        assert_eq!(read_fixture_value(&restored, c"SELECT id FROM owned"), 17);
        let restore_scratch = root.0.join("restore-scratch");
        let prepared = stage_sqlite_restore(
            &root
                .0
                .join("backups/dispatcharr-backup-2026.11.23.12.30.00.zip"),
            &restore_scratch,
            &Limits::default(),
            &Cancellation::default(),
        )
        .unwrap();
        assert_eq!(
            read_fixture_value(prepared.path(), c"SELECT id FROM owned"),
            17
        );
        let stage = prepared.path().to_owned();
        drop(prepared);
        assert!(!stage.exists());
        assert_eq!(fs::read_dir(root.0.join("scratch")).unwrap().count(), 0);
    }
    #[test]
    fn restore_stage_rejects_wrong_database_type_and_unreadable_payload_without_retaining_files() {
        let root = OwnedDirectory::new(&std::env::temp_dir()).unwrap();
        let scratch = root.0.join("restore-scratch");
        for (kind, expected) in [
            ("postgresql", Error::WrongDatabaseType),
            ("sqlite", Error::Metadata),
        ] {
            let archive = root.0.join(format!("{kind}.zip"));
            let mut zip = ZipWriter::new(File::create(&archive).unwrap());
            let options = SimpleFileOptions::default();
            zip.start_file("metadata.json", options).unwrap();
            write!(
                zip,
                "{{\"database_type\":\"{kind}\",\"database_file\":\"payload.bin\"}}"
            )
            .unwrap();
            zip.start_file("payload.bin", options).unwrap();
            zip.write_all(b"not a SQLite database").unwrap();
            zip.finish().unwrap();
            let result = stage_sqlite_restore(
                &archive,
                &scratch,
                &Limits::default(),
                &Cancellation::default(),
            );
            let error = result.err().expect("staging failed");
            assert_eq!(
                std::mem::discriminant(&error),
                std::mem::discriminant(&expected)
            );
            assert_eq!(fs::read_dir(&scratch).unwrap().count(), 0);
        }
    }

    #[test]
    fn cancelled_create_never_makes_an_archive() {
        let root = OwnedDirectory::new(&std::env::temp_dir()).unwrap();
        let database = source(&root.0);
        let cancel = Cancellation::default();
        cancel.cancel();
        assert!(matches!(
            create_backup_for_datastore(
                BackupDatastore::Sqlite {
                    database: &database
                },
                &root.0.join("backups"),
                &root.0.join("scratch"),
                &Clock,
                &Limits::default(),
                &cancel
            ),
            Err(Error::Cancelled)
        ));
        assert!(!root.0.join("backups").exists());
    }
    #[test]
    fn snapshot_capacity_failure_does_not_make_archive() {
        let root = OwnedDirectory::new(&std::env::temp_dir()).unwrap();
        let database = source(&root.0);
        let limits = Limits {
            database_bytes: 16,
            ..Limits::default()
        };
        assert!(matches!(
            create_backup_for_datastore(
                BackupDatastore::Sqlite {
                    database: &database
                },
                &root.0.join("backups"),
                &root.0.join("scratch"),
                &Clock,
                &limits,
                &Cancellation::default()
            ),
            Err(Error::Capacity)
        ));
        assert_eq!(fs::read_dir(root.0.join("backups")).unwrap().count(), 0);
        assert_eq!(fs::read_dir(root.0.join("scratch")).unwrap().count(), 0);
    }
}
