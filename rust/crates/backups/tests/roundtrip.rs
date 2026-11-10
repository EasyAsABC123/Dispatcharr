//! Actual original services cross-load against explicitly owned disposable databases.
//! Original Dispatcharr authors retain application credit.
use dispatcharr_backups::*;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::SystemTime,
};
struct Clock;
impl SourceClock for Clock {
    fn filename_timestamp(&self) -> Result<String, Error> {
        Ok("2026.11.09.12.13.14".into())
    }
    fn created_at_utc(&self) -> Result<String, Error> {
        Ok("2026-11-09T12:13:14+00:00".into())
    }
    fn zip_timestamp(&self, _: SystemTime) -> Result<zip::DateTime, Error> {
        zip::DateTime::from_date_and_time(2026, 11, 9, 12, 13, 14).map_err(|_| Error::Metadata)
    }
}
fn helper(db: &str, action: Value) -> Value {
    let mut child = Command::new("/dispatcharrpy/bin/python")
        .arg("/backup-tools/backup_fixture.py")
        .env("POSTGRES_DB", db)
        .env("BACKUP_ROOT", "/evidence/native-backups")
        .env("BACKUP_DISPOSABLE_FIXTURE", "playback-fixture-only")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(action.to_string().as_bytes())
        .unwrap();
    let result = child.wait_with_output().unwrap();
    std::fs::write("/evidence/last-source-helper-private.log", &result.stderr).unwrap();
    assert!(
        result.status.success(),
        "Original helper failed; private evidence retained"
    );
    serde_json::from_slice(
        result
            .stdout
            .split(|v| *v == b'\n')
            .filter(|v| !v.is_empty())
            .next_back()
            .unwrap(),
    )
    .unwrap()
}
#[test]
#[ignore = "Requires explicitly guarded disposable original PG17 container and three copied databases"]
fn original_native_copied_database_cross_restore() {
    assert_eq!(
        std::env::var("BACKUP_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let names: Vec<String> =
        serde_json::from_str(&std::env::var("BACKUP_DATABASE_NAMES").unwrap()).unwrap();
    assert_eq!(names.len(), 3);
    for db in &names {
        DisposableRestore::authorize(db).unwrap();
    }
    let source: Value =
        serde_json::from_slice(&std::fs::read("/evidence/source-seed.json").unwrap()).unwrap();
    let archive: Value =
        serde_json::from_slice(&std::fs::read("/evidence/source-archive.json").unwrap()).unwrap();
    let runtime = Runtime {
        pg_dump: PathBuf::from("/usr/lib/postgresql/17/bin/pg_dump"),
        pg_restore: PathBuf::from("/usr/lib/postgresql/17/bin/pg_restore"),
        psql: PathBuf::from("/usr/lib/postgresql/17/bin/psql"),
    };
    let cfg = PgConfig {
        host: "127.0.0.1".into(),
        port: 5432,
        user: "dispatch".into(),
        database: names[1].clone(),
        password: Some("playback-fixture-only".into()),
        options: BTreeMap::new(),
    };
    let scratch = Path::new("/evidence/backup-scratch");
    std::fs::create_dir_all(scratch).unwrap();
    let limits = Limits::default();
    let original =
        Path::new("/evidence/source-backups").join(archive["filename"].as_str().unwrap());
    restore_fixture(
        &cfg,
        &runtime,
        &original,
        scratch,
        &limits,
        &Cancellation::default(),
        &DisposableRestore::authorize(&names[1]).unwrap(),
    )
    .unwrap();
    assert_eq!(
        helper(&names[1], json!({"action":"manifest"}))["manifest"],
        source["manifest"],
        "Source ZIP -> native restore exact schema/data/sequence mismatch"
    );
    let credential = helper(&names[1], json!({"action":"credentials"}));
    assert_eq!(
        credential,
        json!({"admin_password":true,"viewer_password":true,"api_key":true,"groups":1,"channel_profiles":1})
    );
    let mut denied = cfg.clone();
    denied.password = Some("deliberately-invalid-fixture-password".into());
    assert!(matches!(
        create_backup(
            &denied,
            &runtime,
            Path::new("/evidence/denied"),
            scratch,
            &Clock,
            &limits,
            &Cancellation::default()
        ),
        Err(Error::Command {
            stage: "pg_dump",
            ..
        })
    ));
    assert_eq!(
        helper(&names[1], json!({"action":"manifest"}))["manifest"],
        source["manifest"],
        "Failed dump must preserve copied source data"
    );
    let original_auth = helper(
        &names[1],
        json!({"action":"source_dump_fault","fault":"password"}),
    );
    assert_eq!(original_auth["failed"], true);
    let original_privilege = helper(
        &names[1],
        json!({"action":"source_dump_fault","fault":"privilege"}),
    );
    assert_eq!(original_privilege["failed"], true);
    let mut denied_role = cfg.clone();
    denied_role.user = format!("{}_denied", names[1]);
    denied_role.password = Some("denied-fixture-password".into());
    assert!(matches!(
        create_backup(
            &denied_role,
            &runtime,
            Path::new("/evidence/denied-role"),
            scratch,
            &Clock,
            &limits,
            &Cancellation::default()
        ),
        Err(Error::Command {
            stage: "pg_dump",
            ..
        })
    ));
    let native = create_backup(
        &cfg,
        &runtime,
        Path::new("/evidence/native-backups"),
        scratch,
        &Clock,
        &limits,
        &Cancellation::default(),
    )
    .unwrap();
    let mut zip = zip::ZipArchive::new(std::fs::File::open(&native).unwrap()).unwrap();
    assert_eq!(zip.len(), 2);
    let metadata: Value = serde_json::from_reader(zip.by_name("metadata.json").unwrap()).unwrap();
    assert_eq!(metadata["format"], "dispatcharr-backup");
    assert_eq!(metadata["version"], 2);
    assert_eq!(metadata["database_file"], "database.dump");
    drop(zip);
    let restored = helper(
        &names[2],
        json!({"action":"source_restore","filename":native.file_name().unwrap().to_str().unwrap()}),
    );
    assert_eq!(
        restored["manifest"], source["manifest"],
        "Native ZIP -> actual original restore task exact schema/data/sequence mismatch"
    );
    assert_eq!(
        helper(&names[2], json!({"action":"credentials"})),
        credential
    );
    assert!(matches!(
        DisposableRestore::authorize("dispatcharr"),
        Err(Error::FixtureAuthorization)
    ));
    let cancel = Cancellation::default();
    cancel.cancel();
    assert!(matches!(
        create_backup(
            &cfg,
            &runtime,
            Path::new("/evidence/cancelled"),
            scratch,
            &Clock,
            &limits,
            &cancel
        ),
        Err(Error::Cancelled)
    ));
    let tiny = Limits {
        archive_bytes: 64,
        ..limits.clone()
    };
    assert!(create_backup(
        &cfg,
        &runtime,
        Path::new("/evidence/bounded"),
        scratch,
        &Clock,
        &tiny,
        &Cancellation::default()
    )
    .is_err());
    assert!(
        std::fs::read_dir(scratch).unwrap().next().is_none(),
        "Owned scratch leaked"
    );
    println!("BACKUP_CROSS_RESTORE_CHECKS {{\"source_to_native\":true,\"native_to_source\":true,\"exact_schema_rows_sequences\":true,\"credentials\":true,\"metadata\":true,\"cancelled_before_start\":true,\"archive_cap\":true,\"scratch_cleanup\":true,\"actual_failed_authentication\":true,\"actual_denied_role\":true}}");
}
