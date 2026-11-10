//! Copied-database-only Create worker probe; no public route/daemon authority.
//! Original Dispatcharr authors retain application credit and AGPL licensing.
use dispatcharr_backups::{Error as BackupError, Limits, PgConfig, Runtime, SourceClock};
use dispatcharr_job_transport::{
    transport::{Config, Jobs},
    worker::{ArchiveHandler, Completed, CreateHandler, Worker},
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    io::{self, Read},
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime},
};
struct UtcClock;
impl SourceClock for UtcClock {
    fn filename_timestamp(&self) -> Result<String, BackupError> {
        Ok(chrono::Utc::now().format("%Y.%m.%d.%H.%M.%S").to_string())
    }
    fn created_at_utc(&self) -> Result<String, BackupError> {
        Ok(chrono::Utc::now().to_rfc3339())
    }
    fn zip_timestamp(&self, time: SystemTime) -> Result<zip::DateTime, BackupError> {
        use chrono::{Datelike, Timelike};
        let t: chrono::DateTime<chrono::Utc> = time.into();
        zip::DateTime::from_date_and_time(
            t.year().try_into().map_err(|_| BackupError::Metadata)?,
            t.month() as u8,
            t.day() as u8,
            t.hour() as u8,
            t.minute() as u8,
            t.second() as u8,
        )
        .map_err(|_| BackupError::Metadata)
    }
}
struct Failure;
impl CreateHandler for Failure {
    fn create(&self) -> Result<Completed, String> {
        Err("fixture create failure".into())
    }
}
struct Gate {
    inner: Arc<dyn CreateHandler>,
    name: PathBuf,
}
impl CreateHandler for Gate {
    fn create(&self) -> Result<Completed, String> {
        std::fs::write(self.name.with_extension("entered"), b"entered")
            .map_err(|_| "Fixture marker IO")?;
        let deadline = std::time::Instant::now() + Duration::from_secs(90);
        while !self.name.with_extension("release").exists() {
            if std::time::Instant::now() > deadline {
                return Err("Fixture marker timeout".into());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        self.inner.create()
    }
}
#[tokio::main]
async fn main() {
    if std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref() != Ok("playback-fixture-only") {
        std::process::exit(2)
    }
    let mut raw = Vec::new();
    if io::stdin().take(16385).read_to_end(&mut raw).is_err() || raw.len() > 16384 {
        std::process::exit(2)
    }
    let value: Value = serde_json::from_slice(&raw).expect("Bounded fixture JSON");
    let string = |key: &str| value[key].as_str().expect("Fixture field");
    let database = std::env::var("POSTGRES_DB").expect("Copied database");
    let jobs = Jobs::connect_fixture(
        string("broker_url"),
        string("result_url"),
        &database,
        Config::default(),
    )
    .await
    .expect("Owned DB8/9");
    let mut handler: Arc<dyn CreateHandler> = if value["handler"] == "failure" {
        Arc::new(Failure)
    } else {
        let limits = Limits {
            database_bytes: 32 << 20,
            archive_bytes: 64 << 20,
            command_timeout: Duration::from_secs(30),
            ..Limits::default()
        };
        Arc::new(
            ArchiveHandler::new_fixture(
                PgConfig {
                    host: "127.0.0.1".into(),
                    port: value["pg_port"]
                        .as_u64()
                        .and_then(|n| n.try_into().ok())
                        .unwrap_or(5432),
                    user: std::env::var("POSTGRES_USER").expect("Fixture PG user"),
                    database: database.clone(),
                    password: Some("playback-fixture-only".into()),
                    options: BTreeMap::new(),
                },
                Runtime {
                    pg_dump: PathBuf::from(string("pg_dump")),
                    pg_restore: PathBuf::from("/usr/bin/pg_restore"),
                    psql: PathBuf::from("/usr/bin/psql"),
                },
                PathBuf::from(string("backup_root")),
                PathBuf::from(string("scratch_root")),
                Arc::new(UtcClock),
                limits,
            )
            .expect("Owned archive authority"),
        )
    };
    if let Some(name) = value["gate"].as_str() {
        assert!(
            name.starts_with("native-worker-")
                && name.len() < 128
                && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
        );
        handler = Arc::new(Gate {
            inner: handler,
            name: PathBuf::from("/evidence").join(name),
        });
    }
    let worker = Worker::new_fixture(jobs, handler);
    let result = worker.run_once(Arc::new(())).await;
    let drain = worker.shutdown(Duration::from_secs(5)).await;
    match (result, drain) {
        (Ok(outcome), Ok(())) => println!(
            "{}",
            json!({"completed":true,"outcome":format!("{outcome:?}")})
        ),
        (error, drain) => {
            eprintln!("Worker fixture failed: {error:?}, drain: {drain:?}");
            std::process::exit(1)
        }
    }
}
