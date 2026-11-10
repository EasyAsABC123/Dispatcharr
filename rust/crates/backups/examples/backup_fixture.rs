//! Guarded disposable component CLI; no production restore or job authorization.
//! Original Dispatcharr authors retain application credit.
use dispatcharr_backups::*;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    io::{self, Read},
    path::{Path, PathBuf},
    time::SystemTime,
};
struct UtcClock;
impl SourceClock for UtcClock {
    fn filename_timestamp(&self) -> Result<String, Error> {
        Ok(chrono::Utc::now().format("%Y.%m.%d.%H.%M.%S").to_string())
    }
    fn created_at_utc(&self) -> Result<String, Error> {
        Ok(chrono::Utc::now().to_rfc3339())
    }
    fn zip_timestamp(&self, time: SystemTime) -> Result<zip::DateTime, Error> {
        let time: chrono::DateTime<chrono::Utc> = time.into();
        use chrono::{Datelike, Timelike};
        zip::DateTime::from_date_and_time(
            time.year().try_into().map_err(|_| Error::Metadata)?,
            time.month() as u8,
            time.day() as u8,
            time.hour() as u8,
            time.minute() as u8,
            time.second() as u8,
        )
        .map_err(|_| Error::Metadata)
    }
}
fn execute(input: Value) -> Result<Value, Error> {
    if std::env::var("BACKUP_DISPOSABLE_FIXTURE").as_deref() != Ok("playback-fixture-only") {
        return Err(Error::FixtureAuthorization);
    }
    let string = |key: &str| {
        input[key]
            .as_str()
            .map(str::to_owned)
            .ok_or(Error::FixtureAuthorization)
    };
    let database = string("database")?;
    let authority = DisposableRestore::authorize(&database)?;
    let config = PgConfig {
        host: string("host")?,
        port: input["port"]
            .as_u64()
            .and_then(|v| v.try_into().ok())
            .ok_or(Error::FixtureAuthorization)?,
        user: string("user")?,
        database,
        password: Some(string("password")?),
        options: BTreeMap::new(),
    };
    let runtime = Runtime {
        pg_dump: PathBuf::from("/usr/local/bin/pg_dump"),
        pg_restore: PathBuf::from("/usr/local/bin/pg_restore"),
        psql: PathBuf::from("/usr/local/bin/psql"),
    };
    let root = Path::new("/fixture");
    let scratch = Path::new("/tmp");
    let limits = Limits::default();
    let cancel = Cancellation::default();
    match string("action")?.as_str() {
        "create" => {
            let archive = create_backup(
                &config, &runtime, root, scratch, &UtcClock, &limits, &cancel,
            )?;
            Ok(
                json!({"created":true,"filename":archive.file_name().and_then(|v|v.to_str()).ok_or(Error::Metadata)?}),
            )
        }
        "restore" => {
            let filename = string("filename")?;
            if Path::new(&filename).file_name().and_then(|v| v.to_str()) != Some(filename.as_str())
            {
                return Err(Error::UnsafeMember);
            }
            restore_fixture(
                &config,
                &runtime,
                &root.join(filename),
                scratch,
                &limits,
                &cancel,
                &authority,
            )?;
            Ok(json!({"restored":true}))
        }
        _ => Err(Error::FixtureAuthorization),
    }
}
fn main() {
    let mut raw = Vec::new();
    if io::stdin().take(65537).read_to_end(&mut raw).is_err() || raw.len() > 65536 {
        eprintln!("bounded fixture request rejected");
        std::process::exit(2)
    }
    let input = serde_json::from_slice(&raw);
    let result = input.map_err(|_| Error::Metadata).and_then(execute);
    match result {
        Ok(value) => println!("{value}"),
        Err(error) => {
            eprintln!("component failed: {error:?}");
            std::process::exit(1)
        }
    }
}
