//! Same actual Main database URL; credentials are never formatted into errors.
//! Original Dispatcharr authors retain application/configuration credit.
use dispatcharr_job_transport::normal_backup::{PgConfig, Runtime};
use std::{collections::BTreeMap, path::PathBuf};
fn decode(value: &str) -> Result<String, String> {
    let mut out = Vec::new();
    let bytes = value.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return Err("Invalid backup database configuration".into());
            }
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3])
                .map_err(|_| "Invalid backup database configuration")?;
            out.push(
                u8::from_str_radix(hex, 16).map_err(|_| "Invalid backup database configuration")?,
            );
            i += 3
        } else {
            out.push(bytes[i]);
            i += 1
        }
    }
    String::from_utf8(out).map_err(|_| "Invalid backup database configuration".into())
}
pub fn configured() -> Result<(PgConfig, Runtime, PathBuf, PathBuf), String> {
    let literal = crate::database_config::url()?;
    let url = url::Url::parse(&literal).map_err(|_| "Invalid backup database configuration")?;
    if !matches!(url.scheme(), "postgres" | "postgresql") {
        return Err("Backup requires actual PostgreSQL configuration".into());
    }
    let mut host = url
        .host_str()
        .unwrap_or("localhost")
        .trim_matches(['[', ']'])
        .to_string();
    let mut options = BTreeMap::new();
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "host" => host = value.into_owned(),
            "sslmode" | "sslrootcert" | "sslcert" | "sslkey" => {
                options.insert(key.into_owned(), value.into_owned());
            }
            "application_name" | "statement-cache-capacity" => {}
            _ => return Err("Unsupported backup database URL option".into()),
        }
    }
    let config = PgConfig {
        host,
        port: url.port().unwrap_or(5432),
        user: decode(url.username())?,
        database: decode(url.path().strip_prefix('/').unwrap_or(url.path()))?,
        password: url.password().map(decode).transpose()?,
        options,
    };
    if config.user.is_empty() || config.database.is_empty() {
        return Err("Backup database identity unavailable".into());
    }
    let runtime = Runtime {
        pg_dump: PathBuf::from("/usr/local/bin/pg_dump"),
        pg_restore: PathBuf::from("/usr/local/bin/pg_restore"),
        psql: PathBuf::from("/usr/local/bin/psql"),
    };
    let root =
        PathBuf::from(std::env::var("BACKUP_ROOT").unwrap_or_else(|_| "/data/backups".into()));
    Ok((config, runtime, root, std::env::temp_dir()))
}
