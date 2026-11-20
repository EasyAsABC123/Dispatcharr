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

#[cfg(target_os = "linux")]
pub fn taskless_selected(
    selected: bool,
    cache: Option<&str>,
) -> Result<Option<dispatcharr_job_transport::normal_backup::VerifiedGuardedHelper>, String> {
    match std::env::var("RUST_BACKUP_TASKLESS_RESTORE_OWNER").as_deref() {
        Err(std::env::VarError::NotPresent) => return Ok(None),
        Ok("rust") => {}
        _ => return Err("RUST_BACKUP_TASKLESS_RESTORE_OWNER must be rust or unset".into()),
    }
    if !selected
        || std::env::var_os("RUST_BACKUP_CREATE_OWNER").is_some()
        || std::env::var_os("RUST_BACKUP_RESTORE_OWNER").is_some()
    {
        return Err("Taskless restore requires only the ordinary Selected backup owner".into());
    }
    let broker = std::env::var("CELERY_BROKER_URL")
        .map_err(|_| "Taskless restore requires an explicit dedicated broker")?;
    let result = std::env::var("CELERY_RESULT_BACKEND")
        .map_err(|_| "Taskless restore requires an explicit dedicated result backend")?;
    let cache = cache.ok_or("Taskless restore requires the actual Selected cache")?;
    if !distinct_namespaces([&broker, &result, cache]) {
        return Err("Taskless restore requires distinct Celery and cache Redis namespaces".into());
    }
    let budget = std::env::var("RUST_SHUTDOWN_SECONDS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .and_then(|total| total.checked_sub(26))
        .filter(|seconds| *seconds >= 10)
        .ok_or("Taskless restore requires an explicit supported shutdown budget")?
        .min(120);
    let exact = option_env!("DISPATCHARR_PACKAGED_TASKLESS_HELPER_SHA256")
        .ok_or("Taskless restore helper runtime was not built into this package")?;
    let helper = dispatcharr_job_transport::normal_backup::VerifiedGuardedHelper::verify(
        exact,
        std::time::Duration::from_secs(budget),
    )
    .map_err(|_| "Taskless restore exact packaged helper verification failed")?;
    Ok(Some(helper))
}

#[cfg(target_os = "linux")]
fn distinct_namespaces(redirects: [&str; 3]) -> bool {
    let mut roles = std::collections::BTreeSet::new();
    redirects.into_iter().all(|address| {
        let Ok(url) = url::Url::parse(address) else {
            return false;
        };
        if url.scheme() != "redis" || url.query().is_some() || url.fragment().is_some() {
            return false;
        }
        let Some(host) = url.host_str() else {
            return false;
        };
        let database = match url.path().trim_start_matches('/') {
            "" => Some(0),
            value => value.parse::<u32>().ok(),
        };
        database.is_some_and(|number| {
            roles.insert((
                host.to_ascii_lowercase(),
                url.port().unwrap_or(6379),
                number,
            ))
        })
    })
}

#[cfg(all(test, target_os = "linux"))]
mod taskless_controls {
    use super::*;

    #[test]
    fn experimental_taskless_requires_three_explicit_distinct_namespaces() {
        assert!(distinct_namespaces([
            "redis://fixture:6379/1",
            "redis://fixture:6379/2",
            "redis://fixture:6379/0",
        ]));
        assert!(!distinct_namespaces([
            "redis://fixture/1",
            "redis://FIXTURE:6379/01",
            "redis://fixture:6379/0",
        ]));
        assert!(!distinct_namespaces([
            "redis://fixture:6379/1?db=0",
            "redis://fixture:6379/2",
            "redis://fixture:6379/0",
        ]));
    }
}
