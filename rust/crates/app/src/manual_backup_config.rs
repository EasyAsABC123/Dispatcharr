//! Explicit manual PostgreSQL backup ownership on ordinary Main.
//! The original Dispatcharr authors retain application credit.

use dispatcharr_job_transport::{
    normal_backup::{Application, ManualApplication},
    normal_revocation::Revoked,
    transport::{Config, Jobs},
};
use dispatcharr_store::{AuthService, Store};
use std::{env, ffi::OsStr, time::Duration};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Create,
    Restore,
}

fn settings<'a>(
    create: Option<&OsStr>,
    restore: Option<&OsStr>,
    competing_owner: bool,
    broker: Option<&'a OsStr>,
    result: Option<&'a OsStr>,
) -> Result<Option<(Kind, &'a str, &'a str)>, &'static str> {
    let kind = match (create, restore) {
        (None, None) => return Ok(None),
        (Some(_), Some(_)) => {
            return Err(
                "Configure only one of RUST_BACKUP_CREATE_OWNER and RUST_BACKUP_RESTORE_OWNER",
            )
        }
        (Some(owner), None) if owner == OsStr::new("rust") => Kind::Create,
        (Some(_), None) => return Err("RUST_BACKUP_CREATE_OWNER must be rust when configured"),
        (None, Some(owner)) if owner == OsStr::new("rust") => Kind::Restore,
        (None, Some(_)) => return Err("RUST_BACKUP_RESTORE_OWNER must be rust when configured"),
    };
    if competing_owner {
        return Err("Manual backup ownership conflicts with other application queue consumers");
    }
    fn explicit(value: &OsStr) -> Option<&str> {
        value
            .to_str()
            .filter(|url| !url.is_empty() && url.len() <= 4096 && url.trim() == *url)
    }
    let endpoints = broker.and_then(explicit).zip(result.and_then(explicit));
    endpoints
        .map(|(broker, result)| Some((kind, broker, result)))
        .ok_or(
            "Manual backup ownership requires explicit CELERY_BROKER_URL and CELERY_RESULT_BACKEND",
        )
}

// Leave two of Main's eight-second quanta for plugin and shared service teardown.
// Main still reports an incomplete overall drain if other owners do not settle.
pub fn restore_drain_grace(shutdown_timeout: Duration) -> Result<Duration, &'static str> {
    let grace = shutdown_timeout
        .saturating_sub(Duration::from_secs(16))
        .min(Duration::from_secs(120));
    if grace.is_zero() {
        return Err("Standalone restore requires RUST_SHUTDOWN_SECONDS greater than 16 seconds");
    }
    Ok(grace)
}

pub async fn prepare(
    competing_owner: bool,
    store: Store,
    auth: AuthService,
    cache: Option<&str>,
) -> Result<Option<ManualApplication>, String> {
    let create = env::var_os("RUST_BACKUP_CREATE_OWNER");
    let restore = env::var_os("RUST_BACKUP_RESTORE_OWNER");
    let broker = env::var_os("CELERY_BROKER_URL");
    let result = env::var_os("CELERY_RESULT_BACKEND");
    let Some((kind, broker, result)) = settings(
        create.as_deref(),
        restore.as_deref(),
        competing_owner,
        broker.as_deref(),
        result.as_deref(),
    )?
    else {
        return Ok(None);
    };
    let cache = cache.ok_or("Manual backup ownership requires Redis system settings")?;
    let (database, runtime, root, scratch) = crate::backup_config::configured()?;
    let jobs = match kind {
        Kind::Create => Jobs::connect_backup(broker, result, Config::default()).await,
        Kind::Restore => Jobs::connect_backup_restore(broker, result, Config::default()).await,
    }
    .map_err(|_| "Manual backup broker initialization failed (credentials omitted)")?;
    let app = Application::prepare(
        jobs,
        Revoked::new(),
        store,
        auth.clone(),
        cache.to_owned(),
        database,
        runtime,
        root,
        scratch,
        auth.producer_identity_source(),
    )
    .map_err(|_| "Manual backup preparation failed")?;
    ManualApplication::new(app)
        .map(Some)
        .map_err(|_| "Manual backup ownership preparation failed".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owner_is_explicit_and_excludes_a_competing_application_consumer() {
        let owner = Some(OsStr::new("rust"));
        let broker = Some(OsStr::new("redis://fixture:6379/4"));
        let result = Some(OsStr::new("redis://fixture:6379/5"));
        assert_eq!(settings(None, None, true, None, None), Ok(None));
        for (create, restore, kind) in [(owner, None, Kind::Create), (None, owner, Kind::Restore)] {
            assert_eq!(
                settings(create, restore, false, broker, result),
                Ok(Some((
                    kind,
                    "redis://fixture:6379/4",
                    "redis://fixture:6379/5"
                )))
            );
            assert!(settings(create, restore, true, broker, result).is_err());
            assert!(settings(create, restore, false, broker, None).is_err());
            assert!(settings(create, restore, false, None, result).is_err());
            assert!(settings(create, restore, false, Some(OsStr::new(" ")), result).is_err());
        }
        let invalid = Some(OsStr::new("other"));
        assert!(settings(invalid, None, false, broker, result).is_err());
        assert!(settings(None, invalid, false, broker, result).is_err());
        assert!(settings(owner, owner, false, broker, result).is_err());
    }

    #[test]
    fn restore_drain_is_bounded_by_main_and_the_restore_ceiling() {
        assert!(restore_drain_grace(Duration::from_secs(12)).is_err());
        assert!(restore_drain_grace(Duration::from_secs(16)).is_err());
        assert_eq!(
            restore_drain_grace(Duration::from_secs(17)),
            Ok(Duration::from_secs(1))
        );
        assert_eq!(
            restore_drain_grace(Duration::from_secs(45)),
            Ok(Duration::from_secs(29))
        );
        assert_eq!(
            restore_drain_grace(Duration::from_secs(600)),
            Ok(Duration::from_secs(120))
        );
    }
}
