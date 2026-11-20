//! Explicit manual PostgreSQL backup ownership on ordinary Main.
//! The original Dispatcharr authors retain application credit.

use dispatcharr_job_transport::{
    normal_backup::{Application, ManualApplication},
    normal_revocation::Revoked,
    transport::{Config, Jobs},
};
use dispatcharr_store::{AuthService, Store};
use std::{env, ffi::OsStr};

fn settings<'a>(
    owner: Option<&OsStr>,
    competing_owner: bool,
    broker: Option<&'a OsStr>,
    result: Option<&'a OsStr>,
) -> Result<Option<(&'a str, &'a str)>, &'static str> {
    let Some(owner) = owner else {
        return Ok(None);
    };
    if owner != OsStr::new("rust") {
        return Err("RUST_BACKUP_CREATE_OWNER must be rust when configured");
    }
    if competing_owner {
        return Err("Manual backup ownership conflicts with other application queue consumers");
    }
    fn explicit(value: &OsStr) -> Option<&str> {
        value
            .to_str()
            .filter(|url| !url.is_empty() && url.len() <= 4096 && url.trim() == *url)
    }
    let endpoints = broker.and_then(explicit).zip(result.and_then(explicit));
    endpoints.map(Some).ok_or(
        "Manual backup ownership requires explicit CELERY_BROKER_URL and CELERY_RESULT_BACKEND",
    )
}

pub async fn prepare(
    competing_owner: bool,
    store: Store,
    auth: AuthService,
    cache: Option<&str>,
) -> Result<Option<ManualApplication>, String> {
    let owner = env::var_os("RUST_BACKUP_CREATE_OWNER");
    let broker = env::var_os("CELERY_BROKER_URL");
    let result = env::var_os("CELERY_RESULT_BACKEND");
    let Some((broker, result)) = settings(
        owner.as_deref(),
        competing_owner,
        broker.as_deref(),
        result.as_deref(),
    )?
    else {
        return Ok(None);
    };
    let cache = cache.ok_or("Manual backup ownership requires Redis system settings")?;
    let (database, runtime, root, scratch) = crate::backup_config::configured()?;
    let jobs = Jobs::connect_backup(broker, result, Config::default())
        .await
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
        assert_eq!(settings(None, true, None, None), Ok(None));
        assert_eq!(
            settings(owner, false, broker, result),
            Ok(Some(("redis://fixture:6379/4", "redis://fixture:6379/5")))
        );
        assert!(settings(owner, true, broker, result).is_err());
        assert!(settings(owner, false, broker, None).is_err());
        assert!(settings(owner, false, None, result).is_err());
        assert!(settings(owner, false, Some(OsStr::new(" ")), result).is_err());
        assert!(settings(Some(OsStr::new("other")), false, broker, result).is_err());
    }
}
