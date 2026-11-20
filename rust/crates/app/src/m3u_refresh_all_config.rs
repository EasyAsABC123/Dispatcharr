//! Opt-in publication for an independently owned Original Celery worker.
use dispatcharr_job_transport::transport::OriginalM3uRefreshAll;
use dispatcharr_store::AuthService;
use std::{env, ffi::OsStr};

const OWNER: &str = "RUST_M3U_REFRESH_ALL_OWNER";
const ORIGINAL_CELERY: &str = "original-celery";

fn broker_for_owner<'a>(
    selected: bool,
    native_queue_enabled: bool,
    owner: Option<&OsStr>,
    broker: Option<&'a OsStr>,
) -> Result<Option<&'a str>, &'static str> {
    if selected || owner.is_none() {
        return Ok(None);
    }
    if owner != Some(OsStr::new(ORIGINAL_CELERY)) {
        return Err("RUST_M3U_REFRESH_ALL_OWNER must be original-celery when configured");
    }
    if native_queue_enabled {
        return Err(
            "Original M3U Refresh All ownership conflicts with Native EPG or version workers",
        );
    }
    let broker = broker
        .and_then(OsStr::to_str)
        .filter(|value| value.trim() == *value && !value.is_empty() && value.len() <= 4096)
        .ok_or("Original M3U Refresh All ownership requires an explicit CELERY_BROKER_URL")?;
    Ok(Some(broker))
}

pub async fn prepare(
    auth: AuthService,
    selected: bool,
    native_queue_enabled: bool,
) -> Result<Option<OriginalM3uRefreshAll>, &'static str> {
    let owner = env::var_os(OWNER);
    let broker = env::var_os("CELERY_BROKER_URL");
    let Some(url) = broker_for_owner(
        selected,
        native_queue_enabled,
        owner.as_deref(),
        broker.as_deref(),
    )?
    else {
        return Ok(None);
    };
    OriginalM3uRefreshAll::connect(auth, url)
        .await
        .map(Some)
        .map_err(|_| "Original M3U Refresh All broker initialization failed (credentials omitted)")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_owner_keeps_default_and_selected_routes_unchanged() {
        let invalid_owner = Some(OsStr::new("unknown"));
        assert_eq!(broker_for_owner(false, false, None, None), Ok(None));
        assert_eq!(broker_for_owner(false, true, None, None), Ok(None));
        assert_eq!(broker_for_owner(true, true, invalid_owner, None), Ok(None));
    }

    #[test]
    fn explicit_original_owner_requires_its_broker_and_no_native_consumer() {
        let owner = Some(OsStr::new(ORIGINAL_CELERY));
        let broker = Some(OsStr::new("redis://owned:6379/4"));
        assert_eq!(
            broker_for_owner(false, false, owner, broker),
            Ok(Some("redis://owned:6379/4"))
        );
        assert!(broker_for_owner(false, true, owner, broker).is_err());
        assert!(broker_for_owner(false, false, owner, None).is_err());
        assert!(broker_for_owner(false, false, owner, Some(OsStr::new(" "))).is_err());
        assert!(broker_for_owner(false, false, Some(OsStr::new("")), broker).is_err());
    }
}
