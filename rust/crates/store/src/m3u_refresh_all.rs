//! All-active M3U refresh: Original Dispatcharr/Django/Celery authors retain credit.
//! Private integration draft; this module does not claim or settle broker tasks.
use crate::{AuthService, StoreError};
use dispatcharr_contracts::BoxFuture;
use serde_json::Value;
use std::{
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    sync::Arc,
};
use uuid::Uuid;

type Owner = Arc<dyn Send + Sync>;
fn unsupported(reason: &'static str) -> StoreError {
    StoreError::Unsupported(reason)
}
#[derive(Debug)]
struct PluginIdentity {
    configured: std::path::PathBuf,
    path: std::path::PathBuf,
    held: std::fs::File,
    dev: u64,
    ino: u64,
}
impl PluginIdentity {
    fn observe() -> Result<Self, StoreError> {
        let configured =
            std::env::var("DISPATCHARR_PLUGINS_DIR").unwrap_or_else(|_| "/data/plugins".into());
        if configured.is_empty() || configured.len() > 4096 {
            return Err(unsupported("Aggregate M3U plugin path capacity"));
        }
        let path = std::fs::canonicalize(&configured)
            .map_err(|_| unsupported("Aggregate M3U plugin directory unavailable"))?;
        let held = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
            .open(&path)
            .map_err(|_| unsupported("Aggregate M3U plugin directory identity unavailable"))?;
        let meta = held
            .metadata()
            .map_err(|_| unsupported("Aggregate M3U plugin directory metadata unavailable"))?;
        let value = Self {
            configured: configured.into(),
            path,
            held,
            dev: meta.dev(),
            ino: meta.ino(),
        };
        value.recheck()?;
        Ok(value)
    }
    fn recheck(&self) -> Result<(), StoreError> {
        let configured = std::fs::canonicalize(&self.configured)
            .map_err(|_| unsupported("Aggregate M3U plugin path changed"))?;
        let before = std::fs::symlink_metadata(&self.path)
            .map_err(|_| unsupported("Aggregate M3U plugin directory changed"))?;
        let held = self
            .held
            .metadata()
            .map_err(|_| unsupported("Aggregate M3U plugin handle unavailable"))?;
        if configured != self.path
            || before.file_type().is_symlink()
            || !before.is_dir()
            || (before.dev(), before.ino()) != (self.dev, self.ino)
            || (held.dev(), held.ino()) != (self.dev, self.ino)
        {
            return Err(unsupported("Aggregate M3U plugin namespace changed"));
        }
        if std::fs::read_dir(&self.path)
            .map_err(|_| unsupported("Aggregate M3U plugin directory read unavailable"))?
            .next()
            .is_some()
        {
            return Err(unsupported(
                "Aggregate M3U general Python plugin hooks require adapter",
            ));
        }
        let after = std::fs::symlink_metadata(&self.path)
            .map_err(|_| unsupported("Aggregate M3U plugin namespace changed"))?;
        if (after.dev(), after.ino()) != (self.dev, self.ino) {
            return Err(unsupported("Aggregate M3U plugin namespace changed"));
        }
        Ok(())
    }
}
async fn observe_plugins(owner: Owner) -> Result<PluginIdentity, StoreError> {
    tokio::task::spawn_blocking(move || {
        let _owner = owner;
        PluginIdentity::observe()
    })
    .await
    .map_err(|_| unsupported("Aggregate M3U plugin observation unsettled"))?
}

const MAX_ACCOUNTS: usize = 4096;
// Source's queryset has no model ordering. Do not invent ID/name/type ordering,
// nor exclude the active custom account, locked accounts, STD or XC accounts.
const ACTIVE: &str = "SELECT id::bigint FROM m3u_m3uaccount WHERE is_active LIMIT 4097";

#[cfg(test)]
pub(crate) fn schema_active_selection() -> &'static str {
    ACTIVE
}

/// Bind to the ACTUAL claimed aggregate frame's root/parent identity. The
/// implementation must record acknowledged children before returning, and
/// retain an uncertain publication's UUID/owner without fabricating a receipt.
pub trait ChildAdmission: Send + Sync {
    fn admit_single(&self, account: i64, owner: Owner)
        -> BoxFuture<'_, Result<(), AdmissionError>>;
}
pub trait RefreshChildPublisher: Send + Sync {
    fn publish_single(&self, account: i64, owner: Owner)
        -> BoxFuture<'_, Result<Uuid, StoreError>>;
}

/// Read capacity before CAS only; caller still owns its unclaimed raw head.
/// The execution query is deliberately fresh, as Source evaluates its queryset
/// in the claimed task rather than freezing API-time account membership.
pub struct Prepared {
    plugin: PluginIdentity,
}
pub enum ExecutionError {
    Operational(StoreError),
    RaisedSql(sqlx::Error),
}
impl From<StoreError> for ExecutionError {
    fn from(error: StoreError) -> Self {
        Self::Operational(error)
    }
}
pub enum AdmissionError {
    Draining,
    Unsupported,
    Operational(StoreError),
}
impl From<StoreError> for AdmissionError {
    fn from(value: StoreError) -> Self {
        Self::Operational(value)
    }
}

pub async fn require_supported_domain(auth: &AuthService, owner: Owner) -> Result<(), StoreError> {
    let _ = observe_plugins(owner.clone()).await?;
    if no_python_plugins(auth, owner).await? {
        return Err(unsupported(
            "Aggregate M3U Python plugin task replacements require adapter",
        ));
    }
    Ok(())
}
async fn no_python_plugins(auth: &AuthService, owner: Owner) -> Result<bool, StoreError> {
    let session = auth.borrow_task_sql_session(owner).await?;
    let observed = session
        .run(|c| {
            Box::pin(async move {
                sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM plugins_pluginconfig)")
                    .fetch_one(c)
                    .await
            })
        })
        .await;
    session.close().await?;
    Ok(observed??)
}
pub async fn preflight(
    auth: &AuthService,
    children: &dyn ChildAdmission,
    owner: Owner,
) -> Result<Prepared, AdmissionError> {
    let plugin = observe_plugins(owner.clone())
        .await
        .map_err(|error| match error {
            StoreError::Unsupported(_) => AdmissionError::Unsupported,
            other => AdmissionError::Operational(other),
        })?;
    if no_python_plugins(auth, owner.clone()).await? {
        return Err(AdmissionError::Unsupported);
    }
    let accounts = accounts(auth, owner.clone())
        .await
        .map_err(|error| match error {
            ExecutionError::Operational(error) => AdmissionError::Operational(error),
            ExecutionError::RaisedSql(error) => {
                AdmissionError::Operational(StoreError::Database(error))
            }
        })?;
    if accounts.len() > MAX_ACCOUNTS {
        return Err(AdmissionError::Unsupported);
    }
    for account in accounts {
        children.admit_single(account, owner.clone()).await?;
    }
    Ok(Prepared { plugin })
}

async fn accounts(auth: &AuthService, owner: Owner) -> Result<Vec<i64>, ExecutionError> {
    let session = auth.borrow_task_sql_session(owner).await?;
    let observed = session
        .run(|connection| {
            Box::pin(async move {
                sqlx::query_scalar::<_, i64>(ACTIVE)
                    .fetch_all(connection)
                    .await
            })
        })
        .await;
    // Physical session-close failure takes precedence even over a known query
    // error. The worker must not publish a successful task terminal in that case.
    session.close().await?;
    let accounts = observed?.map_err(ExecutionError::RaisedSql)?;
    Ok(accounts)
}

/// Invoke ONLY after exact broker CAS and current task-domain admission.
/// The caller's publisher carries real child lineage; no SQL lease spans Redis
/// publication. A publication failure does not undo earlier actual children.
/// Only query errors observed before physical close are eligible for the
/// worker's grounded Source failure serializer. All close/actor errors remain
/// operational regardless of SQLSTATE; no successful terminal can hide them.
pub async fn execute(
    auth: &AuthService,
    publisher: &dyn RefreshChildPublisher,
    admission: &dyn ChildAdmission,
    prepared: Prepared,
    owner: Owner,
) -> Result<Value, ExecutionError> {
    let plugin = prepared.plugin;
    let retained = owner.clone();
    tokio::task::spawn_blocking(move || {
        let _owner = retained;
        plugin.recheck()
    })
    .await
    .map_err(|_| unsupported("Aggregate M3U plugin recheck unsettled"))??;
    if no_python_plugins(auth, owner.clone()).await? {
        return Err(unsupported("Aggregate M3U Python plugin configuration changed").into());
    }
    let accounts = accounts(auth, owner.clone()).await?;
    if accounts.len() > MAX_ACCOUNTS {
        return Err(unsupported("Aggregate M3U account capacity changed").into());
    }
    // Validate the WHOLE fresh Source membership before the first publication.
    // Never skip a Source account to make a supported subset appear successful.
    for account in &accounts {
        admission
            .admit_single(*account, owner.clone())
            .await
            .map_err(|error| match error {
                AdmissionError::Draining => ExecutionError::Operational(unsupported(
                    "Aggregate M3U claimed admission interrupted by drain",
                )),
                AdmissionError::Unsupported => ExecutionError::Operational(unsupported(
                    "Aggregate M3U child domain changed after claim",
                )),
                AdmissionError::Operational(error) => ExecutionError::Operational(error),
            })?;
    }
    let mut count = 0usize;
    for account in accounts {
        let _actual_child_id = publisher.publish_single(account, owner.clone()).await?;
        count += 1;
    }
    Ok(Value::String(format!(
        "Queued M3U refresh for {count} active account(s)."
    )))
}
