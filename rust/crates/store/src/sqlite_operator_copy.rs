//! Explicit operator copy of the verified historical SQLite storage shape.
//! Original Dispatcharr and Django contributors retain full credit.
use std::fmt;
use std::fs::{self, DirBuilder, File};
use std::io;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

use super::*;

type CatalogRow = (String, String, String, String);

const HISTORICAL_CATALOG: &str =
    include_str!("../datasets/sqlite-operator-historical-catalog.json");
const UPGRADED_CATALOG: &str = include_str!("../datasets/sqlite-operator-upgraded-catalog.json");
const SQLITE_SIDECARS: [&str; 3] = ["-journal", "-wal", "-shm"];

#[derive(Debug)]
pub enum UpgradeError {
    Input,
    Busy,
    OutputExists,
    Storage,
    Migration,
}

impl fmt::Display for UpgradeError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str(match self {
            Self::Input => "input must be a detached copy with the verified historical SQLite layout and history",
            Self::Busy => "SQLite input or its related files changed during the snapshot",
            Self::OutputExists => "the output path already exists",
            Self::Storage => "could not prepare or publish the SQLite output copy",
            Self::Migration => "the copy failed migration or validation",
        })
    }
}

fn suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(suffix);
    name.into()
}

fn no_sidecars(path: &Path) -> bool {
    SQLITE_SIDECARS.iter().all(|extension| {
        fs::symlink_metadata(suffix(path, extension))
            .is_err_and(|error| error.kind() == io::ErrorKind::NotFound)
    })
}

fn unchanged_file(path: &Path, original: &fs::Metadata) -> bool {
    fs::symlink_metadata(path).is_ok_and(|observed| {
        observed.file_type().is_file()
            && observed.dev() == original.dev()
            && observed.ino() == original.ino()
            && observed.len() == original.len()
            && observed.mtime() == original.mtime()
            && observed.mtime_nsec() == original.mtime_nsec()
    })
}

struct StagedCopy {
    directory: PathBuf,
    database: PathBuf,
}

impl StagedCopy {
    fn new(parent: &Path) -> Result<Self, UpgradeError> {
        let id = uuid::Uuid::new_v4();
        let directory = parent.join(format!(".dispatcharr-sqlite-upgrade-{id}"));
        DirBuilder::new()
            .mode(0o700)
            .create(&directory)
            .map_err(|_| UpgradeError::Storage)?;
        let database = directory.join("copy.sqlite");
        Ok(Self {
            directory,
            database,
        })
    }
}

impl Drop for StagedCopy {
    fn drop(&mut self) {
        for extension in SQLITE_SIDECARS {
            let _ = fs::remove_file(suffix(&self.database, extension));
        }
        let _ = fs::remove_file(&self.database);
        let _ = fs::remove_dir(&self.directory);
    }
}

async fn open_read_only(path: &Path) -> Result<SqliteConnection, UpgradeError> {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .read_only(true)
        .create_if_missing(false)
        .busy_timeout(Duration::from_secs(1));
    SqliteConnection::connect_with(&options)
        .await
        .map_err(|_| UpgradeError::Input)
}

async fn exact_catalog(
    connection: &mut SqliteConnection,
    catalog: &str,
) -> Result<(), UpgradeError> {
    let expected: Vec<CatalogRow> =
        serde_json::from_str(catalog).map_err(|_| UpgradeError::Storage)?;
    let found = sqlx::query_as::<_, CatalogRow>(
        "SELECT type,name,tbl_name,sql FROM sqlite_schema
         WHERE sql IS NOT NULL AND name NOT GLOB 'sqlite_*' ORDER BY type,name LIMIT ?",
    )
    .bind(i64::try_from(expected.len() + 1).map_err(|_| UpgradeError::Storage)?)
    .fetch_all(connection)
    .await
    .map_err(|_| UpgradeError::Input)?;
    if found != expected {
        return Err(UpgradeError::Input);
    }
    Ok(())
}

async fn historical_history(connection: &mut SqliteConnection) -> Result<(), UpgradeError> {
    let names = sqlx::query_as::<_, (String, String)>(
        "SELECT app,name FROM django_migrations ORDER BY app,name LIMIT 133",
    )
    .fetch_all(connection)
    .await
    .map_err(|_| UpgradeError::Input)?;
    if !names
        .iter()
        .map(|(app, name)| (app.as_str(), name.as_str()))
        .eq(HISTORICAL)
    {
        return Err(UpgradeError::Input);
    }
    Ok(())
}

async fn integrity(connection: &mut SqliteConnection) -> Result<(), UpgradeError> {
    let result: String = sqlx::query_scalar("PRAGMA integrity_check(1)")
        .fetch_one(&mut *connection)
        .await
        .map_err(|_| UpgradeError::Migration)?;
    let foreign_keys = sqlx::query("PRAGMA foreign_key_check")
        .fetch_optional(connection)
        .await
        .map_err(|_| UpgradeError::Migration)?;
    if result != "ok" || foreign_keys.is_some() {
        return Err(UpgradeError::Migration);
    }
    Ok(())
}

async fn snapshot(input: &Path, stage: &Path) -> Result<(), UpgradeError> {
    let original = fs::symlink_metadata(input).map_err(|_| UpgradeError::Input)?;
    if !original.file_type().is_file() || !no_sidecars(input) {
        return Err(UpgradeError::Input);
    }
    let mut source = open_read_only(input).await?;
    let result = async {
        let journal: String = sqlx::query_scalar("PRAGMA journal_mode")
            .fetch_one(&mut source)
            .await
            .map_err(|_| UpgradeError::Input)?;
        if journal != "delete" {
            return Err(UpgradeError::Input);
        }
        exact_catalog(&mut source, HISTORICAL_CATALOG).await?;
        historical_history(&mut source).await?;
        let destination = stage.to_str().ok_or(UpgradeError::Storage)?;
        sqlx::query("VACUUM INTO ?")
            .bind(destination)
            .execute(&mut source)
            .await
            .map_err(|_| UpgradeError::Busy)?;
        if !unchanged_file(input, &original) || !no_sidecars(input) {
            return Err(UpgradeError::Busy);
        }
        Ok(())
    }
    .await;
    let closed = source.close().await.map_err(|_| UpgradeError::Busy);
    result.and(closed)
}

async fn apply_steps(staging: &Path, timezone: &str) -> Result<(), Failure> {
    let mut snapshot = open_read_only(staging)
        .await
        .map_err(|_| Failure::OutsideKnownState)?;
    exact_catalog(&mut snapshot, HISTORICAL_CATALOG)
        .await
        .map_err(|_| Failure::OutsideKnownState)?;
    historical_history(&mut snapshot)
        .await
        .map_err(|_| Failure::OutsideKnownState)?;
    integrity(&mut snapshot)
        .await
        .map_err(|_| Failure::OutsideKnownState)?;
    snapshot.close().await?;

    super::run(staging).await?;
    app_data::run(staging).await?;
    core_event_profile::run(staging).await?;
    epg_guide_prefix::run(staging).await?;
    epg_index::run(staging).await?;
    plugins::run(staging).await?;
    m3u_expiration::run(staging).await?;
    connect::run(staging).await?;
    core_settings::run(staging, timezone).await?;
    core_profiles::run(staging).await?;
    core_final::run(staging).await?;
    channels_flags::run(staging).await?;
    channels_identity::run(staging).await?;
    channels_overrides::run(staging).await?;
    channels_catchup::run(staging).await?;
    channels_relations::run(staging).await?;
    vod::run(staging).await?;
    if vod::run(staging).await? != 0 {
        return Err(Failure::OutsideKnownState);
    }
    let mut upgraded = open_read_only(staging)
        .await
        .map_err(|_| Failure::OutsideKnownState)?;
    exact_catalog(&mut upgraded, UPGRADED_CATALOG)
        .await
        .map_err(|_| Failure::OutsideKnownState)?;
    integrity(&mut upgraded)
        .await
        .map_err(|_| Failure::OutsideKnownState)?;
    upgraded.close().await?;
    Ok(())
}

/// Create the completed compatibility copy at an explicitly absent output path.
pub async fn upgrade_copy(input: &Path, output: &Path, timezone: &str) -> Result<(), UpgradeError> {
    match fs::symlink_metadata(output) {
        Ok(_) => return Err(UpgradeError::OutputExists),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(_) => return Err(UpgradeError::Storage),
    }
    let directory = output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let directory = directory
        .canonicalize()
        .map_err(|_| UpgradeError::Storage)?;
    let output = directory.join(output.file_name().ok_or(UpgradeError::Storage)?);
    if !no_sidecars(&output) {
        return Err(UpgradeError::OutputExists);
    }
    let staged = StagedCopy::new(&directory)?;
    snapshot(input, &staged.database).await?;
    fs::set_permissions(&staged.database, fs::Permissions::from_mode(0o600))
        .map_err(|_| UpgradeError::Storage)?;
    apply_steps(&staged.database, timezone)
        .await
        .map_err(|_| UpgradeError::Migration)?;
    if !no_sidecars(&staged.database) {
        return Err(UpgradeError::Storage);
    }
    File::open(&staged.database)
        .and_then(|file| file.sync_all())
        .map_err(|_| UpgradeError::Storage)?;
    if !no_sidecars(&output) {
        return Err(UpgradeError::OutputExists);
    }
    fs::hard_link(&staged.database, output).map_err(|error| {
        if error.kind() == io::ErrorKind::AlreadyExists {
            UpgradeError::OutputExists
        } else {
            UpgradeError::Storage
        }
    })?;
    Ok(())
}
