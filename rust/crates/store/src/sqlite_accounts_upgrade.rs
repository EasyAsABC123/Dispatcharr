//! Private copied-file executor for the first two pending Original account migrations.
//! Original Dispatcharr and Django authors retain full credit.
use chrono::Utc;
use sqlx::{sqlite::SqliteConnectOptions, Connection, SqliteConnection, Transaction};
use std::{path::Path, time::Duration};

const HISTORICAL: [(&str, &str); 132] = include!("../datasets/sqlite-v0112-planned-prefix.in");
const KEY: (&str, &str) = ("accounts", "0004_user_api_key");
const MANAGER: (&str, &str) = ("accounts", "0005_alter_user_managers");
const ADD_KEY: &str = r#"ALTER TABLE "accounts_user" ADD COLUMN "api_key" varchar(200) NULL"#;
const KEY_INDEX: &str =
    r#"CREATE INDEX "accounts_user_api_key_dd94a1a5" ON "accounts_user" ("api_key")"#;
const RECORDER: &str = "INSERT INTO django_migrations(app,name,applied) VALUES(?,?,?)";

#[derive(Debug, PartialEq)]
pub(super) enum Upgrade {
    AppliedBoth,
    AppliedManager,
    Unchanged,
}

#[derive(Debug)]
pub(super) enum Failure {
    OutsideKnownState,
    Database(sqlx::Error),
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::OutsideKnownState => "SQLite Accounts history is outside the supported state",
            Self::Database(_) => "SQLite Accounts migration failed",
        })
    }
}

impl std::error::Error for Failure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::OutsideKnownState => None,
            Self::Database(error) => Some(error),
        }
    }
}

impl From<sqlx::Error> for Failure {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

/// Run against the caller's isolated working copy. This prototype has no
/// Main entry point and does not open or write the retained fixture master.
pub(super) async fn run(path: &Path) -> Result<Upgrade, Failure> {
    let metadata = std::fs::symlink_metadata(path).map_err(|_| Failure::OutsideKnownState)?;
    if !metadata.file_type().is_file() {
        return Err(Failure::OutsideKnownState);
    }
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(false)
        .foreign_keys(true)
        .busy_timeout(Duration::from_secs(1));
    let mut conn = SqliteConnection::connect_with(&options).await?;
    let result = async {
        let mut transaction = conn.begin_with("BEGIN IMMEDIATE").await?;
        let result = perform(&mut transaction).await;
        if result.is_ok() {
            transaction.commit().await?;
        } else {
            transaction.rollback().await?;
        }
        result
    }
    .await;
    let close = conn.close().await;
    close?;
    result
}

async fn perform(transaction: &mut Transaction<'_, sqlx::Sqlite>) -> Result<Upgrade, Failure> {
    let names: Vec<(String, String)> = sqlx::query_as(
        "SELECT CASE WHEN length(CAST(app AS BLOB))<=255 THEN app ELSE '' END,
                CASE WHEN length(CAST(name AS BLOB))<=255 THEN name ELSE '' END
         FROM django_migrations LIMIT 135",
    )
    .fetch_all(&mut **transaction)
    .await?;
    let mut observed: Vec<_> = names
        .iter()
        .map(|(app, name)| (app.as_str(), name.as_str()))
        .collect();
    let has_key = observed.contains(&KEY);
    let has_manager = observed.contains(&MANAGER);
    let mut expected = HISTORICAL.to_vec();
    if has_key {
        expected.push(KEY);
    }
    if has_manager {
        if !has_key {
            return Err(Failure::OutsideKnownState);
        }
        expected.push(MANAGER);
    }
    expected.sort_unstable();
    observed.sort_unstable();
    if observed != expected {
        return Err(Failure::OutsideKnownState);
    }
    let existing_type: Option<(String, i64, Option<String>)> = sqlx::query_as(
        r#"SELECT type, "notnull", dflt_value FROM pragma_table_info('accounts_user') WHERE name='api_key'"#,
    )
    .fetch_optional(&mut **transaction)
    .await?;
    if has_key {
        let index: Option<String> = sqlx::query_scalar(
            "SELECT sql FROM sqlite_schema WHERE type='index' AND tbl_name='accounts_user'
             AND name='accounts_user_api_key_dd94a1a5'",
        )
        .fetch_optional(&mut **transaction)
        .await?;
        if existing_type
            .as_ref()
            .map(|(name, not_null, default)| (name.as_str(), *not_null, default.as_deref()))
            != Some(("varchar(200)", 0, None))
            || index.as_deref() != Some(KEY_INDEX)
        {
            return Err(Failure::OutsideKnownState);
        }
    } else {
        if existing_type.is_some() {
            return Err(Failure::OutsideKnownState);
        }
        sqlx::query(ADD_KEY).execute(&mut **transaction).await?;
        sqlx::query(KEY_INDEX).execute(&mut **transaction).await?;
        record(transaction, KEY).await?;
    }
    if has_manager {
        return Ok(Upgrade::Unchanged);
    }
    // AlterModelManagers changes Django's in-memory migration state only.
    record(transaction, MANAGER).await?;
    Ok(if has_key {
        Upgrade::AppliedManager
    } else {
        Upgrade::AppliedBoth
    })
}

async fn record(
    transaction: &mut Transaction<'_, sqlx::Sqlite>,
    (app, name): (&str, &str),
) -> Result<(), Failure> {
    // Django's SQLite timezone-aware recorder stores a naive UTC timestamp.
    let now = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
    let applied = now.trim_end_matches('Z').replace('T', " ");
    sqlx::query(RECORDER)
        .bind(app)
        .bind(name)
        .bind(applied)
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

#[cfg(test)]
#[cfg(test)]
#[path = "sqlite_accounts_upgrade_contract.rs"]
mod contract;

#[path = "sqlite_accounts_appdata_upgrade.rs"]
mod app_data;

#[path = "sqlite_core_event_profile_upgrade.rs"]
mod core_event_profile;

#[path = "sqlite_epg_guide_prefix_upgrade.rs"]
mod epg_guide_prefix;

#[path = "sqlite_epg_index_upgrade.rs"]
mod epg_index;

#[path = "sqlite_plugins_upgrade.rs"]
mod plugins;

#[path = "sqlite_m3u_expiration_upgrade.rs"]
mod m3u_expiration;

#[path = "sqlite_connect_upgrade.rs"]
mod connect;

#[path = "sqlite_core_settings_upgrade.rs"]
mod core_settings;

#[path = "sqlite_core_profiles_upgrade.rs"]
mod core_profiles;

#[path = "sqlite_core_final_upgrade.rs"]
mod core_final;

#[path = "sqlite_channels_flags_upgrade.rs"]
mod channels_flags;

#[path = "sqlite_channels_identity_upgrade.rs"]
mod channels_identity;

#[path = "sqlite_channels_overrides_upgrade.rs"]
mod channels_overrides;

#[path = "sqlite_channels_catchup_upgrade.rs"]
mod channels_catchup;

#[path = "sqlite_channels_relations_upgrade.rs"]
mod channels_relations;

#[path = "sqlite_vod_upgrade.rs"]
mod vod;

#[cfg(feature = "sqlite-historical-upgrade")]
#[path = "sqlite_operator_copy.rs"]
pub mod operator_copy;
