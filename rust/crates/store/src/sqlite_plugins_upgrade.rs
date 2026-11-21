//! Pinned Original Plugins 0002–0004 on an owned historical SQLite copy.
//! Original Dispatcharr and Django contributors retain full credit.
use std::collections::BTreeMap;

use super::*;

pub(super) const STEPS: [(&str, &str); 3] = [
    ("plugins", "0002_pluginrepo"),
    ("plugins", "0003_update_official_repo_url"),
    ("plugins", "0004_plugin_schedule_ownership"),
];
const CATALOG: &str = include_str!("../datasets/sqlite-plugins-upgrade-catalog.json");
const REPO_OLD: &str =
    "https://raw.githubusercontent.com/Dispatcharr/Plugins/releases/manifest.json";
const REPO_FINAL: &str = "https://dispatcharr.github.io/Plugins/manifest.json";

pub(super) async fn run(path: &Path) -> Result<usize, Failure> {
    if !std::fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_file()) {
        return Err(Failure::OutsideKnownState);
    }
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(false)
        .foreign_keys(false)
        .busy_timeout(Duration::from_secs(1));
    let mut connection = SqliteConnection::connect_with(&options).await?;
    let result = async {
        let mut tx = connection.begin_with("BEGIN IMMEDIATE").await?;
        match execute(&mut tx).await {
            Ok(applied) => {
                tx.commit().await?;
                Ok(applied)
            }
            Err(error) => {
                tx.rollback().await?;
                Err(error)
            }
        }
    }
    .await;
    connection.close().await?;
    result
}

async fn check_layout(
    tx: &mut Transaction<'_, sqlx::Sqlite>,
    tables: &BTreeMap<String, Vec<String>>,
) -> Result<(), Failure> {
    for (name, expected) in tables {
        let observed = sqlx::query_scalar::<_, String>(
            "SELECT sql FROM sqlite_schema WHERE tbl_name=? AND sql IS NOT NULL
             ORDER BY type DESC,name LIMIT 4",
        )
        .bind(name)
        .fetch_all(&mut **tx)
        .await?;
        if observed != *expected {
            return Err(Failure::OutsideKnownState);
        }
    }
    Ok(())
}

async fn execute(tx: &mut Transaction<'_, sqlx::Sqlite>) -> Result<usize, Failure> {
    let names = sqlx::query_as::<_, (String, String)>(
        "SELECT CASE WHEN length(CAST(app AS BLOB))<=255 THEN app ELSE '' END,
                CASE WHEN length(CAST(name AS BLOB))<=255 THEN name ELSE '' END
         FROM django_migrations ORDER BY app,name LIMIT 152",
    )
    .fetch_all(&mut **tx)
    .await?;
    let prefix = names
        .len()
        .checked_sub(148)
        .filter(|n| *n <= STEPS.len())
        .ok_or(Failure::OutsideKnownState)?;
    let mut expected = HISTORICAL.to_vec();
    expected.extend([KEY, MANAGER]);
    expected.extend(app_data::STEPS);
    expected.extend(core_event_profile::STEPS);
    expected.extend(epg_guide_prefix::STEPS);
    expected.extend(epg_index::STEPS);
    expected.extend(&STEPS[..prefix]);
    expected.sort_unstable();
    if !names
        .iter()
        .map(|(app, name)| (app.as_str(), name.as_str()))
        .eq(expected)
    {
        return Err(Failure::OutsideKnownState);
    }
    let catalogs = catalogs()?;
    check_layout(tx, &catalogs[prefix]).await?;
    for (index, catalog) in catalogs.iter().enumerate().skip(prefix + 1) {
        apply_step(tx, index - 1, &catalogs).await?;
        check_layout(tx, catalog).await?;
    }
    if sqlx::query("PRAGMA foreign_key_check")
        .fetch_optional(&mut **tx)
        .await?
        .is_some()
    {
        return Err(Failure::OutsideKnownState);
    }
    Ok(STEPS.len() - prefix)
}

fn catalogs() -> Result<[BTreeMap<String, Vec<String>>; 4], Failure> {
    serde_json::from_str(CATALOG).map_err(|_| Failure::OutsideKnownState)
}

fn clock() -> String {
    let now = Utc::now().naive_utc();
    let layout = if now.and_utc().timestamp_subsec_micros() == 0 {
        "%Y-%m-%d %H:%M:%S"
    } else {
        "%Y-%m-%d %H:%M:%S%.6f"
    };
    now.format(layout).to_string()
}

async fn apply_step(
    tx: &mut Transaction<'_, sqlx::Sqlite>,
    index: usize,
    catalogs: &[BTreeMap<String, Vec<String>>; 4],
) -> Result<(), Failure> {
    let statements = match index {
        0 => {
            sqlx::query(&catalogs[1]["plugins_pluginrepo"][0])
                .execute(&mut **tx)
                .await?;
            sqlx::query("INSERT INTO plugins_pluginrepo(name,url,is_official,enabled,cached_manifest,last_fetched,public_key,signature_verified,last_fetch_status,created_at,updated_at)
                         VALUES('Dispatcharr Official',?,1,1,'{}',NULL,'',NULL,'',?,?)")
                .bind(REPO_OLD)
                .bind(clock())
                .bind(clock())
                .execute(&mut **tx)
                .await?;
            include_str!("../datasets/sqlite-plugins-0002.sql")
        }
        1 => {
            sqlx::query("UPDATE plugins_pluginrepo SET url=? WHERE is_official=1")
                .bind(REPO_FINAL)
                .execute(&mut **tx)
                .await?;
            ""
        }
        2 => include_str!("../datasets/sqlite-plugins-0004.sql"),
        _ => unreachable!(),
    };
    for statement in statements.lines() {
        sqlx::query(statement).execute(&mut **tx).await?;
    }
    record(tx, STEPS[index]).await
}

#[cfg(test)]
#[path = "sqlite_plugins_upgrade_contract.rs"]
mod contract;
