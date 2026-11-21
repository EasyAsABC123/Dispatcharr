//! Pinned Original Channels 0031–0032 on detached historical SQLite copies.
//! Original Dispatcharr and Django contributors retain full credit.
use std::collections::BTreeMap;

use super::*;

pub(super) const APP: &str = "dispatcharr_channels";
pub(super) const STEPS: [&str; 2] = [
    "0031_channelgroupm3uaccount_is_stale_and_more",
    "0032_channel_is_adult_stream_is_adult",
];
const CATALOG: &str = include_str!("../datasets/sqlite-channels-flags-catalog.json");
const SQL: [&str; 2] = [
    include_str!("../datasets/sqlite-channels-0031.sql"),
    include_str!("../datasets/sqlite-channels-0032.sql"),
];

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
            Ok(count) => {
                tx.commit().await?;
                Ok(count)
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
    layout: &BTreeMap<String, Vec<String>>,
) -> Result<(), Failure> {
    for (table, expected) in layout {
        let observed = sqlx::query_scalar::<_, String>(
            "SELECT sql FROM sqlite_schema WHERE tbl_name=? AND sql IS NOT NULL
             ORDER BY type DESC,name LIMIT 15",
        )
        .bind(table)
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
         FROM django_migrations ORDER BY app,name LIMIT 173",
    )
    .fetch_all(&mut **tx)
    .await?;
    let prefix = names
        .len()
        .checked_sub(170)
        .filter(|count| *count <= STEPS.len())
        .ok_or(Failure::OutsideKnownState)?;
    let mut expected = HISTORICAL.to_vec();
    expected.extend([KEY, MANAGER]);
    expected.extend(app_data::STEPS);
    expected.extend(core_event_profile::STEPS);
    expected.extend(epg_guide_prefix::STEPS);
    expected.extend(epg_index::STEPS);
    expected.extend(plugins::STEPS);
    expected.push(m3u_expiration::STEP);
    expected.extend(connect::STEPS.iter().map(|name| (connect::APP, *name)));
    expected.extend(core_settings::STEPS);
    expected.extend(
        core_profiles::STEPS
            .iter()
            .chain(&core_final::STEPS)
            .map(|step| ("core", *step)),
    );
    expected.extend(STEPS[..prefix].iter().map(|step| (APP, *step)));
    expected.sort_unstable();
    if !names
        .iter()
        .map(|(app, name)| (app.as_str(), name.as_str()))
        .eq(expected)
    {
        return Err(Failure::OutsideKnownState);
    }
    let catalogs: [BTreeMap<String, Vec<String>>; 3] =
        serde_json::from_str(CATALOG).map_err(|_| Failure::OutsideKnownState)?;
    check_layout(tx, &catalogs[prefix]).await?;
    for (index, catalog) in catalogs.iter().enumerate().skip(prefix + 1) {
        apply_step(tx, index - 1).await?;
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

async fn apply_step(tx: &mut Transaction<'_, sqlx::Sqlite>, index: usize) -> Result<(), Failure> {
    for statement in SQL[index].lines() {
        let command = sqlx::query(statement);
        if statement.ends_with("0, ? FROM \"dispatcharr_channels_channelgroupm3uaccount\"") {
            let now = Utc::now().naive_utc();
            let format = if now.and_utc().timestamp_subsec_micros() == 0 {
                "%Y-%m-%d %H:%M:%S"
            } else {
                "%Y-%m-%d %H:%M:%S%.6f"
            };
            command
                .bind(now.format(format).to_string())
                .execute(&mut **tx)
                .await?;
        } else {
            command.execute(&mut **tx).await?;
        }
    }
    record(tx, (APP, STEPS[index])).await
}

#[cfg(test)]
#[path = "sqlite_channels_flags_contract.rs"]
mod contract;
