//! Pinned Channels 0037 on owned SQLite copies under the separate compatibility authority.
//! Original Dispatcharr and Django contributors retain full credit.
use std::collections::BTreeMap;

use super::*;

pub(super) const STEP: &str = "0037_auto_sync_overhaul";
const CATALOG: &str = include_str!("../datasets/sqlite-channels-overrides-catalog.json");
const SQL: &str = include_str!("../datasets/sqlite-channels-0037-compatible.sql");

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
             ORDER BY type DESC,name LIMIT 16",
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
    let history = sqlx::query_as::<_, (String, String)>(
        "SELECT CASE WHEN length(CAST(app AS BLOB))<=255 THEN app ELSE '' END,
                CASE WHEN length(CAST(name AS BLOB))<=255 THEN name ELSE '' END
         FROM django_migrations ORDER BY app,name LIMIT 178",
    )
    .fetch_all(&mut **tx)
    .await?;
    let applied = match history.len() {
        176 => false,
        177 => true,
        _ => return Err(Failure::OutsideKnownState),
    };
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
            .map(|name| ("core", *name)),
    );
    expected.extend(
        channels_flags::STEPS
            .iter()
            .chain(&channels_identity::STEPS)
            .map(|name| (channels_flags::APP, *name)),
    );
    if applied {
        expected.push((channels_flags::APP, STEP));
    }
    expected.sort_unstable();
    if !history
        .iter()
        .map(|(app, name)| (app.as_str(), name.as_str()))
        .eq(expected)
    {
        return Err(Failure::OutsideKnownState);
    }
    let catalogs: [BTreeMap<String, Vec<String>>; 2] =
        serde_json::from_str(CATALOG).map_err(|_| Failure::OutsideKnownState)?;
    check_layout(tx, &catalogs[usize::from(applied)]).await?;
    if !applied {
        settle_orphans(tx).await?;
        for stmt in SQL.lines() {
            sqlx::query(stmt).execute(&mut **tx).await?;
        }
        check_layout(tx, &catalogs[1]).await?;
        record(tx, (channels_flags::APP, STEP)).await?;
    }
    if sqlx::query("PRAGMA foreign_key_check")
        .fetch_optional(&mut **tx)
        .await?
        .is_some()
    {
        return Err(Failure::OutsideKnownState);
    }
    Ok(usize::from(!applied))
}

async fn settle_orphans(tx: &mut Transaction<'_, sqlx::Sqlite>) -> Result<(), Failure> {
    // Reproduce the pinned Django rule without changing the channel or link identities.
    sqlx::query(
        "WITH ownership AS MATERIALIZED (
            SELECT c.id, CASE WHEN COUNT(DISTINCT s.m3u_account_id)=1
                              THEN MAX(s.m3u_account_id) END AS account
            FROM dispatcharr_channels_channel c
            LEFT JOIN dispatcharr_channels_channelstream cs ON cs.channel_id=c.id
            LEFT JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id
            WHERE c.auto_created=1 AND c.auto_created_by_id IS NULL
            GROUP BY c.id
         )
         UPDATE dispatcharr_channels_channel
         SET auto_created=(SELECT account IS NOT NULL FROM ownership WHERE ownership.id=dispatcharr_channels_channel.id),
             auto_created_by_id=(SELECT account FROM ownership WHERE ownership.id=dispatcharr_channels_channel.id)
         WHERE id IN (SELECT id FROM ownership)",
    )
    .execute(&mut **tx)
    .await?;
    Ok(())
}

#[cfg(test)]
#[path = "sqlite_channels_overrides_contract.rs"]
mod contract;
