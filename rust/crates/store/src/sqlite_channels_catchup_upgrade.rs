//! Pinned 0038–0039 under the owned SQLite compatibility authority.
//! Original Dispatcharr and Django contributors retain full credit.
use std::collections::BTreeMap;

use serde_json::Value;

use super::*;

pub(super) const STEPS: [&str; 2] = ["0038_add_catchup_fields", "0039_add_radio_fields"];
const CATALOG: &str = include_str!("../datasets/sqlite-channels-catchup-catalog.json");
const SQL: [&str; 2] = [
    include_str!("../datasets/sqlite-channels-0038-compatible.sql"),
    include_str!("../datasets/sqlite-channels-0039-compatible.sql"),
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
            "SELECT sql FROM sqlite_schema WHERE tbl_name=? AND sql IS NOT NULL ORDER BY type DESC,name LIMIT 17",
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
    let records = sqlx::query_as::<_, (String, String)>(
        "SELECT CASE WHEN length(CAST(app AS BLOB))<=255 THEN app ELSE '' END,
                CASE WHEN length(CAST(name AS BLOB))<=255 THEN name ELSE '' END
         FROM django_migrations ORDER BY app,name LIMIT 180",
    )
    .fetch_all(&mut **tx)
    .await?;
    let prefix = records
        .len()
        .checked_sub(177)
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
            .map(|name| ("core", *name)),
    );
    expected.extend(
        channels_flags::STEPS
            .iter()
            .chain(&channels_identity::STEPS)
            .chain(std::iter::once(&channels_overrides::STEP))
            .chain(&STEPS[..prefix])
            .map(|name| (channels_flags::APP, *name)),
    );
    expected.sort_unstable();
    if !records
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
    for sql in SQL[index].lines() {
        sqlx::query(sql).execute(&mut **tx).await?;
    }
    if index == 0 {
        backfill(tx).await?;
    }
    record(tx, (channels_flags::APP, STEPS[index])).await
}

fn duration(metadata: Option<&str>) -> Result<Option<i32>, Failure> {
    let Some(metadata) = metadata else {
        return Ok(None);
    };
    let value: Value = serde_json::from_str(metadata).map_err(|_| Failure::OutsideKnownState)?;
    let Some(values) = value.as_object() else {
        return Ok(None);
    };
    let enabled = match values.get("tv_archive") {
        Some(Value::Bool(flag)) => *flag,
        Some(Value::Number(n)) => n.as_i64() == Some(1),
        Some(Value::String(s)) => matches!(s.as_str(), "1" | "true" | "True"),
        _ => false,
    };
    if !enabled {
        return Ok(None);
    }
    let raw = match values.get("tv_archive_duration") {
        Some(Value::Number(n)) => n.to_string(),
        Some(Value::String(s)) => s.clone(),
        _ => return Ok(Some(7)),
    };
    if raw.is_empty() || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
        return Ok(Some(7));
    }
    let raw = raw.trim_start_matches('0');
    if raw.is_empty() {
        return Ok(Some(0));
    }
    Ok(Some(raw.parse().map_err(|_| Failure::OutsideKnownState)?))
}

async fn backfill(tx: &mut Transaction<'_, sqlx::Sqlite>) -> Result<(), Failure> {
    let mut previous: Option<i64> = None;
    loop {
        let rows = sqlx::query_as::<_, (i64, Option<String>)>(
            "SELECT id,custom_properties FROM dispatcharr_channels_stream
             WHERE (?1 IS NULL OR id>?1) ORDER BY id LIMIT 32",
        )
        .bind(previous)
        .fetch_all(&mut **tx)
        .await?;
        if rows.is_empty() {
            break;
        }
        for (id, metadata) in rows {
            if let Some(days) = duration(metadata.as_deref())? {
                sqlx::query(
                    "UPDATE dispatcharr_channels_stream SET is_catchup=1,catchup_days=? WHERE id=?",
                )
                .bind(days)
                .bind(id)
                .execute(&mut **tx)
                .await?;
            }
            previous = Some(id);
        }
    }
    sqlx::query(
        "UPDATE dispatcharr_channels_channel AS c SET
           is_catchup=EXISTS(
             SELECT 1 FROM dispatcharr_channels_channelstream cs
             JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id
             WHERE cs.channel_id=c.id AND s.is_catchup=1),
           catchup_days=COALESCE((
             SELECT MAX(s.catchup_days) FROM dispatcharr_channels_channelstream cs
             JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id
             WHERE cs.channel_id=c.id AND s.is_catchup=1),0)",
    )
    .execute(&mut **tx)
    .await?;
    Ok(())
}

#[cfg(test)]
#[path = "sqlite_channels_catchup_contract.rs"]
mod contract;
