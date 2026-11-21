//! Pinned historical VOD 0003–0006 under the owned SQLite compatibility authority.
//! Original Dispatcharr and Django contributors retain full credit.
use std::collections::{BTreeMap, HashSet};

use serde_json::Value;

use super::*;

const STEPS: [&str; 4] = [
    "0003_vodlogo_alter_movie_logo_alter_series_logo",
    "0004_m3uepisoderelation_series_relation",
    "0005_movie_is_adult",
    "0006_movie_series_language",
];
const CATALOG: &str = include_str!("../datasets/sqlite-vod-upgrade-catalog.json");
const LOGO: &str = include_str!("../datasets/sqlite-vod-logo-table.sql");
const SQL: [&str; 4] = [
    include_str!("../datasets/sqlite-vod-0003.sql"),
    include_str!("../datasets/sqlite-vod-0004.sql"),
    include_str!("../datasets/sqlite-vod-0005.sql"),
    include_str!("../datasets/sqlite-vod-0006.sql"),
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
    expected: &BTreeMap<String, Vec<String>>,
) -> Result<(), Failure> {
    for (table, catalog) in expected {
        let found = sqlx::query_scalar::<_, String>(
            "SELECT sql FROM sqlite_schema WHERE tbl_name=? AND sql IS NOT NULL ORDER BY type DESC,name LIMIT 20",
        )
        .bind(table)
        .fetch_all(&mut **tx)
        .await?;
        if found != *catalog {
            return Err(Failure::OutsideKnownState);
        }
    }
    Ok(())
}

async fn execute(tx: &mut Transaction<'_, sqlx::Sqlite>) -> Result<usize, Failure> {
    let names = sqlx::query_as::<_, (String, String)>(
        "SELECT CASE WHEN length(CAST(app AS BLOB))<=255 THEN app ELSE '' END,
                CASE WHEN length(CAST(name AS BLOB))<=255 THEN name ELSE '' END
         FROM django_migrations ORDER BY app,name LIMIT 185",
    )
    .fetch_all(&mut **tx)
    .await?;
    let prefix = names
        .len()
        .checked_sub(180)
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
            .chain(&channels_catchup::STEPS)
            .chain(std::iter::once(&channels_relations::STEP))
            .map(|name| (channels_flags::APP, *name)),
    );
    expected.extend(STEPS[..prefix].iter().map(|name| ("vod", *name)));
    expected.sort_unstable();
    if !names
        .iter()
        .map(|(app, name)| (app.as_str(), name.as_str()))
        .eq(expected)
    {
        return Err(Failure::OutsideKnownState);
    }
    let catalogs: [BTreeMap<String, Vec<String>>; 5] =
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
    if index == 0 {
        remap_logos(tx).await?;
    }
    for stmt in SQL[index].lines() {
        sqlx::query(stmt).execute(&mut **tx).await?;
    }
    match index {
        0 => {
            sqlx::query(
                "DELETE FROM dispatcharr_channels_logo
                 WHERE id IN (
                     SELECT l.id FROM dispatcharr_channels_logo l
                     INNER JOIN vod_vodlogo v ON l.url=v.url
                     LEFT JOIN dispatcharr_channels_channel c ON c.logo_id=l.id
                     WHERE c.id IS NULL
                 )",
            )
            .execute(&mut **tx)
            .await?;
        }
        2 => backfill_adult(tx).await?,
        _ => {}
    }
    record(tx, ("vod", STEPS[index])).await
}

async fn remap_logos(tx: &mut Transaction<'_, sqlx::Sqlite>) -> Result<(), Failure> {
    sqlx::query(LOGO.trim_end()).execute(&mut **tx).await?;
    sqlx::query(
        "INSERT INTO vod_vodlogo(name,url)
         SELECT DISTINCT l.name,l.url FROM dispatcharr_channels_logo l
         WHERE l.id IN (
             SELECT DISTINCT logo_id FROM vod_movie WHERE logo_id IS NOT NULL
             UNION SELECT DISTINCT logo_id FROM vod_series WHERE logo_id IS NOT NULL
         )
         ON CONFLICT(url) DO NOTHING",
    )
    .execute(&mut **tx)
    .await?;
    for stmt in [
        "UPDATE vod_movie SET logo_id=(
             SELECT v.id FROM dispatcharr_channels_logo l
             JOIN vod_vodlogo v ON v.url=l.url WHERE l.id=vod_movie.logo_id
         ) WHERE logo_id IN (
             SELECT l.id FROM dispatcharr_channels_logo l JOIN vod_vodlogo v ON v.url=l.url
         )",
        "UPDATE vod_series SET logo_id=(
             SELECT v.id FROM dispatcharr_channels_logo l
             JOIN vod_vodlogo v ON v.url=l.url WHERE l.id=vod_series.logo_id
         ) WHERE logo_id IN (
             SELECT l.id FROM dispatcharr_channels_logo l JOIN vod_vodlogo v ON v.url=l.url
         )",
    ] {
        sqlx::query(stmt).execute(&mut **tx).await?;
    }
    Ok(())
}

fn from_property(raw: &str) -> Result<bool, Failure> {
    let custom: Value = serde_json::from_str(raw).map_err(|_| Failure::OutsideKnownState)?;
    if !crate::channels::truthy(Some(&custom)) {
        return Ok(false);
    }
    let custom = custom.as_object().ok_or(Failure::OutsideKnownState)?;
    let info = custom.get("basic_data");
    if !crate::channels::truthy(info) {
        return Ok(false);
    }
    let indicator = info
        .and_then(Value::as_object)
        .ok_or(Failure::OutsideKnownState)?
        .get("is_adult");
    Ok(indicator
        .and_then(|value| crate::json_compat::python_json_int(value).ok())
        .and_then(|value| value.as_i64())
        == Some(1))
}

async fn backfill_adult(tx: &mut Transaction<'_, sqlx::Sqlite>) -> Result<(), Failure> {
    let mut last: Option<i64> = None;
    let mut selected = HashSet::new();
    loop {
        let rows = sqlx::query_as::<_, (i64, i64, String)>(
            "SELECT id,movie_id,custom_properties FROM vod_m3umovierelation
             WHERE custom_properties IS NOT NULL AND (?1 IS NULL OR id>?1) ORDER BY id LIMIT 32",
        )
        .bind(last)
        .fetch_all(&mut **tx)
        .await?;
        if rows.is_empty() {
            break;
        }
        for (id, film, props) in rows {
            if from_property(&props)? {
                selected.insert(film);
            }
            last = Some(id);
        }
    }
    for film in selected {
        sqlx::query("UPDATE vod_movie SET is_adult=1 WHERE id=?")
            .bind(film)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "sqlite_vod_upgrade_contract.rs"]
mod contract;
