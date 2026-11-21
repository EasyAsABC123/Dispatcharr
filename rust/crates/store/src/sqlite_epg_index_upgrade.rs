//! Pinned Original EPG 0024–0028 on an owned historical SQLite copy.
//! Original Dispatcharr and Django contributors retain full credit.
use serde_json::Value;
use std::collections::BTreeMap;

use super::*;

pub(super) const STEPS: [(&str, &str); 5] = [
    (
        "epg",
        "0024_remove_epgsource_api_key_epgsource_password_and_more",
    ),
    ("epg", "0025_programdata_epg_id_index"),
    ("epg", "0026_epgsourceindex"),
    ("epg", "0027_programdata_epg_start_end_index"),
    ("epg", "0028_programdata_epg_end_index"),
];
const CATALOG: &str = include_str!("../datasets/sqlite-epg-index-catalog.json");
const SQL: [&str; 5] = [
    include_str!("../datasets/sqlite-epg-0024.sql"),
    include_str!("../datasets/sqlite-epg-0025.sql"),
    include_str!("../datasets/sqlite-epg-0026.sql"),
    include_str!("../datasets/sqlite-epg-0027.sql"),
    include_str!("../datasets/sqlite-epg-0028.sql"),
];

pub(super) async fn run(path: &Path) -> Result<usize, Failure> {
    if !std::fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_file()) {
        return Err(Failure::OutsideKnownState);
    }
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(false)
        .foreign_keys(true)
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
             ORDER BY type DESC,name LIMIT 7",
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
         FROM django_migrations ORDER BY app,name LIMIT 149",
    )
    .fetch_all(&mut **tx)
    .await?;
    let prefix = names
        .len()
        .checked_sub(143)
        .filter(|n| *n <= STEPS.len())
        .ok_or(Failure::OutsideKnownState)?;
    let mut expected = HISTORICAL.to_vec();
    expected.extend([KEY, MANAGER]);
    expected.extend(app_data::STEPS);
    expected.extend(core_event_profile::STEPS);
    expected.extend(epg_guide_prefix::STEPS);
    expected.extend(&STEPS[..prefix]);
    expected.sort_unstable();
    if !names
        .iter()
        .map(|(app, name)| (app.as_str(), name.as_str()))
        .eq(expected)
    {
        return Err(Failure::OutsideKnownState);
    }
    let catalogs: [BTreeMap<String, Vec<String>>; 6] =
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
        sqlx::query(statement).execute(&mut **tx).await?;
    }
    if STEPS[index].1 == "0026_epgsourceindex" {
        move_indexes(tx).await?;
        sqlx::query(r#"ALTER TABLE "epg_epgsource" DROP COLUMN "programme_index""#)
            .execute(&mut **tx)
            .await?;
    }
    record(tx, STEPS[index]).await
}

async fn move_indexes(tx: &mut Transaction<'_, sqlx::Sqlite>) -> Result<(), Failure> {
    let mut after: Option<i64> = None;
    loop {
        let rows = sqlx::query_as::<_, (i64, String)>(
            "SELECT id,programme_index FROM epg_epgsource
             WHERE programme_index IS NOT NULL AND (?1 IS NULL OR id>?1) ORDER BY id LIMIT 32",
        )
        .bind(after)
        .fetch_all(&mut **tx)
        .await?;
        if rows.is_empty() {
            return Ok(());
        }
        for (source, raw) in rows {
            let value: Value =
                serde_json::from_str(&raw).map_err(|_| Failure::OutsideKnownState)?;
            // Django excludes SQL NULL before decoding. Stored JSON null is
            // decoded as None and saved as SQL NULL in the new nullable field.
            let storage = if value.is_null() {
                None
            } else {
                let compact = crate::json_compat::compact_json_bytes(&value)
                    .map_err(|_| Failure::OutsideKnownState)?;
                Some(app_data::django_storage_json(&compact)?)
            };
            let now = Utc::now().naive_utc();
            let layout = if now.and_utc().timestamp_subsec_micros() == 0 {
                "%Y-%m-%d %H:%M:%S"
            } else {
                "%Y-%m-%d %H:%M:%S%.6f"
            };
            sqlx::query("INSERT INTO epg_epgsourceindex(source_id,data,updated_at) VALUES(?,?,?)")
                .bind(source)
                .bind(storage)
                .bind(now.format(layout).to_string())
                .execute(&mut **tx)
                .await?;
            after = Some(source);
        }
    }
}

#[cfg(test)]
#[path = "sqlite_epg_index_contract.rs"]
mod contract;
