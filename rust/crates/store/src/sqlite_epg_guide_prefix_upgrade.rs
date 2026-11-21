//! Pinned Original EPG 0021–0023 on an owned historical SQLite copy.
//! Original Dispatcharr and Django contributors retain full credit.
use super::*;

pub(super) const STEPS: [(&str, &str); 3] = [
    ("epg", "0021_epgsource_priority"),
    ("epg", "0022_alter_epgdata_name"),
    ("epg", "0023_epgsource_programme_index"),
];
const SOURCE_BEFORE: &str = include_str!("../datasets/sqlite-epg-source-before.sql");
const SOURCE_AFTER: &str = include_str!("../datasets/sqlite-epg-source-after.sql");
const SOURCE_WITH_INDEX: &str = include_str!("../datasets/sqlite-epg-source-programme-index.sql");
const DATA_BEFORE: &str = include_str!("../datasets/sqlite-epg-data-before.sql");
const DATA_AFTER: &str = include_str!("../datasets/sqlite-epg-data-after.sql");
const SQL: [&str; 3] = [
    include_str!("../datasets/sqlite-epg-source-remake.sql"),
    include_str!("../datasets/sqlite-epg-data-remake.sql"),
    include_str!("../datasets/sqlite-epg-source-add-programme-index.sql"),
];

pub(super) async fn run(path: &Path) -> Result<usize, Failure> {
    if !std::fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_file()) {
        return Err(Failure::OutsideKnownState);
    }
    // Django's SQLite table remake uses a dedicated connection with foreign
    // key enforcement disabled during copy/drop/rename and checks the result.
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
    prefix: usize,
) -> Result<(), Failure> {
    let source = [SOURCE_BEFORE, SOURCE_AFTER, SOURCE_AFTER, SOURCE_WITH_INDEX][prefix];
    let data = if prefix >= 2 { DATA_AFTER } else { DATA_BEFORE };
    for (table, expected) in [("epg_epgsource", source), ("epg_epgdata", data)] {
        let observed = sqlx::query_scalar::<_, String>(
            "SELECT sql FROM sqlite_schema WHERE tbl_name=? AND sql IS NOT NULL
             ORDER BY type DESC,name LIMIT 6",
        )
        .bind(table)
        .fetch_all(&mut **tx)
        .await?;
        if !observed.iter().map(String::as_str).eq(expected.lines()) {
            return Err(Failure::OutsideKnownState);
        }
    }
    Ok(())
}

async fn execute(tx: &mut Transaction<'_, sqlx::Sqlite>) -> Result<usize, Failure> {
    let names = sqlx::query_as::<_, (String, String)>(
        "SELECT CASE WHEN length(CAST(app AS BLOB))<=255 THEN app ELSE '' END,
                CASE WHEN length(CAST(name AS BLOB))<=255 THEN name ELSE '' END
         FROM django_migrations ORDER BY app,name LIMIT 144",
    )
    .fetch_all(&mut **tx)
    .await?;
    let prefix = names
        .len()
        .checked_sub(140)
        .filter(|n| *n <= STEPS.len())
        .ok_or(Failure::OutsideKnownState)?;
    let mut expected = HISTORICAL.to_vec();
    expected.extend([KEY, MANAGER]);
    expected.extend(app_data::STEPS);
    expected.extend(core_event_profile::STEPS);
    expected.extend(&STEPS[..prefix]);
    expected.sort_unstable();
    if !names
        .iter()
        .map(|(app, name)| (app.as_str(), name.as_str()))
        .eq(expected)
    {
        return Err(Failure::OutsideKnownState);
    }
    check_layout(tx, prefix).await?;
    for (statements, step) in SQL.into_iter().zip(STEPS).skip(prefix) {
        for statement in statements.lines() {
            sqlx::query(statement).execute(&mut **tx).await?;
        }
        record(tx, step).await?;
    }
    check_layout(tx, STEPS.len()).await?;
    if sqlx::query("PRAGMA foreign_key_check")
        .fetch_optional(&mut **tx)
        .await?
        .is_some()
    {
        return Err(Failure::OutsideKnownState);
    }
    Ok(STEPS.len() - prefix)
}

#[cfg(test)]
#[path = "sqlite_epg_guide_prefix_contract.rs"]
mod contract;
