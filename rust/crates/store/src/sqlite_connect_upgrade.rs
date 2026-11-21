//! Pinned Original Connect 0001–0005 on a detached historical SQLite copy.
//! Original Dispatcharr and Django contributors retain full credit.
use super::*;

pub(super) const APP: &str = "dispatcharr_connect";
pub(super) const STEPS: [&str; 5] = [
    "0001_initial",
    "0002_alter_eventsubscription_event",
    "0003_alter_eventsubscription_event",
    "0004_alter_eventsubscription_event",
    "0005_alter_eventsubscription_event",
];
const INIT: &str = include_str!("../datasets/sqlite-connect-initial.sql");

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
    initialized: bool,
) -> Result<(), Failure> {
    let observed = sqlx::query_scalar::<_, String>(
        "SELECT sql FROM sqlite_schema
         WHERE tbl_name IN ('dispatcharr_connect_integration','dispatcharr_connect_eventsubscription',
                            'dispatcharr_connect_deliverylog') AND sql IS NOT NULL
         ORDER BY tbl_name DESC,type DESC,name LIMIT 7",
    )
    .fetch_all(&mut **tx)
    .await?;
    let expected = if initialized { INIT } else { "" };
    if !observed.iter().map(String::as_str).eq(expected.lines()) {
        return Err(Failure::OutsideKnownState);
    }
    Ok(())
}

async fn execute(tx: &mut Transaction<'_, sqlx::Sqlite>) -> Result<usize, Failure> {
    let names = sqlx::query_as::<_, (String, String)>(
        "SELECT CASE WHEN length(CAST(app AS BLOB))<=255 THEN app ELSE '' END,
                CASE WHEN length(CAST(name AS BLOB))<=255 THEN name ELSE '' END
         FROM django_migrations ORDER BY app,name LIMIT 158",
    )
    .fetch_all(&mut **tx)
    .await?;
    let prefix = names
        .len()
        .checked_sub(152)
        .filter(|n| *n <= STEPS.len())
        .ok_or(Failure::OutsideKnownState)?;
    let mut expected = HISTORICAL.to_vec();
    expected.extend([KEY, MANAGER]);
    expected.extend(app_data::STEPS);
    expected.extend(core_event_profile::STEPS);
    expected.extend(epg_guide_prefix::STEPS);
    expected.extend(epg_index::STEPS);
    expected.extend(plugins::STEPS);
    expected.push(m3u_expiration::STEP);
    expected.extend(STEPS[..prefix].iter().map(|name| (APP, *name)));
    expected.sort_unstable();
    if !names
        .iter()
        .map(|(app, name)| (app.as_str(), name.as_str()))
        .eq(expected)
    {
        return Err(Failure::OutsideKnownState);
    }
    check_layout(tx, prefix > 0).await?;
    for (index, step) in STEPS.iter().copied().enumerate().skip(prefix) {
        apply_step(tx, index, step).await?;
    }
    check_layout(tx, true).await?;
    if sqlx::query("PRAGMA foreign_key_check")
        .fetch_optional(&mut **tx)
        .await?
        .is_some()
    {
        return Err(Failure::OutsideKnownState);
    }
    Ok(STEPS.len() - prefix)
}

async fn apply_step(
    tx: &mut Transaction<'_, sqlx::Sqlite>,
    index: usize,
    name: &str,
) -> Result<(), Failure> {
    if index == 0 {
        for statement in INIT.lines() {
            sqlx::query(statement).execute(&mut **tx).await?;
        }
    }
    // Pinned 0002–0005 change Django's field choices. The SQLite catalog
    // and legacy event values remain as they were after 0001.
    record(tx, (APP, name)).await
}

#[cfg(test)]
#[path = "sqlite_connect_upgrade_contract.rs"]
mod contract;
