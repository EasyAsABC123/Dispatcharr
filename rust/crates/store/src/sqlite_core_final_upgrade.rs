//! Pinned Original Core 0029–0032 on a detached historical SQLite copy.
//! Original Dispatcharr and Django contributors retain full credit.
use super::*;

pub(super) const STEPS: [&str; 4] = [
    "0029_update_default_stream_and_output_profiles",
    "0030_rename_default_stream_profile_names",
    "0031_default_reverse_proxy_auth_settings",
    "0032_limit_default_interleave_delta",
];
const OLD_STREAM_DDL: &str = include_str!("../datasets/sqlite-core-final-streamprofile.sql");
const FIRST_ARGS: &str =
    include_str!("../datasets/sqlite-core-original-0029-profile-parameters.json");
const FINAL_ARGS: &str =
    include_str!("../datasets/sqlite-core-original-0032-profile-parameters.json");

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

async fn check_layout(tx: &mut Transaction<'_, sqlx::Sqlite>) -> Result<(), Failure> {
    core_profiles::check_layout(tx, true).await?;
    if !core_profiles::layout(tx, "core_streamprofile")
        .await?
        .iter()
        .map(String::as_str)
        .eq(OLD_STREAM_DDL.lines())
    {
        return Err(Failure::OutsideKnownState);
    }
    Ok(())
}

async fn execute(tx: &mut Transaction<'_, sqlx::Sqlite>) -> Result<usize, Failure> {
    let names = sqlx::query_as::<_, (String, String)>(
        "SELECT CASE WHEN length(CAST(app AS BLOB))<=255 THEN app ELSE '' END,
                CASE WHEN length(CAST(name AS BLOB))<=255 THEN name ELSE '' END
         FROM django_migrations ORDER BY app,name LIMIT 171",
    )
    .fetch_all(&mut **tx)
    .await?;
    let prefix = names
        .len()
        .checked_sub(166)
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
            .chain(&STEPS[..prefix])
            .map(|step| ("core", *step)),
    );
    expected.sort_unstable();
    if !names
        .iter()
        .map(|(app, name)| (app.as_str(), name.as_str()))
        .eq(expected)
    {
        return Err(Failure::OutsideKnownState);
    }
    check_layout(tx).await?;
    for (index, step) in STEPS.iter().copied().enumerate().skip(prefix) {
        apply_step(tx, index, step).await?;
    }
    check_layout(tx).await?;
    if sqlx::query("PRAGMA foreign_key_check")
        .fetch_optional(&mut **tx)
        .await?
        .is_some()
    {
        return Err(Failure::OutsideKnownState);
    }
    Ok(STEPS.len() - prefix)
}

async fn update_parameters(
    tx: &mut Transaction<'_, sqlx::Sqlite>,
    data: &str,
) -> Result<(), Failure> {
    let profiles: Vec<(String, String, String)> =
        serde_json::from_str(data).map_err(|_| Failure::OutsideKnownState)?;
    for (table, name, args) in profiles {
        let statement = match table.as_str() {
            "output" => "UPDATE core_outputprofile SET parameters=? WHERE name=? AND locked=1",
            "stream" => "UPDATE core_streamprofile SET parameters=? WHERE name=? AND locked=1",
            _ => return Err(Failure::OutsideKnownState),
        };
        sqlx::query(statement)
            .bind(args)
            .bind(name)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

async fn apply_step(
    tx: &mut Transaction<'_, sqlx::Sqlite>,
    index: usize,
    step: &str,
) -> Result<(), Failure> {
    match index {
        0 => update_parameters(tx, FIRST_ARGS).await?,
        1 => {
            // Source only changes field help text before these SQLite
            // case-insensitive, non-wildcard name updates.
            for (previous, next) in [("ffmpeg", "FFmpeg"), ("streamlink", "Streamlink")] {
                sqlx::query(r#"UPDATE core_streamprofile SET name=? WHERE name LIKE ? ESCAPE '\' AND locked=1"#)
                    .bind(next)
                    .bind(previous)
                    .execute(&mut **tx)
                    .await?;
            }
        }
        2 => {
            sqlx::query(
                "INSERT INTO core_coresettings(key,name,value)
                 SELECT 'reverse_proxy_auth','Reverse Proxy Auth',?
                 WHERE NOT EXISTS(SELECT 1 FROM core_coresettings WHERE key='reverse_proxy_auth')",
            )
            .bind(r#"{"enabled": false, "header": ""}"#)
            .execute(&mut **tx)
            .await?;
        }
        3 => update_parameters(tx, FINAL_ARGS).await?,
        _ => unreachable!(),
    }
    record(tx, ("core", step)).await
}

#[cfg(test)]
#[path = "sqlite_core_final_contract.rs"]
mod contract;
