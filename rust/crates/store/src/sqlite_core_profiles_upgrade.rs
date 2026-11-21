//! Pinned Original Core 0023–0028 on a detached historical SQLite copy.
//! Original Dispatcharr and Django contributors retain full credit.
use serde_json::{json, Map, Value};

use super::*;

pub(super) const STEPS: [&str; 6] = [
    "0023_alter_systemevent_event_type",
    "0024_outputprofile",
    "0025_move_preferred_region_and_auto_import_to_system_settings",
    "0026_add_channel_client_wait_period",
    "0027_vlc_play_and_exit",
    "0028_alter_systemevent_event_type",
];
const OUTPUT_DDL: &str = include_str!("../datasets/sqlite-core-outputprofile.sql");
const ORIGINAL_PROFILES: &str = include_str!("../datasets/sqlite-core-outputprofile-original.json");
const SETTINGS_LAYOUTS: &str =
    include_str!("../datasets/sqlite-core-settings-accepted-layouts.json");
const EVENT_DDL: &str = include_str!("../datasets/sqlite-core-systemevent.sql");

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

pub(super) async fn layout(
    tx: &mut Transaction<'_, sqlx::Sqlite>,
    table: &str,
) -> Result<Vec<String>, Failure> {
    Ok(sqlx::query_scalar::<_, String>(
        "SELECT sql FROM sqlite_schema WHERE tbl_name=? AND sql IS NOT NULL ORDER BY type DESC,name LIMIT 8",
    )
    .bind(table)
    .fetch_all(&mut **tx)
    .await?)
}

pub(super) async fn check_layout(
    tx: &mut Transaction<'_, sqlx::Sqlite>,
    has_output: bool,
) -> Result<(), Failure> {
    let settings: [Vec<String>; 2] =
        serde_json::from_str(SETTINGS_LAYOUTS).map_err(|_| Failure::OutsideKnownState)?;
    if !settings.contains(&layout(tx, "core_coresettings").await?)
        || !layout(tx, "core_systemevent")
            .await?
            .iter()
            .map(String::as_str)
            .eq(EVENT_DDL.lines())
    {
        return Err(Failure::OutsideKnownState);
    }
    let output = if has_output { OUTPUT_DDL } else { "" };
    if !layout(tx, "core_outputprofile")
        .await?
        .iter()
        .map(String::as_str)
        .eq(output.lines())
    {
        return Err(Failure::OutsideKnownState);
    }
    Ok(())
}

async fn execute(tx: &mut Transaction<'_, sqlx::Sqlite>) -> Result<usize, Failure> {
    let names = sqlx::query_as::<_, (String, String)>(
        "SELECT CASE WHEN length(CAST(app AS BLOB))<=255 THEN app ELSE '' END,
                CASE WHEN length(CAST(name AS BLOB))<=255 THEN name ELSE '' END
         FROM django_migrations ORDER BY app,name LIMIT 167",
    )
    .fetch_all(&mut **tx)
    .await?;
    let prefix = names
        .len()
        .checked_sub(160)
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
    expected.extend(connect::STEPS.iter().map(|name| (connect::APP, *name)));
    expected.extend(core_settings::STEPS);
    expected.extend(STEPS[..prefix].iter().map(|name| ("core", *name)));
    expected.sort_unstable();
    if !names
        .iter()
        .map(|(app, name)| (app.as_str(), name.as_str()))
        .eq(expected)
    {
        return Err(Failure::OutsideKnownState);
    }
    check_layout(tx, prefix >= 2).await?;
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
    step: &str,
) -> Result<(), Failure> {
    match index {
        0 | 5 => {} // The Django choice edits preserve schema and stored event values.
        1 => seed_output(tx).await?,
        2 => move_stream_fields(tx).await?,
        3 => update_proxy(tx).await?,
        4 => {
            sqlx::query("UPDATE core_streamprofile SET parameters=? WHERE name='VLC' AND locked=1")
                .bind(core_event_profile::VLC_ARGS)
                .execute(&mut **tx)
                .await?;
        }
        _ => unreachable!(),
    }
    record(tx, ("core", step)).await
}

async fn seed_output(tx: &mut Transaction<'_, sqlx::Sqlite>) -> Result<(), Failure> {
    sqlx::query(OUTPUT_DDL.trim_end())
        .execute(&mut **tx)
        .await?;
    let rows: [(String, String, String, i64, i64); 2] =
        serde_json::from_str(ORIGINAL_PROFILES).map_err(|_| Failure::OutsideKnownState)?;
    for (name, command, parameters, locked, active) in rows {
        sqlx::query("INSERT INTO core_outputprofile(name,command,parameters,locked,is_active) VALUES(?,?,?,?,?)")
            .bind(name).bind(command).bind(parameters).bind(locked).bind(active)
            .execute(&mut **tx).await?;
    }
    Ok(())
}

async fn setting_object(
    tx: &mut Transaction<'_, sqlx::Sqlite>,
    key: &str,
) -> Result<Option<Map<String, Value>>, Failure> {
    let raw: Option<String> = sqlx::query_scalar("SELECT value FROM core_coresettings WHERE key=?")
        .bind(key)
        .fetch_optional(&mut **tx)
        .await?;
    raw.map(|raw| {
        let value: Value = serde_json::from_str(&raw).map_err(|_| Failure::OutsideKnownState)?;
        Ok(if let Value::Object(values) = value {
            values
        } else {
            Map::new()
        })
    })
    .transpose()
}

async fn update_object(
    tx: &mut Transaction<'_, sqlx::Sqlite>,
    key: &str,
    values: Map<String, Value>,
) -> Result<(), Failure> {
    sqlx::query("UPDATE core_coresettings SET value=? WHERE key=?")
        .bind(core_settings::original_json(&Value::Object(values))?)
        .bind(key)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn move_stream_fields(tx: &mut Transaction<'_, sqlx::Sqlite>) -> Result<(), Failure> {
    let stream = setting_object(tx, "stream_settings").await?;
    let system = setting_object(tx, "system_settings").await?;
    if system.is_none() {
        sqlx::query("INSERT INTO core_coresettings(key,name,value) VALUES('system_settings','System Settings','{}')")
            .execute(&mut **tx).await?;
    }
    let mut system = system.unwrap_or_default();
    if let Some(mut stream) = stream {
        for field in ["preferred_region", "auto_import_mapped_files"] {
            if let Some(value) = stream.shift_remove(field) {
                if system.get(field).is_none_or(Value::is_null) {
                    system.insert(field.to_owned(), value);
                }
            }
        }
        update_object(tx, "stream_settings", stream).await?;
    }
    system.entry("preferred_region").or_insert(Value::Null);
    system
        .entry("auto_import_mapped_files")
        .or_insert(json!(true));
    update_object(tx, "system_settings", system).await
}

async fn update_proxy(tx: &mut Transaction<'_, sqlx::Sqlite>) -> Result<(), Failure> {
    let Some(mut proxy) = setting_object(tx, "proxy_settings").await? else {
        return Ok(());
    };
    proxy
        .entry("channel_client_wait_period")
        .or_insert(json!(5));
    let current = match proxy.get("channel_init_grace_period") {
        None => 5.0,
        Some(Value::Bool(flag)) => f64::from(u8::from(*flag)),
        Some(Value::Number(value)) => value
            .to_string()
            .parse::<f64>()
            .map_err(|_| Failure::OutsideKnownState)?,
        _ => return Err(Failure::OutsideKnownState),
    };
    if current < 60.0 {
        proxy.insert("channel_init_grace_period".to_owned(), json!(60));
    }
    update_object(tx, "proxy_settings", proxy).await
}

#[cfg(test)]
#[path = "sqlite_core_profiles_contract.rs"]
mod contract;
