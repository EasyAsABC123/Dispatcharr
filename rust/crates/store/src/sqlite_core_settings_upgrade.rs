//! Pinned Original Core 0020, 0021, and 022 on a detached historical SQLite copy.
//! Original Dispatcharr and Django contributors retain full credit.
use std::collections::BTreeMap;

use serde_json::{json, Value};

use super::*;

pub(super) const STEPS: [(&str, &str); 3] = [
    ("core", "0020_change_coresettings_value_to_jsonfield"),
    ("core", "0021_systemnotification_notificationdismissal"),
    ("core", "022_default_user_limit_settings"),
];
const CATALOG: &str = include_str!("../datasets/sqlite-core-settings-notifications-catalog.json");
const SETTINGS_DDL: &str = include_str!("../datasets/sqlite-core-settings-json.sql");
const NOTIFICATIONS_DDL: &str = include_str!("../datasets/sqlite-core-notifications.sql");

pub(super) async fn run(path: &Path, display_zone: &str) -> Result<usize, Failure> {
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
        match execute(&mut tx, display_zone).await {
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
    layout: &BTreeMap<String, Vec<String>>,
) -> Result<(), Failure> {
    for (table, expected) in layout {
        let observed = sqlx::query_scalar::<_, String>(
            "SELECT sql FROM sqlite_schema WHERE tbl_name=? AND sql IS NOT NULL
             ORDER BY type DESC,name LIMIT 10",
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

async fn execute(
    tx: &mut Transaction<'_, sqlx::Sqlite>,
    display_zone: &str,
) -> Result<usize, Failure> {
    let names = sqlx::query_as::<_, (String, String)>(
        "SELECT CASE WHEN length(CAST(app AS BLOB))<=255 THEN app ELSE '' END,
                CASE WHEN length(CAST(name AS BLOB))<=255 THEN name ELSE '' END
         FROM django_migrations ORDER BY app,name LIMIT 161",
    )
    .fetch_all(&mut **tx)
    .await?;
    let prefix = names
        .len()
        .checked_sub(157)
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
    expected.extend(&STEPS[..prefix]);
    expected.sort_unstable();
    if !names
        .iter()
        .map(|(app, name)| (app.as_str(), name.as_str()))
        .eq(expected)
    {
        return Err(Failure::OutsideKnownState);
    }
    let catalogs: [BTreeMap<String, Vec<String>>; 4] =
        serde_json::from_str(CATALOG).map_err(|_| Failure::OutsideKnownState)?;
    check_layout(tx, &catalogs[prefix]).await?;
    for (index, catalog) in catalogs.iter().enumerate().skip(prefix + 1) {
        apply_step(tx, index - 1, display_zone).await?;
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

async fn apply_step(
    tx: &mut Transaction<'_, sqlx::Sqlite>,
    index: usize,
    display_zone: &str,
) -> Result<(), Failure> {
    match index {
        0 => {
            rewrite_old_values(tx).await?;
            for statement in SETTINGS_DDL.lines() {
                sqlx::query(statement).execute(&mut **tx).await?;
            }
            consolidate(tx, display_zone).await?;
        }
        1 => {
            for statement in NOTIFICATIONS_DDL.lines() {
                sqlx::query(statement).execute(&mut **tx).await?;
            }
        }
        2 => {
            sqlx::query("INSERT INTO core_coresettings(key,name,value) VALUES('user_limit_settings','User Limit Settings','{}')")
                .execute(&mut **tx).await?;
        }
        _ => unreachable!(),
    }
    record(tx, STEPS[index]).await
}

fn original_value(raw: &str) -> Result<Value, Failure> {
    if matches!(raw.as_bytes().first(), Some(b'{' | b'[')) {
        if let Ok(value) = serde_json::from_str(raw) {
            return Ok(value);
        }
    }
    let digits = raw.trim_start_matches('-');
    if !digits.is_empty() && digits.chars().all(char::is_numeric) {
        if let Ok(integer) = crate::json_compat::python_int_string(raw) {
            return serde_json::from_str(&integer.to_string())
                .map_err(|_| Failure::OutsideKnownState);
        }
    } else if let Some(number) = dispatcharr_ingest_core::source_float_literal(raw) {
        return serde_json::Number::from_f64(number)
            .map(Value::Number)
            .ok_or(Failure::OutsideKnownState);
    }
    Ok(match raw.to_lowercase().as_str() {
        "true" | "yes" | "on" => Value::Bool(true),
        "false" | "no" | "off" => Value::Bool(false),
        _ => Value::String(raw.to_owned()),
    })
}

pub(super) fn original_json(value: &Value) -> Result<String, Failure> {
    let compact =
        crate::json_compat::compact_json_bytes(value).map_err(|_| Failure::OutsideKnownState)?;
    app_data::django_storage_json(&compact)
}

async fn rewrite_old_values(tx: &mut Transaction<'_, sqlx::Sqlite>) -> Result<(), Failure> {
    let mut previous: Option<i64> = None;
    loop {
        let rows = sqlx::query_as::<_, (i64, String)>(
            "SELECT id,value FROM core_coresettings WHERE (?1 IS NULL OR id>?1) ORDER BY id LIMIT 32",
        )
        .bind(previous)
        .fetch_all(&mut **tx)
        .await?;
        if rows.is_empty() {
            return Ok(());
        }
        for (id, raw) in rows {
            sqlx::query("UPDATE core_coresettings SET value=? WHERE id=?")
                .bind(original_json(&original_value(&raw)?)?)
                .bind(id)
                .execute(&mut **tx)
                .await?;
            previous = Some(id);
        }
    }
}

async fn set_group(
    tx: &mut Transaction<'_, sqlx::Sqlite>,
    key: &str,
    title: &str,
    value: &Value,
) -> Result<(), Failure> {
    let serialized = original_json(value)?;
    // QuerySet.update_or_create only consumes a sequence slot for insertion.
    let updated = sqlx::query("UPDATE core_coresettings SET name=?,value=? WHERE key=?")
        .bind(title)
        .bind(&serialized)
        .bind(key)
        .execute(&mut **tx)
        .await?;
    if updated.rows_affected() == 0 {
        sqlx::query("INSERT INTO core_coresettings(key,name,value) VALUES(?,?,?)")
            .bind(key)
            .bind(title)
            .bind(serialized)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

async fn rename_or_seed(
    tx: &mut Transaction<'_, sqlx::Sqlite>,
    old: &str,
    key: &str,
    title: &str,
    initial: &Value,
) -> Result<(), Failure> {
    sqlx::query("UPDATE core_coresettings SET key=? WHERE key=?")
        .bind(key)
        .bind(old)
        .execute(&mut **tx)
        .await?;
    sqlx::query("INSERT INTO core_coresettings(key,name,value) SELECT ?,?,? WHERE NOT EXISTS(SELECT 1 FROM core_coresettings WHERE key=?)")
        .bind(key)
        .bind(title)
        .bind(original_json(initial)?)
        .bind(key)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn consolidate(
    tx: &mut Transaction<'_, sqlx::Sqlite>,
    display_zone: &str,
) -> Result<(), Failure> {
    let source = sqlx::query_as::<_, (String, String)>("SELECT key,value FROM core_coresettings")
        .fetch_all(&mut **tx)
        .await?;
    let values = source
        .into_iter()
        .map(|(key, raw)| {
            let value =
                serde_json::from_str::<Value>(&raw).map_err(|_| Failure::OutsideKnownState)?;
            Ok((key, value))
        })
        .collect::<Result<BTreeMap<_, _>, Failure>>()?;
    let get = |key: &str, default: Value| {
        values
            .get(key)
            .filter(|v| !v.is_null())
            .cloned()
            .unwrap_or(default)
    };
    let truthy = |key: &str, default: Value| {
        values
            .get(key)
            .filter(|v| crate::channels::truthy(Some(v)))
            .cloned()
            .unwrap_or(default)
    };
    let int = |key: &str, default: i64| {
        crate::json_compat::python_json_int(&truthy(key, json!(default)))
            .map_err(|_| Failure::OutsideKnownState)
    };
    let zone = if display_zone.parse::<chrono_tz::Tz>().is_ok() {
        display_zone
    } else {
        "UTC"
    };
    let groups = [
        (
            "stream_settings",
            "Stream Settings",
            json!({
                "default_user_agent": get("default-user-agent", Value::Null),
                "default_stream_profile": get("default-stream-profile", Value::Null),
                "m3u_hash_key": get("m3u-hash-key", json!("")),
                "preferred_region": get("preferred-region", Value::Null),
                "auto_import_mapped_files": get("auto-import-mapped-files", Value::Null),
            }),
        ),
        (
            "dvr_settings",
            "DVR Settings",
            json!({
                "tv_template": get("dvr-tv-template", json!("TV_Shows/{show}/S{season:02d}E{episode:02d}.mkv")),
                "movie_template": get("dvr-movie-template", json!("Movies/{title} ({year}).mkv")),
                "tv_fallback_dir": get("dvr-tv-fallback-dir", json!("TV_Shows")),
                "tv_fallback_template": get("dvr-tv-fallback-template", json!("TV_Shows/{show}/{start}.mkv")),
                "movie_fallback_template": get("dvr-movie-fallback-template", json!("Movies/{start}.mkv")),
                "comskip_enabled": crate::channels::truthy(values.get("dvr-comskip-enabled")),
                "comskip_custom_path": get("dvr-comskip-custom-path", json!("")),
                "pre_offset_minutes": int("dvr-pre-offset-minutes", 0)?,
                "post_offset_minutes": int("dvr-post-offset-minutes", 0)?,
                "series_rules": get("dvr-series-rules", json!([])),
            }),
        ),
        (
            "backup_settings",
            "Backup Settings",
            json!({
                "schedule_enabled": get("backup_schedule_enabled", json!(true)),
                "schedule_frequency": truthy("backup_schedule_frequency", json!("daily")),
                "schedule_time": truthy("backup_schedule_time", json!("03:00")),
                "schedule_day_of_week": get("backup_schedule_day_of_week", json!(0)),
                "retention_count": get("backup_retention_count", json!(3)),
                "schedule_cron_expression": truthy("backup_schedule_cron_expression", json!("")),
            }),
        ),
        (
            "system_settings",
            "System Settings",
            json!({
                "time_zone": get("system-time-zone", json!(zone)),
                "max_system_events": int("max-system-events", 100)?,
            }),
        ),
    ];
    for (key, name, payload) in groups {
        set_group(tx, key, name, &payload).await?;
    }
    rename_or_seed(
        tx,
        "proxy-settings",
        "proxy_settings",
        "Proxy Settings",
        &json!({
            "buffering_timeout": 15, "buffering_speed": 1.0, "redis_chunk_ttl": 60,
            "channel_shutdown_delay": 0, "channel_init_grace_period": 5,
        }),
    )
    .await?;
    rename_or_seed(
        tx,
        "network-access",
        "network_access",
        "Network Access",
        &json!({}),
    )
    .await?;
    // Source removes individual legacy settings once the six groups exist.
    sqlx::query("DELETE FROM core_coresettings WHERE key NOT IN ('stream_settings','dvr_settings','backup_settings','system_settings','proxy_settings','network_access')")
        .execute(&mut **tx).await?;
    Ok(())
}

#[cfg(test)]
#[path = "sqlite_core_settings_contract.rs"]
mod contract;
