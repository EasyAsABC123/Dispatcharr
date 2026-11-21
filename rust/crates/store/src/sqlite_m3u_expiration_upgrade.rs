//! Pinned Original M3U 0019 on an owned historical SQLite copy.
//! Original Dispatcharr and Django contributors retain full credit.
use chrono::{Datelike, NaiveDateTime};
use serde_json::Value;

use super::*;

pub(super) const STEP: (&str, &str) = ("m3u", "0019_m3uaccountprofile_exp_date");
const CATALOG: &str = include_str!("../datasets/sqlite-m3u-expiration-catalog.json");

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
    expected: &[String],
) -> Result<(), Failure> {
    let observed = sqlx::query_scalar::<_, String>(
        "SELECT sql FROM sqlite_schema WHERE tbl_name='m3u_m3uaccountprofile' AND sql IS NOT NULL
         ORDER BY type DESC,name LIMIT 4",
    )
    .fetch_all(&mut **tx)
    .await?;
    if observed != expected {
        return Err(Failure::OutsideKnownState);
    }
    Ok(())
}

async fn execute(tx: &mut Transaction<'_, sqlx::Sqlite>) -> Result<usize, Failure> {
    let names = sqlx::query_as::<_, (String, String)>(
        "SELECT CASE WHEN length(CAST(app AS BLOB))<=255 THEN app ELSE '' END,
                CASE WHEN length(CAST(name AS BLOB))<=255 THEN name ELSE '' END
         FROM django_migrations ORDER BY app,name LIMIT 153",
    )
    .fetch_all(&mut **tx)
    .await?;
    let applied = match names.len() {
        151 => false,
        152 => true,
        _ => return Err(Failure::OutsideKnownState),
    };
    let mut expected = HISTORICAL.to_vec();
    expected.extend([KEY, MANAGER]);
    expected.extend(app_data::STEPS);
    expected.extend(core_event_profile::STEPS);
    expected.extend(epg_guide_prefix::STEPS);
    expected.extend(epg_index::STEPS);
    expected.extend(plugins::STEPS);
    if applied {
        expected.push(STEP);
    }
    expected.sort_unstable();
    if !names
        .iter()
        .map(|(app, name)| (app.as_str(), name.as_str()))
        .eq(expected)
    {
        return Err(Failure::OutsideKnownState);
    }
    let catalog: [Vec<String>; 2] =
        serde_json::from_str(CATALOG).map_err(|_| Failure::OutsideKnownState)?;
    check_layout(tx, &catalog[usize::from(applied)]).await?;
    if !applied {
        sqlx::query(r#"ALTER TABLE "m3u_m3uaccountprofile" ADD COLUMN "exp_date" datetime NULL"#)
            .execute(&mut **tx)
            .await?;
        backfill(tx).await?;
        check_layout(tx, &catalog[1]).await?;
        record(tx, STEP).await?;
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

fn string_number(raw: &str) -> Option<f64> {
    let trimmed = raw.trim();
    if !trimmed.contains('_') {
        return trimmed.parse().ok();
    }
    let bytes = trimmed.as_bytes();
    let mut number = String::with_capacity(trimmed.len());
    for (index, byte) in bytes.iter().copied().enumerate() {
        if byte == b'_' {
            if !index
                .checked_sub(1)
                .and_then(|i| bytes.get(i))
                .is_some_and(u8::is_ascii_digit)
                || !bytes.get(index + 1).is_some_and(u8::is_ascii_digit)
            {
                return None;
            }
        } else if byte.is_ascii() {
            number.push(char::from(byte));
        } else {
            return None;
        }
    }
    number.parse().ok()
}

fn expiration(value: &Value) -> Result<Option<NaiveDateTime>, Failure> {
    let number = match value {
        Value::Number(v) => Some(
            v.to_string()
                .parse::<f64>()
                .map_err(|_| Failure::OutsideKnownState)?,
        ),
        Value::Bool(v) => Some(f64::from(u8::from(*v))),
        Value::String(raw) => string_number(raw),
        _ => None,
    };
    if let Some(value) = number {
        // CPython raises OverflowError for a value outside time_t's signed
        // range; the migration's catch clause does not consume that error.
        if value.is_infinite()
            || !(-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&value)
                && !value.is_nan()
        {
            return Err(Failure::OutsideKnownState);
        }
        if let Some(number) = serde_json::Number::from_f64(value) {
            if let Some(parsed) = crate::xc_account_provider::expiration(&Value::Number(number)) {
                return Ok(Some(parsed.naive_utc()));
            }
        }
    }
    let Some(parsed) = value.as_str().and_then(crate::epg_grid_iso::datetime) else {
        return Ok(None);
    };
    let utc = parsed
        .local
        .checked_sub_signed(chrono::Duration::microseconds(parsed.offset_micros))
        .filter(|value| (1..=9999).contains(&value.year()))
        .ok_or(Failure::OutsideKnownState)?;
    Ok(Some(utc))
}

fn profile_expiration(raw: &str) -> Result<Option<NaiveDateTime>, Failure> {
    let custom: Value = serde_json::from_str(raw).map_err(|_| Failure::OutsideKnownState)?;
    let custom = custom.as_object().ok_or(Failure::OutsideKnownState)?;
    let Some(info) = custom.get("user_info") else {
        return Ok(None);
    };
    let info = info.as_object().ok_or(Failure::OutsideKnownState)?;
    match info.get("exp_date") {
        Some(exp) => expiration(exp),
        None => Ok(None),
    }
}

async fn backfill(tx: &mut Transaction<'_, sqlx::Sqlite>) -> Result<(), Failure> {
    let mut previous: Option<i64> = None;
    loop {
        let rows = sqlx::query_as::<_, (i64, String)>(
            "SELECT id,custom_properties FROM m3u_m3uaccountprofile
             WHERE custom_properties IS NOT NULL AND (?1 IS NULL OR id>?1) ORDER BY id LIMIT 32",
        )
        .bind(previous)
        .fetch_all(&mut **tx)
        .await?;
        if rows.is_empty() {
            return Ok(());
        }
        for (id, raw) in rows {
            if let Some(date) = profile_expiration(&raw)? {
                let format = if date.and_utc().timestamp_subsec_micros() == 0 {
                    "%Y-%m-%d %H:%M:%S"
                } else {
                    "%Y-%m-%d %H:%M:%S%.6f"
                };
                sqlx::query("UPDATE m3u_m3uaccountprofile SET exp_date=? WHERE id=?")
                    .bind(date.format(format).to_string())
                    .bind(id)
                    .execute(&mut **tx)
                    .await?;
            }
            previous = Some(id);
        }
    }
}

#[cfg(test)]
#[path = "sqlite_m3u_expiration_contract.rs"]
mod contract;
