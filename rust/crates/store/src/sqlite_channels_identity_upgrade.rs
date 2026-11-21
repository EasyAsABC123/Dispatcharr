//! Pinned Original Channels 0033–0036 on detached historical SQLite.
//! Original Dispatcharr and Django contributors retain full credit.
use std::collections::{BTreeMap, HashSet};

use dispatcharr_ingest_core::{stream_hash_nullable, NullableStreamIdentity};
use serde_json::Value;
use sqlx::Row;

use super::*;

pub(super) const STEPS: [&str; 4] = [
    "0033_stream_id_stream_chno",
    "0034_remove_stream_dispatcharr_stream_id_idx_and_more",
    "0035_alter_channel_name_alter_stream_name",
    "0036_alter_stream_name",
];
const CATALOG: &str = include_str!("../datasets/sqlite-channels-identity-catalog.json");
const SQL: [&str; 4] = [
    include_str!("../datasets/sqlite-channels-0033-indexes.sql"),
    include_str!("../datasets/sqlite-channels-0034.sql"),
    include_str!("../datasets/sqlite-channels-0035.sql"),
    include_str!("../datasets/sqlite-channels-0036.sql"),
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
            "SELECT sql FROM sqlite_schema WHERE tbl_name=? AND sql IS NOT NULL
             ORDER BY type DESC,name LIMIT 15",
        )
        .bind(table)
        .fetch_all(&mut **tx)
        .await?;
        if observed != *expected {
            return Err(Failure::OutsideKnownState);
        }
    }
    let inbound = sqlx::query_as::<_, (String, String)>(
        r#"SELECT owner.name,fk."from" FROM sqlite_schema owner
           JOIN pragma_foreign_key_list(owner.name) fk
           WHERE owner.type='table' AND fk."table"='dispatcharr_channels_stream' LIMIT 2"#,
    )
    .fetch_all(&mut **tx)
    .await?;
    if inbound.as_slice()
        != [(
            "dispatcharr_channels_channelstream".to_owned(),
            "stream_id".to_owned(),
        )]
    {
        return Err(Failure::OutsideKnownState);
    }
    Ok(())
}

async fn execute(tx: &mut Transaction<'_, sqlx::Sqlite>) -> Result<usize, Failure> {
    let names = sqlx::query_as::<_, (String, String)>(
        "SELECT CASE WHEN length(CAST(app AS BLOB))<=255 THEN app ELSE '' END,
                CASE WHEN length(CAST(name AS BLOB))<=255 THEN name ELSE '' END
         FROM django_migrations ORDER BY app,name LIMIT 177",
    )
    .fetch_all(&mut **tx)
    .await?;
    let prefix = names
        .len()
        .checked_sub(172)
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
    expected.extend(
        core_profiles::STEPS
            .iter()
            .chain(&core_final::STEPS)
            .map(|name| ("core", *name)),
    );
    expected.extend(
        channels_flags::STEPS
            .iter()
            .chain(&STEPS[..prefix])
            .map(|name| (channels_flags::APP, *name)),
    );
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
        sqlx::query(
            r#"ALTER TABLE "dispatcharr_channels_stream" ADD COLUMN "stream_id" integer NULL"#,
        )
        .execute(&mut **tx)
        .await?;
        sqlx::query(
            r#"ALTER TABLE "dispatcharr_channels_stream" ADD COLUMN "stream_chno" real NULL"#,
        )
        .execute(&mut **tx)
        .await?;
        populate(tx).await?;
    }
    for sql in SQL[index].lines() {
        sqlx::query(sql).execute(&mut **tx).await?;
    }
    record(tx, (channels_flags::APP, STEPS[index])).await
}

struct LegacyStream {
    id: i64,
    name: String,
    tvg_id: Option<String>,
    m3u_account_id: Option<i64>,
    account_type: Option<String>,
    group_name: Option<String>,
    stream_hash: Option<String>,
    custom_properties: Option<String>,
}

async fn hash_keys(tx: &mut Transaction<'_, sqlx::Sqlite>) -> Result<String, Failure> {
    let setting: Option<String> =
        sqlx::query_scalar("SELECT value FROM core_coresettings WHERE key='stream_settings'")
            .fetch_optional(&mut **tx)
            .await?;
    let Some(setting) = setting else {
        return Ok(String::new());
    };
    let payload: Value = serde_json::from_str(&setting).map_err(|_| Failure::OutsideKnownState)?;
    if !crate::channels::truthy(Some(&payload)) {
        return Ok(String::new());
    }
    let value = payload
        .as_object()
        .ok_or(Failure::OutsideKnownState)?
        .get("m3u_hash_key");
    if !crate::channels::truthy(value) {
        return Ok(String::new());
    }
    value
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or(Failure::OutsideKnownState)
}

fn stream_number(value: &Value) -> Option<f64> {
    match value {
        Value::Number(v) => v.to_string().parse::<f64>().ok(),
        Value::Bool(v) => Some(f64::from(u8::from(*v))),
        Value::String(text) => dispatcharr_ingest_core::source_float_literal(text),
        _ => None,
    }
}

async fn populate(tx: &mut Transaction<'_, sqlx::Sqlite>) -> Result<(), Failure> {
    let keys = hash_keys(tx).await?;
    let keys: Vec<_> = keys
        .split(',')
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .collect();
    let streams = sqlx::query(
        "SELECT s.id,s.name,s.tvg_id,s.m3u_account_id,a.account_type,g.name AS group_name,
                s.stream_hash,s.custom_properties
         FROM dispatcharr_channels_stream s
         LEFT JOIN m3u_m3uaccount a ON a.id=s.m3u_account_id
         LEFT JOIN dispatcharr_channels_channelgroup g ON g.id=s.channel_group_id
         ORDER BY s.updated_at DESC",
    )
    .fetch_all(&mut **tx)
    .await?;
    let mut seen_hashes = HashSet::new();
    let mut duplicates = Vec::new();
    for raw in streams {
        let row = LegacyStream {
            id: raw.try_get("id")?,
            name: raw.try_get("name")?,
            tvg_id: raw.try_get("tvg_id")?,
            m3u_account_id: raw.try_get("m3u_account_id")?,
            account_type: raw.try_get("account_type")?,
            group_name: raw.try_get("group_name")?,
            stream_hash: raw.try_get("stream_hash")?,
            custom_properties: raw.try_get("custom_properties")?,
        };
        let kind = row.account_type.as_deref().unwrap_or("");
        let xc = kind == "XC";
        let fields = row
            .custom_properties
            .as_deref()
            .map(serde_json::from_str::<Value>)
            .transpose()
            .map_err(|_| Failure::OutsideKnownState)?;
        let fields = fields.as_ref().and_then(Value::as_object);
        let raw_id = fields.and_then(|value| value.get("stream_id"));
        let provider = if xc && crate::channels::truthy(raw_id) {
            match crate::json_compat::python_json_int(raw_id.expect("truthy value is present")) {
                Ok(value) => Some(value.as_i64().ok_or(Failure::OutsideKnownState)?),
                Err(_) => None,
            }
        } else {
            None
        };
        let number = fields
            .and_then(|fields| {
                if xc {
                    fields.get("num")
                } else {
                    [
                        "tvg-chno",
                        "TVG-CHNO",
                        "tvg-Chno",
                        "Tvg-Chno",
                        "channel-number",
                        "Channel-Number",
                        "CHANNEL-NUMBER",
                    ]
                    .iter()
                    .find_map(|key| fields.get(*key))
                }
            })
            .and_then(stream_number);
        let recalculated = if xc && provider.is_some_and(|id| id != 0) && keys.contains(&"url") {
            let hash = stream_hash_nullable(
                &NullableStreamIdentity {
                    name: Some(&row.name),
                    url: None,
                    tvg_id: row.tvg_id.as_deref(),
                    m3u_id: row.m3u_account_id,
                    group: row.group_name.as_deref(),
                    account_type: kind,
                    stream_id: provider,
                },
                &keys,
            );
            if !seen_hashes.insert(hash.clone()) {
                duplicates.push(row.id);
                continue;
            }
            Some(hash)
        } else {
            None
        };
        if provider.is_some() || number.is_some() || recalculated.is_some() {
            sqlx::query("UPDATE dispatcharr_channels_stream SET stream_id=?,stream_chno=?,stream_hash=? WHERE id=?")
                .bind(provider).bind(number).bind(recalculated.or(row.stream_hash)).bind(row.id)
                .execute(&mut **tx).await?;
        }
    }
    for id in duplicates {
        // The pinned Django foreign key cascades the sole incoming relation.
        // SQLite's table-rebuild transaction keeps foreign keys disabled.
        sqlx::query("DELETE FROM dispatcharr_channels_channelstream WHERE stream_id=?")
            .bind(id)
            .execute(&mut **tx)
            .await?;
        sqlx::query("DELETE FROM dispatcharr_channels_stream WHERE id=?")
            .bind(id)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "sqlite_channels_identity_contract.rs"]
mod contract;
