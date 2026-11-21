//! Pinned Original Channels 0040 on a detached historical SQLite copy.
//! Original Dispatcharr and Django contributors retain full credit.
use std::collections::BTreeMap;

use super::*;

pub(super) const STEP: &str = "0040_channelgroup_custom_relation";
const CATALOG: &str = include_str!("../datasets/sqlite-channels-custom-group-catalog.json");

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
    let catalog: BTreeMap<String, Vec<String>> =
        serde_json::from_str(CATALOG).map_err(|_| Failure::OutsideKnownState)?;
    for (table, expected) in catalog {
        let found = sqlx::query_scalar::<_, String>(
            "SELECT sql FROM sqlite_schema WHERE tbl_name=? AND sql IS NOT NULL ORDER BY type DESC,name LIMIT 16",
        )
        .bind(table)
        .fetch_all(&mut **tx)
        .await?;
        if found != expected {
            return Err(Failure::OutsideKnownState);
        }
    }
    Ok(())
}

async fn execute(tx: &mut Transaction<'_, sqlx::Sqlite>) -> Result<usize, Failure> {
    let names = sqlx::query_as::<_, (String, String)>(
        "SELECT CASE WHEN length(CAST(app AS BLOB))<=255 THEN app ELSE '' END,
                CASE WHEN length(CAST(name AS BLOB))<=255 THEN name ELSE '' END
         FROM django_migrations ORDER BY app,name LIMIT 181",
    )
    .fetch_all(&mut **tx)
    .await?;
    let applied = match names.len() {
        179 => false,
        180 => true,
        _ => return Err(Failure::OutsideKnownState),
    };
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
            .map(|name| (channels_flags::APP, *name)),
    );
    if applied {
        expected.push((channels_flags::APP, STEP));
    }
    expected.sort_unstable();
    if !names
        .iter()
        .map(|(app, name)| (app.as_str(), name.as_str()))
        .eq(expected)
    {
        return Err(Failure::OutsideKnownState);
    }
    check_layout(tx).await?;
    if !applied {
        let account = custom_owner(tx).await?;
        let group_ids = sqlx::query_scalar::<_, i64>(
            "SELECT id FROM dispatcharr_channels_channelgroup
             WHERE id IN (SELECT channel_group_id FROM dispatcharr_channels_channel
                          WHERE channel_group_id IS NOT NULL AND auto_created=0)
             ORDER BY id",
        )
        .fetch_all(&mut **tx)
        .await?;
        for group in group_ids {
            sqlx::query(
                "INSERT OR IGNORE INTO dispatcharr_channels_channelgroupm3uaccount(
                    channel_group_id,m3u_account_id,enabled,auto_channel_sync,auto_sync_channel_start,
                    auto_sync_channel_end,is_stale,last_seen,custom_properties)
                 VALUES(?,?,1,0,NULL,NULL,0,?,'{}')",
            )
            .bind(group).bind(account).bind(clock()).execute(&mut **tx).await?;
        }
        record(tx, (channels_flags::APP, STEP)).await?;
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

fn clock() -> String {
    let now = Utc::now().naive_utc();
    let format = if now.and_utc().timestamp_subsec_micros() == 0 {
        "%Y-%m-%d %H:%M:%S"
    } else {
        "%Y-%m-%d %H:%M:%S%.6f"
    };
    now.format(format).to_string()
}

async fn custom_owner(tx: &mut Transaction<'_, sqlx::Sqlite>) -> Result<i64, Failure> {
    let existing = sqlx::query_as::<_, (i64, bool)>(
        "SELECT id,locked FROM m3u_m3uaccount WHERE name='custom'",
    )
    .fetch_optional(&mut **tx)
    .await?;
    if let Some((id, locked)) = existing {
        if locked {
            return Ok(id);
        }
        let mut next = 2_u64;
        let mut name = "custom (user)".to_owned();
        loop {
            let occupied = sqlx::query_scalar::<_, i64>(
                "SELECT EXISTS(SELECT 1 FROM m3u_m3uaccount WHERE name=?)",
            )
            .bind(&name)
            .fetch_one(&mut **tx)
            .await?;
            if occupied == 0 {
                break;
            }
            name = format!("custom (user) {next}");
            next = next.checked_add(1).ok_or(Failure::OutsideKnownState)?;
        }
        sqlx::query("UPDATE m3u_m3uaccount SET name=? WHERE id=?")
            .bind(name)
            .bind(id)
            .execute(&mut **tx)
            .await?;
    }
    let agent: Option<i64> =
        sqlx::query_scalar("SELECT id FROM core_useragent ORDER BY id LIMIT 1")
            .fetch_optional(&mut **tx)
            .await?;
    let created = sqlx::query(
        "INSERT INTO m3u_m3uaccount(name,max_streams,is_active,locked,user_agent_id,created_at,updated_at,
                                   refresh_interval,stale_stream_days,account_type,status,priority,custom_properties)
         VALUES('custom',0,1,1,?,?,NULL,0,7,'STD','idle',0,'{}')",
    ).bind(agent).bind(clock()).execute(&mut **tx).await?.last_insert_rowid();
    sqlx::query(
        "INSERT INTO m3u_m3uaccountprofile(m3u_account_id,name,is_default,is_active,max_streams,search_pattern,
                                         replace_pattern,current_viewers,custom_properties,exp_date)
         VALUES(?,'custom Default',1,1,0,'^(.*)$','$1',0,'{}',NULL)",
    ).bind(created).execute(&mut **tx).await?;
    Ok(created)
}

#[cfg(test)]
#[path = "sqlite_channels_relations_contract.rs"]
mod contract;
