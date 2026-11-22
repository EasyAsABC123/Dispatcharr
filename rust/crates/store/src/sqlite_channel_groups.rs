//! SQLite projections for the existing Channel Groups collection.
//! Original Dispatcharr authors and contributors retain application credit.

use super::{group_json, provider_json, truthy, StoreError, User};
use chrono::NaiveDateTime;
use serde_json::{json, Value};
use sqlx::{sqlite::SqliteRow, Connection, Row, SqliteConnection};
use std::collections::HashMap;

const COLLECT: &str = r"
    WITH visible_counts AS (
        SELECT COALESCE(o.channel_group_id,c.channel_group_id) AS gid, count(*) AS n
        FROM dispatcharr_channels_channel c
        LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id
        WHERE ?1 OR (c.user_level<=?2 AND (NOT ?3 OR NOT c.is_adult))
        GROUP BY COALESCE(o.channel_group_id,c.channel_group_id)
    )
    SELECT g.id, g.name, COALESCE(c.n,0) AS channel_count,
       EXISTS(SELECT 1 FROM dispatcharr_channels_channelgroupm3uaccount r
              JOIN m3u_m3uaccount a ON a.id=r.m3u_account_id
              WHERE r.channel_group_id=g.id AND a.locked) AS is_custom
    FROM dispatcharr_channels_channelgroup g
    LEFT JOIN visible_counts c ON c.gid=g.id
    WHERE ?1 OR c.gid IS NOT NULL
    ORDER BY g.id";

const PROVIDERS: &str = r"
    WITH counts AS (
        SELECT m3u_account_id,channel_group_id,count(*) AS n
        FROM dispatcharr_channels_stream
        WHERE channel_group_id IS NOT NULL
        GROUP BY m3u_account_id,channel_group_id
    )
    SELECT r.channel_group_id,r.enabled,r.auto_channel_sync,r.auto_sync_channel_start,
           r.auto_sync_channel_end,r.custom_properties,r.is_stale,r.last_seen,
           COALESCE(s.n,0) AS stream_count
    FROM dispatcharr_channels_channelgroupm3uaccount r
    JOIN m3u_m3uaccount a ON a.id=r.m3u_account_id
    LEFT JOIN counts s ON s.channel_group_id=r.channel_group_id AND s.m3u_account_id=r.m3u_account_id
    WHERE NOT a.locked AND (?1 IS NULL OR r.channel_group_id=?1)
    ORDER BY r.id";

pub(crate) const COLLECTION_SCHEMA_QUERIES: [&str; 2] = [COLLECT, PROVIDERS];

pub(crate) async fn collection(
    connection: &mut SqliteConnection,
    user: &User,
) -> Result<Vec<Value>, StoreError> {
    let admin = user.user_level >= 10;
    let mut snapshot = connection.begin().await?;
    let groups = sqlx::query(COLLECT)
        .bind(admin)
        .bind(user.user_level)
        .bind(truthy(user.custom_properties.get("hide_adult_content")))
        .fetch_all(&mut *snapshot)
        .await?;
    let mut providers: HashMap<i64, Vec<Value>> = HashMap::new();
    if admin && !groups.is_empty() {
        for row in sqlx::query(PROVIDERS)
            .bind(None::<i64>)
            .fetch_all(&mut *snapshot)
            .await?
        {
            providers
                .entry(row.try_get("channel_group_id")?)
                .or_default()
                .push(provider(row)?);
        }
    }
    snapshot.commit().await?;
    groups
        .into_iter()
        .map(|row| {
            let id = row.try_get("id")?;
            Ok(group_json(
                id,
                row.try_get("name")?,
                row.try_get("channel_count")?,
                row.try_get("is_custom")?,
                providers.remove(&id).unwrap_or_default(),
            ))
        })
        .collect()
}

fn provider(row: SqliteRow) -> Result<Value, StoreError> {
    let last_seen: NaiveDateTime = row.try_get("last_seen")?;
    let json_row = json!({
        "channel_group_id": row.try_get::<i64,_>("channel_group_id")?,
        "enabled": row.try_get::<bool,_>("enabled")?,
        "auto_channel_sync": row.try_get::<bool,_>("auto_channel_sync")?,
        "auto_sync_channel_start": row.try_get::<Option<f64>,_>("auto_sync_channel_start")?,
        "auto_sync_channel_end": row.try_get::<Option<f64>,_>("auto_sync_channel_end")?,
        "custom_properties": row.try_get::<Option<String>,_>("custom_properties")?,
        "is_stale": row.try_get::<bool,_>("is_stale")?,
        "last_seen": last_seen.and_utc().to_rfc3339(),
    });
    Ok(provider_json(&json_row, row.try_get("stream_count")?))
}

#[cfg(feature = "sqlite-copied-data-runtime")]
#[path = "sqlite_channel_group_writes.rs"]
pub(crate) mod writes;
