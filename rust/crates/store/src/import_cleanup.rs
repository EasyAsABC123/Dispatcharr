//! Original Dispatcharr cleanup_streams semantics, including overlapping counts.
//! Full application and algorithm credit belongs to the original authors.
use crate::{Store, StoreError};
use chrono::{DateTime, Datelike, TimeDelta, Utc};
use serde::Serialize;
use serde_json::json;
use sqlx::Row;
const MAX_CLEANUP_ROWS: i64 = 1_000_000;
fn cutoff(scan: DateTime<Utc>, days: i64) -> Result<DateTime<Utc>, StoreError> {
    TimeDelta::try_days(days)
        .and_then(|days| scan.checked_sub_signed(days))
        .filter(|date| (1..=9999).contains(&date.year()))
        .ok_or_else(|| StoreError::InvalidInput(json!({"detail":"Invalid stream stale cutoff."})))
}
#[derive(Clone, Debug, serde::Deserialize)]
pub struct GroupCleanupAccount {
    pub id: i64,
    pub locked: bool,
    pub name: String,
    pub stale_stream_days: i64,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct GroupCleanupCounts {
    pub removed_relationships: u64,
    pub removed_groups: u64,
}
const DISABLED:&str="NOT EXISTS(SELECT 1 FROM dispatcharr_channels_channelgroupm3uaccount r WHERE r.m3u_account_id=s.m3u_account_id AND r.channel_group_id=s.channel_group_id AND r.enabled)";
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct StreamCleanupCounts {
    pub disabled_group_matches: u64,
    pub stale_matches: u64,
    pub reported_total: u64,
    pub deleted_streams: u64,
}
impl Store {
    /// The two source deletions are separate transactions. Both source counts
    /// are taken first, so an old stream in a disabled group is counted twice
    /// in reported_total even though it is physically deleted only once.
    pub async fn cleanup_m3u_streams(
        &self,
        account_id: i64,
        scan_time: DateTime<Utc>,
    ) -> Result<StreamCleanupCounts, StoreError> {
        let days: Option<i64> = sqlx::query_scalar(
            "SELECT stale_stream_days::bigint FROM m3u_m3uaccount WHERE id=$1 AND is_active",
        )
        .bind(account_id)
        .fetch_optional(&self.pool)
        .await?;
        let days = days.ok_or(StoreError::NotFound)?;
        let cutoff = cutoff(scan_time, days)?;
        let counts=sqlx::query(&format!("SELECT count(*) FILTER(WHERE {DISABLED})::bigint AS disabled,count(*) FILTER(WHERE s.last_seen<$2)::bigint AS stale,count(*) FILTER(WHERE {DISABLED} OR s.last_seen<$2)::bigint AS unique_rows FROM dispatcharr_channels_stream s WHERE s.m3u_account_id=$1"))
            .bind(account_id).bind(cutoff).fetch_one(&self.pool).await?;
        if counts.try_get::<i64, _>("unique_rows")? > MAX_CLEANUP_ROWS {
            return Err(StoreError::InvalidInput(
                json!({"detail":"Stream cleanup capacity exceeded."}),
            ));
        }
        let disabled = counts.try_get::<i64, _>("disabled")? as u64;
        let stale = counts.try_get::<i64, _>("stale")? as u64;
        let mut deleted = 0_u64;
        for predicate in [DISABLED, "s.last_seen<$2"] {
            let mut tx = self.pool.begin().await?;
            loop {
                // Bounded row-id batches within each original transaction;
                // stream deletion uses the existing collector/rollup port.
                let ids:Vec<i64>=sqlx::query_scalar(&format!("SELECT s.id::bigint FROM dispatcharr_channels_stream s WHERE s.m3u_account_id=$1 AND {predicate} AND $2::timestamptz IS NOT NULL ORDER BY s.id LIMIT 1000 FOR UPDATE OF s"))
                    .bind(account_id).bind(cutoff).fetch_all(&mut *tx).await?;
                if ids.is_empty() {
                    break;
                }
                deleted = deleted
                    .checked_add(ids.len() as u64)
                    .filter(|total| *total <= MAX_CLEANUP_ROWS as u64)
                    .ok_or_else(|| {
                        StoreError::InvalidInput(
                            json!({"detail":"Stream cleanup capacity exceeded."}),
                        )
                    })?;
                crate::stream_writes::remove(&mut tx, &ids).await?;
            }
            tx.commit().await?;
        }
        Ok(StreamCleanupCounts {
            disabled_group_matches: disabled,
            stale_matches: stale,
            reported_total: disabled + stale,
            deleted_streams: deleted,
        })
    }

    /// Source receives an account snapshot. The locked `custom` account skips
    /// cleanup before date arithmetic. Streams/overrides alone do not protect
    /// an orphan group; the original collector sets those references to NULL.
    pub async fn cleanup_m3u_group_relationships(
        &self,
        account: &GroupCleanupAccount,
        scan_time: DateTime<Utc>,
    ) -> Result<GroupCleanupCounts, StoreError> {
        if account.locked && account.name == "custom" {
            return Ok(GroupCleanupCounts::default());
        }
        let cutoff = cutoff(scan_time, account.stale_stream_days)?;
        let stale=sqlx::query("SELECT channel_group_id::bigint AS group_id FROM dispatcharr_channels_channelgroupm3uaccount WHERE m3u_account_id=$1 AND last_seen<$2 LIMIT 32769")
            .bind(account.id).bind(cutoff).fetch_all(&self.pool).await?;
        if stale.len() > 32768 {
            return Err(StoreError::InvalidInput(
                json!({"detail":"Group cleanup capacity exceeded."}),
            ));
        }
        if stale.is_empty() {
            return Ok(GroupCleanupCounts::default());
        }
        let group_ids = stale
            .iter()
            .map(|row| row.try_get::<i64, _>("group_id"))
            .collect::<Result<Vec<_>, _>>()?;
        // Preserve the separate original relationship-delete commit even if
        // the subsequent orphan-group collector fails.
        sqlx::query("DELETE FROM dispatcharr_channels_channelgroupm3uaccount WHERE m3u_account_id=$1 AND last_seen<$2")
            .bind(account.id).bind(cutoff).execute(&self.pool).await?;
        let mut tx = self.pool.begin().await?;
        let orphans:Vec<i64>=sqlx::query_scalar("SELECT g.id::bigint FROM dispatcharr_channels_channelgroup g WHERE g.id=ANY($1) AND NOT EXISTS(SELECT 1 FROM dispatcharr_channels_channelgroupm3uaccount r WHERE r.channel_group_id=g.id) AND NOT EXISTS(SELECT 1 FROM dispatcharr_channels_channel c WHERE c.channel_group_id=g.id) FOR UPDATE OF g")
            .bind(group_ids).fetch_all(&mut *tx).await?;
        if !orphans.is_empty() {
            sqlx::query("UPDATE dispatcharr_channels_stream SET channel_group_id=NULL WHERE channel_group_id=ANY($1)")
                .bind(&orphans).execute(&mut *tx).await?;
            sqlx::query("UPDATE dispatcharr_channels_channeloverride SET channel_group_id=NULL WHERE channel_group_id=ANY($1)")
                .bind(&orphans).execute(&mut *tx).await?;
            sqlx::query("DELETE FROM dispatcharr_channels_channelgroup WHERE id=ANY($1)")
                .bind(&orphans)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(GroupCleanupCounts {
            removed_relationships: stale.len() as u64,
            removed_groups: orphans.len() as u64,
        })
    }
}
