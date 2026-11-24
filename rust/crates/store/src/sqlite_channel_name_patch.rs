//! Ordinary React string-name PATCH on the admitted manual one-stream domain.
//! Original Dispatcharr, Django and DRF authors retain contract and application credit.
use super::single_from_stream::{Body, Owner};
use crate::{StoreError, User};
use axum::{
    extract::{ConnectInfo, OriginalUri, Path, State},
    http::HeaderMap,
    response::Response,
};
use serde_json::json;
use sqlx::{Connection, Row, SqliteConnection};
use std::{collections::HashMap, net::SocketAddr};
use url::Url;

pub(crate) async fn handle<O: Owner>(
    State(owner): State<O>,
    Path(raw): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    body: Body,
) -> Result<Response, StoreError> {
    owner
        .request()?
        .channel_name_patch(peer, headers, uri, raw, body)
        .await
}
fn outside() -> StoreError {
    StoreError::Unsupported("Copied SQLite name PATCH supports only a string name on a manual one-stream channel with an already custom group and no Guide/override work")
}

/// BEGIN IMMEDIATE holds object qualification, update, snapshot and commit together.
/// Stock manual-channel save signals do no compact/Guide/event work in this domain;
/// mark_custom's get_or_create resolves the existing locked custom-account relation.
pub(crate) async fn command(
    db: &mut SqliteConnection,
    user: &User,
    raw: String,
    body: Body,
    base: &Url,
    limit: usize,
) -> Result<Vec<u8>, StoreError> {
    let body = crate::admin_writes::body_object(body)?;
    let id: i64 = raw
        .parse()
        .map_err(|_| StoreError::StatusBody(404, json!({"detail":"Not found."})))?;
    let mut tx = db.begin_with("BEGIN IMMEDIATE").await?;
    let row=sqlx::query("SELECT auto_created,auto_created_by_id,channel_group_id,epg_data_id,logo_id,stream_profile_id FROM dispatcharr_channels_channel WHERE id=?").bind(id).fetch_optional(&mut *tx).await?.ok_or_else(crate::channels::missing)?;
    if body.keys().any(|k| !matches!(k.as_str(), "id" | "name")) {
        return Err(outside());
    }
    let name = body
        .get("name")
        .filter(|v| v.is_string())
        .ok_or_else(outside)?;
    // Python str.strip includes these four separators beyond Rust whitespace.
    // Keep that unqualified input outside this shared-validator string domain.
    if name
        .as_str()
        .is_some_and(|s| s.chars().any(|c| matches!(c, '\u{1c}'..='\u{1f}')))
    {
        return Err(outside());
    }
    let name = crate::admin_writes::character("name", name, Some(512), false)?;
    let group: Option<i64> = row.try_get("channel_group_id")?;
    if row.try_get::<bool, _>("auto_created")?
        || row
            .try_get::<Option<i64>, _>("auto_created_by_id")?
            .is_some()
        || row.try_get::<Option<i64>, _>("epg_data_id")?.is_some()
        || row.try_get::<Option<i64>, _>("logo_id")?.is_some()
        || row
            .try_get::<Option<i64>, _>("stream_profile_id")?
            .is_some()
    {
        return Err(outside());
    }
    let group = group.ok_or_else(outside)?;
    let eligible:bool=sqlx::query_scalar("SELECT (SELECT count(*) FROM dispatcharr_channels_channelstream WHERE channel_id=?1)=1 AND NOT EXISTS(SELECT 1 FROM dispatcharr_channels_channeloverride WHERE channel_id=?1) AND (SELECT count(*) FROM m3u_m3uaccount WHERE name='custom' AND locked)=1 AND EXISTS(SELECT 1 FROM dispatcharr_channels_channelgroupm3uaccount r JOIN m3u_m3uaccount a ON a.id=r.m3u_account_id WHERE r.channel_group_id=?2 AND a.name='custom' AND a.locked) AND NOT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='trigger' AND tbl_name='dispatcharr_channels_channel')").bind(id).bind(group).fetch_one(&mut *tx).await?;
    if !eligible {
        return Err(outside());
    }
    // Bound source-row material before the normal shared channel projection.
    let cbytes = crate::channels::FIELDS
        .iter()
        .filter(|f| **f != "streams")
        .map(|f| format!("COALESCE(length(CAST(c.{f} AS BLOB)),0)"))
        .collect::<Vec<_>>()
        .join("+");
    let sbytes = crate::channels::STREAM_FIELDS
        .iter()
        .map(|f| format!("COALESCE(length(CAST(s.{f} AS BLOB)),0)"))
        .collect::<Vec<_>>()
        .join("+");
    let bytes:i64=sqlx::query_scalar(&format!("SELECT {cbytes}+{sbytes}+COALESCE(length(CAST(g.name AS BLOB)),0)+COALESCE(length(CAST(a.name AS BLOB)),0) FROM dispatcharr_channels_channel c JOIN dispatcharr_channels_channelstream cs ON cs.channel_id=c.id JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id JOIN dispatcharr_channels_channelgroup g ON g.id=c.channel_group_id LEFT JOIN m3u_m3uaccount a ON a.id=s.m3u_account_id WHERE c.id=?")).bind(id).fetch_one(&mut *tx).await?;
    if bytes < 0 || bytes as usize > limit {
        return Err(outside());
    }
    sqlx::query("UPDATE dispatcharr_channels_channel SET name=?,updated_at=? WHERE id=?")
        .bind(name)
        .bind(chrono::Utc::now().naive_utc())
        .bind(id)
        .execute(&mut *tx)
        .await?;
    let value =
        crate::channels::sqlite_list::read(&mut tx, user, &HashMap::new(), base, Some(id)).await?;
    let encoded = crate::json_compat::compact_json_bytes(&value).map_err(|_| outside())?;
    let encoded = String::from_utf8(encoded)
        .map_err(|_| outside())?
        .replace("\u{7f}", "\\u007f")
        .into_bytes();
    if encoded.len() > limit {
        return Err(outside());
    }
    tx.commit().await?;
    Ok(encoded)
}
