//! Original Dispatcharr core helper read APIs. Original authors retain credit.
//! The timezone dataset is pinned to the original runtime, not the host OS.
use crate::{AuthService, StoreError};
use axum::{
    extract::{ConnectInfo, RawQuery, State},
    http::HeaderMap,
    routing::get,
    Json, Router,
};
use chrono::{DateTime, SecondsFormat, Utc};
use num_bigint::BigInt;
use serde_json::{json, Map, Value};
use sqlx::Row;
use std::{net::SocketAddr, sync::OnceLock};

fn timezone_data() -> &'static Value {
    static DATA: OnceLock<Value> = OnceLock::new();
    DATA.get_or_init(|| {
        let zones: Vec<String> =
            serde_json::from_str(include_str!("../datasets/pytz-common-timezones.json"))
                .expect("pinned pytz dataset");
        let mut grouped = Map::new();
        for zone in &zones {
            let region = zone.split_once('/').map_or("Other", |(region, _)| region);
            grouped
                .entry(region)
                .or_insert_with(|| json!([]))
                .as_array_mut()
                .expect("timezone group")
                .push(zone.clone().into());
        }
        json!({"count":zones.len(),"timezones":zones,"grouped":grouped})
    })
}
async fn timezones(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Json<Value>, StoreError> {
    auth.account_user(peer, headers, false).await?;
    Ok(Json(timezone_data().clone()))
}
fn event_error() -> StoreError {
    StoreError::StatusBody(500, json!({"error":"Failed to fetch system events"}))
}
// Python int(query_string) accepts underscore separators but not decimal fractions.
fn integer(raw: &str) -> Result<BigInt, StoreError> {
    let raw = raw.trim();
    let digits = raw.strip_prefix(['+', '-']).unwrap_or(raw);
    if digits.is_empty()
        || digits.starts_with('_')
        || digits.ends_with('_')
        || digits.contains("__")
        || !digits.bytes().all(|v| v.is_ascii_digit() || v == b'_')
        || digits.bytes().filter(u8::is_ascii_digit).count() > 4300
    {
        return Err(event_error());
    }
    BigInt::parse_bytes(raw.replace('_', "").as_bytes(), 10).ok_or_else(event_error)
}
struct Page {
    limit: BigInt,
    offset: BigInt,
    fetch: Option<(i64, i64)>,
    kind: Option<String>,
}
fn page(raw: Option<String>) -> Result<Page, StoreError> {
    let params: std::collections::HashMap<String, String> =
        url::form_urlencoded::parse(raw.as_deref().unwrap_or("").as_bytes())
            .into_owned()
            .collect();
    let limit = integer(params.get("limit").map_or("100", String::as_str))?.min(1000.into());
    let offset = integer(params.get("offset").map_or("0", String::as_str))?;
    let stop = &offset + &limit;
    if offset < BigInt::from(0) || stop < BigInt::from(0) {
        return Err(event_error());
    }
    let fetch = if limit <= 0.into() {
        None
    } else {
        Some((
            limit.to_string().parse().map_err(|_| event_error())?,
            offset.to_string().parse().map_err(|_| event_error())?,
        ))
    };
    Ok(Page {
        limit,
        offset,
        fetch,
        kind: params.get("event_type").filter(|v| !v.is_empty()).cloned(),
    })
}
fn display(kind: &str) -> &str {
    match kind {
        "channel_start" => "Channel Started",
        "channel_stop" => "Channel Stopped",
        "channel_buffering" => "Channel Buffering",
        "channel_failover" => "Channel Failover",
        "channel_reconnect" => "Channel Reconnected",
        "channel_error" => "Channel Error",
        "client_connect" => "Client Connected",
        "client_disconnect" => "Client Disconnected",
        "recording_start" => "Recording Started",
        "recording_end" => "Recording Ended",
        "stream_switch" => "Stream Switched",
        "m3u_refresh" => "M3U Refreshed",
        "m3u_download" => "M3U Downloaded",
        "m3u_error" => "M3U Error",
        "epg_refresh" => "EPG Refreshed",
        "epg_download" => "EPG Downloaded",
        "epg_error" => "EPG Error",
        "login_success" => "Login Successful",
        "login_failed" => "Login Failed",
        "logout" => "User Logged Out",
        "m3u_blocked" => "M3U Download Blocked",
        "epg_blocked" => "EPG Download Blocked",
        "vod_start" => "VOD Started",
        "vod_stop" => "VOD Stopped",
        other => other,
    }
}
async fn event_data(auth: &AuthService, raw: Option<String>) -> Result<Value, StoreError> {
    let page = page(raw)?;
    let total: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM core_systemevent WHERE ($1::text IS NULL OR event_type=$1)",
    )
    .bind(&page.kind)
    .fetch_one(auth.store.pool())
    .await?;
    let mut events = Vec::new();
    if let Some((limit, offset)) = page.fetch {
        let rows=sqlx::query("SELECT id::bigint AS id,event_type,timestamp,channel_id,channel_name,details FROM core_systemevent WHERE ($1::text IS NULL OR event_type=$1) ORDER BY timestamp DESC LIMIT $2 OFFSET $3").bind(&page.kind).bind(limit).bind(offset).fetch_all(auth.store.pool()).await?;
        for row in rows {
            let kind: String = row.try_get("event_type")?;
            let timestamp: DateTime<Utc> = row.try_get("timestamp")?;
            let timestamp = timestamp.to_rfc3339_opts(
                if timestamp.timestamp_subsec_micros() == 0 {
                    SecondsFormat::Secs
                } else {
                    SecondsFormat::Micros
                },
                false,
            );
            events.push(json!({"id":row.try_get::<i64,_>("id")?,"event_type":kind,"event_type_display":display(&kind),"timestamp":timestamp,"channel_id":row.try_get::<Option<uuid::Uuid>,_>("channel_id")?,"channel_name":row.try_get::<Option<String>,_>("channel_name")?,"details":row.try_get::<Value,_>("details")?}));
        }
    }
    Ok(
        json!({"events":events,"count":events.len(),"total":total,"offset":serde_json::from_str::<Value>(&page.offset.to_string()).expect("integer"),"limit":serde_json::from_str::<Value>(&page.limit.to_string()).expect("integer")}),
    )
}
async fn events(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Result<Json<Value>, StoreError> {
    auth.account_user(peer, headers, true).await?;
    event_data(&auth, raw)
        .await
        .map(Json)
        .map_err(|_| event_error())
}
pub(crate) fn router(auth: AuthService) -> Router {
    Router::new()
        .route("/api/core/timezones/", get(timezones))
        .route("/api/core/system-events/", get(events))
        .with_state(auth)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pinned_zones_are_sorted_grouped_and_complete() {
        let data = timezone_data();
        let zones = data["timezones"].as_array().unwrap();
        assert_eq!(zones.len(), 433);
        assert!(zones.windows(2).all(|w| w[0].as_str() < w[1].as_str()));
        assert_eq!(
            data["grouped"]
                .as_object()
                .unwrap()
                .values()
                .map(|v| v.as_array().unwrap().len())
                .sum::<usize>(),
            433
        );
        assert!(data["grouped"]["Other"]
            .as_array()
            .unwrap()
            .contains(&json!("UTC")));
    }
    #[test]
    fn source_pagination_retains_empty_negative_slice() {
        let p = page(Some("offset=10&limit=-1".into())).unwrap();
        assert!(p.fetch.is_none());
        assert_eq!(p.limit, (-1).into());
        assert!(page(Some("limit=-1".into())).is_err());
        assert!(page(Some("limit=1.0".into())).is_err());
        let p = page(Some("limit=9_999&offset=+2&event_type=&limit=2000".into())).unwrap();
        assert_eq!(p.fetch, Some((1000, 2)));
        assert_eq!(p.kind, None);
    }
}
