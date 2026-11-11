//! Source-derived cached provider-info extension and host controls.
//! Actual Source90 TCP and owned-resource gates passed; canonical app,
//! provider refresh, image serving and full React modal gates are separate.
//! Original Dispatcharr authors retain application/algorithm credit.
use super::{
    vod_api_projection::truthy,
    vod_api_unified_query::{self as unified_query, QueryFault},
};
use chrono::{DateTime, Utc};
use serde_json::Value;
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Eligibility {
    Cached,
    RefreshRequired,
}
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Fault {
    Framework,
    Series(String),
    Capacity,
    Uncaptured(&'static str),
}
fn last<'a>(query: &'a [(String, String)], key: &str) -> Option<&'a str> {
    query
        .iter()
        .rev()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.as_str())
}
fn flag(query: &[(String, String)], key: &str, default: &str) -> bool {
    last(query, key).unwrap_or(default).to_lowercase() == "true"
}
pub(super) fn include_episodes(query: &[(String, String)]) -> bool {
    flag(query, "include_episodes", "true")
}
fn properties(relation: &Value) -> Result<Value, Fault> {
    let raw = &relation["custom_properties"];
    if !truthy(raw) {
        return Ok(serde_json::json!({}));
    }
    if !raw.is_object() {
        return Err(Fault::Framework);
    }
    Ok(raw.clone())
}
fn timestamp(raw: &Value) -> Result<Option<DateTime<Utc>>, Fault> {
    if raw.is_null() {
        return Ok(None);
    }
    DateTime::parse_from_rfc3339(raw.as_str().ok_or(Fault::Framework)?)
        .map(|value| Some(value.with_timezone(&Utc)))
        .map_err(|_| Fault::Framework)
}
pub(super) fn movie(
    relation: &Value,
    query: &[(String, String)],
    now: DateTime<Utc>,
) -> Result<Eligibility, Fault> {
    let props = properties(relation)?;
    let last = timestamp(&relation["last_advanced_refresh"])?;
    Ok(
        if flag(query, "force_refresh", "false")
            || !truthy(&props["detailed_fetched"])
            || last.is_none_or(|last| {
                now.signed_duration_since(last) > chrono::Duration::seconds(86400)
            })
        {
            Eligibility::RefreshRequired
        } else {
            Eligibility::Cached
        },
    )
}
pub(super) fn series(
    relation: &Value,
    query: &[(String, String)],
    now: DateTime<Utc>,
) -> Result<Eligibility, Fault> {
    // Source parses the interval before looking at flags. It constructs timedelta
    // only in the final elif: huge intervals do NOT fail when refresh is forced.
    let hours = unified_query::integer(last(query, "refresh_interval").unwrap_or("24")).map_err(
        |error| match error {
            QueryFault::View(message) => Fault::Series(message),
            QueryFault::Capacity => Fault::Capacity,
            QueryFault::Uncaptured(message) => Fault::Uncaptured(message),
        },
    )?;
    let props = properties(relation)?;
    if !truthy(&props["episodes_fetched"])
        || !truthy(&props["detailed_fetched"])
        || flag(query, "force_refresh", "false")
    {
        return Ok(Eligibility::RefreshRequired);
    }
    let Some(last) = timestamp(&relation["last_episode_refresh"])? else {
        return Ok(Eligibility::RefreshRequired);
    };
    let hours = hours
        .to_string()
        .parse::<i64>()
        .map_err(|_| Fault::Series("Python int too large to convert to C int".into()))?;
    // CPython timedelta's normalized day bound is independent of its integer
    // conversion bound. Until actual Source corpus captures that message this
    // grammar returns an explicit unsupported result, never a cached false200.
    if !(-23_999_999_976..=23_999_999_999).contains(&hours) {
        return Err(Fault::Uncaptured("timedelta normalized-day overflow"));
    }
    Ok(
        if now.signed_duration_since(last) > chrono::Duration::hours(hours) {
            Eligibility::RefreshRequired
        } else {
            Eligibility::Cached
        },
    )
}
