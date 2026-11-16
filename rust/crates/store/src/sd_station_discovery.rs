//! Source stations-only Schedules Direct discovery; no guide/program ingestion.
//! Original Dispatcharr authors retain all application and algorithm credit.
use super::{
    sd_guide::{Error, StationOutcome},
    sd_guide_auth::{self as auth, Source},
    sd_guide_http as http,
    sd_guide_progress::Progress,
};
use crate::StoreError;
use serde_json::{json, Value};
use std::{collections::HashMap, time::Duration};

const NO_LINEUPS:&str="No lineups configured. Use the Manage Lineups option in the EPG source settings to add a lineup.";
const NO_STATIONS: &str = "No stations found across all Schedules Direct lineups.";
// The same rows/types used by the actual production .bind calls below are
// exposed to the existing schema peer; no copied test SQL is needed.
pub(super) const EXISTING: &str = "SELECT tvg_id FROM epg_epgdata WHERE epg_source_id=$1";
pub(super) const INSERT:&str="INSERT INTO epg_epgdata(tvg_id,name,icon_url,epg_source_id) SELECT x.sid,x.name,x.logo,$1 FROM jsonb_to_recordset($2) x(sid text,name text,logo text) ON CONFLICT DO NOTHING";
pub(super) const UPDATE:&str="UPDATE epg_epgdata e SET name=x.name,icon_url=x.logo FROM jsonb_to_recordset($2) x(sid text,name text,logo text) WHERE e.epg_source_id=$1 AND e.tvg_id=x.sid AND (e.name IS DISTINCT FROM x.name OR e.icon_url IS DISTINCT FROM x.logo)";

#[derive(Debug, Clone, PartialEq)]
struct Station {
    sid: String,
    name: String,
    logo: Option<String>,
}
fn text(value: &Value) -> Result<&str, Error> {
    value.as_str().ok_or(Error::Unsupported)
}
fn metadata(value: &Value, style: &str) -> Result<Vec<Station>, Error> {
    let stations = value.get("stations").unwrap_or(&Value::Null);
    let empty = Vec::new();
    let stations = match stations {
        Value::Null => &empty,
        Value::Array(v) => v,
        _ => return Err(Error::Unsupported),
    };
    let mut output = Vec::new();
    for station in stations {
        let Some(sid) = station.get("stationID") else {
            continue;
        };
        if !crate::xc_live_provider::truthy(sid) {
            continue;
        }
        let sid = text(sid)?.to_owned();
        let name = station
            .get("name")
            .filter(|v| crate::xc_live_provider::truthy(v))
            .unwrap_or(station.get("stationID").ok_or(Error::Unsupported)?);
        // General Python str(non-string) metadata remains explicitly outside this
        // initial provider-text domain; never silently stringify with JSON spelling.
        let name = text(name)?.chars().take(512).collect();
        let logos = station
            .get("stationLogo")
            .filter(|v| crate::xc_live_provider::truthy(v))
            .or_else(|| station.get("logo"))
            .unwrap_or(&Value::Null);
        let selected = match logos {
            Value::Array(v) if !v.is_empty() => Some(
                v.iter()
                    .find(|v| v.get("category").and_then(Value::as_str) == Some(style))
                    .unwrap_or(&v[0]),
            ),
            Value::Object(_) => Some(logos),
            Value::Null | Value::Array(_) => None,
            _ => return Err(Error::Unsupported),
        };
        let logo = selected
            .and_then(|v| {
                v.get("URL")
                    .filter(|v| crate::xc_live_provider::truthy(v))
                    .or_else(|| v.get("url"))
            })
            .filter(|v| !v.is_null())
            .map(text)
            .transpose()?
            .map(str::to_owned)
            .filter(|v| v.chars().count() <= 500);
        output.push(Station { sid, name, logo });
    }
    Ok(output)
}
fn offline(value: &Value) -> bool {
    let one = [value.get("systemStatus").cloned().unwrap_or(Value::Null)];
    let values = one[0].as_array().map(Vec::as_slice).unwrap_or(&one);
    values.iter().any(|v| {
        v.get("status")
            .unwrap_or(v)
            .as_str()
            .is_some_and(|s| s.trim().eq_ignore_ascii_case("offline"))
    })
}
fn caught(error: &auth::Error) -> bool {
    matches!(error, auth::Error::Http(http::Error::Transport))
}

/// Caller retains the actual claimed task, admitted SQL session, revocation
/// closure and shared Source auth/cache. No independent worker/connection pool.
pub(super) async fn discover(
    source: &mut Source,
    progress: &Progress,
    stopped: &(dyn Fn() -> bool + Send + Sync),
) -> Result<StationOutcome, Error> {
    let result = source.obtain().await?;
    let Some(mut token) = result.token.clone() else {
        return Ok(StationOutcome::Auth(result));
    };
    if stopped() {
        return Err(Error::Operational);
    }
    match source
        .authorized(
            reqwest::Method::GET,
            "/status",
            &mut token,
            None,
            Duration::from_secs(30),
        )
        .await
    {
        Ok(reply) if reply.status < 400 => {
            if reply.value.as_ref().is_some_and(offline) {
                return Ok(StationOutcome::Idle("Schedules Direct system is currently offline. Do not retry for at least 1 hour.".into()));
            }
        }
        Ok(_) => {}
        Err(e) if caught(&e) => {}
        Err(e) => return Err(e.into()),
    }
    progress
        .parsing(10, Some("Fetching subscribed lineups...".into()))
        .await;
    let reply = match source
        .authorized(
            reqwest::Method::GET,
            "/lineups",
            &mut token,
            None,
            Duration::from_secs(30),
        )
        .await
    {
        Ok(r) => r,
        Err(e) if caught(&e) => {
            return Ok(StationOutcome::Error(
                "Failed to fetch Schedules Direct lineups: Connection error".into(),
            ))
        }
        Err(e) => return Err(e.into()),
    };
    if reply.status == 400
        && reply
            .value
            .as_ref()
            .and_then(|v| v.get("code"))
            .and_then(Value::as_i64)
            == Some(4102)
    {
        return Ok(StationOutcome::Idle(NO_LINEUPS.into()));
    }
    if let Some(error) = reply.request_error() {
        return Ok(StationOutcome::Error(format!(
            "Failed to fetch Schedules Direct lineups: {error}"
        )));
    }
    let body = reply.value.ok_or(Error::Unsupported)?;
    let lineups = match body.get("lineups") {
        None => Vec::new(),
        Some(Value::Array(v)) => v
            .iter()
            .filter(|v| {
                !v.get("isDeleted")
                    .is_some_and(crate::xc_live_provider::truthy)
            })
            .collect(),
        _ => return Err(Error::Unsupported),
    };
    if lineups.is_empty() {
        return Ok(StationOutcome::Idle(NO_LINEUPS.into()));
    }
    progress
        .parsing(
            18,
            Some(format!(
                "Fetching station metadata for {} lineup(s)...",
                lineups.len()
            )),
        )
        .await;
    let style = source
        .properties
        .get("logo_style")
        .map(text)
        .transpose()?
        .unwrap_or("dark")
        .to_owned();
    let mut stations = Vec::<Station>::new();
    let mut index = HashMap::<String, usize>::new();
    for lineup in lineups {
        if stopped() {
            return Err(Error::Operational);
        }
        let Some(id) = lineup
            .get("lineupID")
            .filter(|v| crate::xc_live_provider::truthy(v))
            .or_else(|| lineup.get("lineup"))
        else {
            continue;
        };
        if !crate::xc_live_provider::truthy(id) {
            continue;
        }
        let id = text(id)?;
        let reply = match source
            .authorized(
                reqwest::Method::GET,
                &format!("/lineups/{id}"),
                &mut token,
                None,
                Duration::from_secs(30),
            )
            .await
        {
            Ok(r) => r,
            Err(e) if caught(&e) => continue,
            Err(e) => return Err(e.into()),
        };
        if reply.status >= 400 {
            continue;
        }
        for station in metadata(&reply.value.ok_or(Error::Unsupported)?, &style)? {
            if let Some(at) = index.get(&station.sid) {
                stations[*at] = station
            } else {
                if stations.len() >= 65536 {
                    return Err(Error::Capacity);
                }
                index.insert(station.sid.clone(), stations.len());
                stations.push(station);
            }
        }
    }
    if stations.is_empty() {
        return Ok(StationOutcome::Error(NO_STATIONS.into()));
    }
    let count = stations.len() as i64;
    let id = source.id;
    source
        .session
        .run(move |c| {
            Box::pin(async move {
                sqlx::query("UPDATE epg_epgsource SET status='parsing',last_message=$2 WHERE id=$1")
                    .bind(id)
                    .bind(format!("Syncing {count} stations..."))
                    .execute(c)
                    .await
                    .map(|_| ())
                    .map_err(StoreError::from)
            })
        })
        .await??;
    progress
        .parsing(
            28,
            Some(format!(
                "Syncing {} stations to database...",
                stations.len()
            )),
        )
        .await;
    // Source snapshots existing identities before independent bulk_create and
    // bulk_update calls. New rows racing after this snapshot are not overwritten.
    let existing: Vec<Option<String>> = source
        .session
        .run(move |c| {
            Box::pin(async move {
                sqlx::query_scalar(EXISTING)
                    .bind(id)
                    .fetch_all(c)
                    .await
                    .map_err(StoreError::from)
            })
        })
        .await??;
    let existing = existing
        .into_iter()
        .flatten()
        .collect::<std::collections::HashSet<_>>();
    let mut creates = Vec::new();
    let mut updates = Vec::new();
    for station in &stations {
        let row = json!({"sid":station.sid,"name":station.name,"logo":station.logo});
        if existing.contains(&station.sid) {
            updates.push(row)
        } else {
            creates.push(row)
        }
    }
    for (sql, rows) in [(INSERT, creates), (UPDATE, updates)] {
        if stopped() {
            return Err(Error::Operational);
        }
        if !rows.is_empty() {
            source
                .session
                .run(move |c| {
                    Box::pin(async move {
                        sqlx::query(sql)
                            .bind(id)
                            .bind(json!(rows))
                            .execute(c)
                            .await
                            .map(|_| ())
                            .map_err(StoreError::from)
                    })
                })
                .await??;
        }
    }
    progress
        .parsing(
            30,
            Some(format!(
                "Stations synced ({} stations). Preparing schedule fetch...",
                stations.len()
            )),
        )
        .await;
    Ok(StationOutcome::Success(stations.len() as u64))
}
#[cfg(test)]
mod controls {
    use super::*;
    #[test]
    fn text_metadata_logo_preference_and_lengths() {
        let rows=metadata(&json!({"stations":[{"stationID":"1","name":"x".repeat(513),"stationLogo":[{"category":"light","URL":"other"},{"category":"dark","url":"chosen"}]},{"stationID":"2","name":"","logo":{"URL":"x".repeat(501)}}]}),"dark").unwrap();
        assert_eq!(rows[0].name.chars().count(), 512);
        assert_eq!(rows[0].logo.as_deref(), Some("chosen"));
        assert_eq!(rows[1].name, "2");
        assert_eq!(rows[1].logo, None);
    }
    #[test]
    fn malformed_metadata_is_not_silently_empty() {
        assert!(metadata(&json!({"stations":"oops"}), "dark").is_err());
        assert!(metadata(&json!({"stations":[{"stationID":"1","name":10}]}), "dark").is_err());
    }
}
