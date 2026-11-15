//! Actual SD token/status/delta/schedule/program requests and bounded ingestion.
//! Original Dispatcharr/Requests authors retain all pipeline algorithm credit.
use super::{
    sd_guide_auth::{self as auth, Source},
    sd_guide_http::{self as http, Reply},
    sd_guide_programme as programme,
    sd_guide_state::{self as state, State},
};
use serde_json::{json, Value};
use std::time::Duration;
#[derive(Debug)]
pub(super) enum Error {
    Auth(auth::Error),
    State(state::Error),
    Projection(programme::Error),
    Unsupported,
    Provider { status: u16, code: Option<i64> },
    Stopped,
}
impl From<auth::Error> for Error {
    fn from(e: auth::Error) -> Self {
        Self::Auth(e)
    }
}
impl From<state::Error> for Error {
    fn from(e: state::Error) -> Self {
        Self::State(e)
    }
}
impl From<programme::Error> for Error {
    fn from(e: programme::Error) -> Self {
        Self::Projection(e)
    }
}
pub(super) enum Outcome {
    AuthDeclined(auth::AuthResult),
    Offline,
    EmptyMapping,
    InvalidStations,
    InvalidSingle(i64),
    SkippedSingle {
        token: String,
    },
    Unchanged {
        token: String,
    },
    Md5Unavailable,
    NoSchedules,
    Written {
        programmes: u64,
        stations: u64,
        skipped_unmapped: u64,
        token: String,
    },
}
fn system_status(value: &Value) -> Result<bool, Error> {
    let Some(value) = value.get("systemStatus") else {
        return Ok(false);
    };
    let one = [value.clone()];
    let values = value.as_array().map(Vec::as_slice).unwrap_or(&one);
    for status in values {
        let status = if let Some(object) = status.as_object() {
            object.get("status").unwrap_or(&Value::Null)
        } else {
            status
        };
        match status {
            Value::String(s) => {
                if s.trim().eq_ignore_ascii_case("offline") {
                    return Ok(true);
                }
            }
            Value::Null => {}
            _ => {}
        }
    }
    Ok(false)
}
fn known_failure(error: &Error) -> bool {
    match error {
        Error::Auth(auth::Error::Http(http::Error::Transport)) => true,
        Error::Provider { status, code } => *status >= 400 || code.is_some_and(|c| c != 0),
        _ => false,
    }
}
async fn request_retry(
    source: &mut Source,
    token: &mut String,
    path: &str,
    payload: &Value,
    stopped: &(dyn Fn() -> bool + Send + Sync),
) -> Result<Reply, Error> {
    for attempt in 1..=3 {
        if stopped() {
            return Err(Error::Stopped);
        }
        let result = source
            .authorized(
                reqwest::Method::POST,
                path,
                token,
                Some(payload),
                Duration::from_secs(120),
            )
            .await
            .map_err(Error::from)
            .and_then(|reply| {
                if reply.status >= 400 || reply.payload_code.is_some_and(|n| n != 0) {
                    Err(Error::Provider {
                        status: reply.status,
                        code: reply.payload_code,
                    })
                } else {
                    Ok(reply)
                }
            });
        match result {
            Ok(reply) => return Ok(reply),
            Err(error) => {
                if !known_failure(&error)
                    || matches!(&error,Error::Provider{code:Some(code),..}if !matches!(*code,3000|3001))
                    || attempt == 3
                {
                    return Err(error);
                }
                // Source2s/4s backoff belongs to the SAME accepted physical task owner.
                tokio::time::sleep(Duration::from_secs(2 * attempt)).await;
            }
        }
    }
    unreachable!("three actual SD attempts")
}
/// The caller owns Source task lock/normal admission/SQL physical closure. It
/// must map actual outcomes to Source status/events and run post-refresh work;
/// this function never writes a backend result or pretends those effects ran.
pub(super) async fn ingest(
    source: &mut Source,
    state: &mut State,
    stopped: &(dyn Fn() -> bool + Send + Sync),
    progress: &super::sd_guide_progress::Progress,
) -> Result<Outcome, Error> {
    let auth = source.obtain().await?;
    let Some(mut token) = auth.token.clone() else {
        return Ok(Outcome::AuthDeclined(auth));
    };
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
            if let Some(value) = reply.value {
                if system_status(&value)? {
                    return Ok(Outcome::Offline);
                }
            }
        }
        Ok(_) => {}
        Err(auth::Error::Http(http::Error::Transport)) => {}
        Err(error) => return Err(error.into()),
    }
    state.refresh_mappings().await?;
    let (mapped, stations) = state.mapping_counts().await?;
    if stations == 0 {
        return Ok(match state.single {
            Some(id) => Outcome::InvalidSingle(id),
            None if mapped == 0 => Outcome::EmptyMapping,
            None => Outcome::InvalidStations,
        });
    }
    let mut country = None;
    match source
        .authorized(
            reqwest::Method::GET,
            "/lineups",
            &mut token,
            None,
            Duration::from_secs(30),
        )
        .await
    {
        Ok(reply) if reply.status < 400 => {
            if let Some(value) = reply.value {
                if let Some(lineups) = value.get("lineups").and_then(Value::as_array) {
                    for lineup in lineups {
                        let id = lineup
                            .get("lineupID")
                            .filter(|v| crate::xc_live_provider::truthy(v))
                            .or_else(|| lineup.get("lineup"))
                            .and_then(Value::as_str)
                            .unwrap_or("");
                        if let Some((prefix, _)) = id.split_once('-') {
                            country = Some(prefix.to_owned());
                            break;
                        }
                    }
                }
            }
        }
        Ok(_) => {}
        Err(auth::Error::Http(http::Error::Transport)) => {}
        Err(error) => return Err(error.into()),
    }
    let setup_message = if state.single.is_some() {
        format!(
            "Fetching guide data for {}...",
            state.station_label().await?
        )
    } else {
        format!("Fetching guide data for {stations} mapped stations...")
    };
    progress.parsing(15, Some(setup_message)).await;
    state.reset_window(chrono::Local::now().date_naive())?;
    let mut after = 0;
    let mut station_count = 0u64;
    let mut md5_failed = false;
    state.prune_schedule_cache().await?;
    let selected = state.fetch_station_count().await?;
    if selected > 0 {
        progress
            .parsing(
                33,
                Some(format!(
                    "Checking schedule MD5s for {selected} mapped stations over 20 days..."
                )),
            )
            .await;
    }

    loop {
        let page = state.station_page(after).await?;
        if page.is_empty() {
            break;
        }
        after = page.last().ok_or(Error::Unsupported)?.0;
        station_count += page.len() as u64;
        let request = state.md5_request(&page);
        match request_retry(source, &mut token, "/schedules/md5", &request, stopped).await {
            Ok(reply) => {
                let Some(value) = reply.value else {
                    md5_failed = true;
                    continue;
                };
                if !value.is_object() {
                    return Err(Error::Unsupported);
                }
                state.record_schedule_md5(value).await?;
            }
            Err(error) if known_failure(&error) => md5_failed = true,
            Err(error) => return Err(error),
        }
    }
    if station_count == 0 {
        return Ok(if state.single.is_some() {
            Outcome::SkippedSingle { token }
        } else {
            Outcome::EmptyMapping
        });
    }
    let raw_changed = state.select_changes().await?;
    if md5_failed && raw_changed == 0 {
        return Ok(Outcome::Md5Unavailable);
    }
    let changed = state.changed_station_count().await?;
    progress
        .parsing(
            38,
            Some(format!(
                "MD5 check complete: {changed} stations have schedule updates."
            )),
        )
        .await;
    if changed == 0 {
        return Ok(Outcome::Unchanged { token });
    }
    // Source emits one request per7-day window, containing all changed stations.
    // Charge the existing64MiB JSON work ceiling while collecting its bounded
    // request; do not silently split a Source HTTP request into new attempts.
    let windows = state.days.chunks(7).collect::<Vec<_>>();
    for (index, window) in windows.iter().enumerate() {
        progress
            .parsing(
                (38 + ((index as f64 / windows.len() as f64) * 22.0) as i64).min(59),
                Some(format!(
                    "Fetching schedules: batch {} of {}...",
                    index + 1,
                    windows.len()
                )),
            )
            .await;
        let mut body = Vec::new();
        let mut after = 0;
        let mut work = 64usize * 1024 * 1024;
        loop {
            let page = state.station_page(after).await?;
            if page.is_empty() {
                break;
            }
            after = page.last().ok_or(Error::Unsupported)?.0;
            for (_, station, _) in page {
                let changed = state.changed_dates(&station).await?;
                let dates = window
                    .iter()
                    .filter(|d| changed.contains(d))
                    .map(ToString::to_string)
                    .collect::<Vec<_>>();
                if dates.is_empty() {
                    continue;
                }
                let charge = 256usize
                    .checked_add(station.len().saturating_mul(4))
                    .and_then(|n| n.checked_add(dates.len() * 128))
                    .ok_or(state::Error::Capacity)?;
                work = work.checked_sub(charge).ok_or(state::Error::Capacity)?;
                body.push(json!({"stationID":station,"date":dates}));
            }
        }
        if body.is_empty() {
            continue;
        }
        match request_retry(
            source,
            &mut token,
            "/schedules",
            &Value::Array(body),
            stopped,
        )
        .await
        {
            Ok(reply) => {
                let Some(payload) = reply.value else { continue };
                if !payload.is_array() {
                    return Err(Error::Unsupported);
                }
                state.stage_schedules(payload).await?;
            }
            Err(error) if known_failure(&error) => continue,
            Err(error) => return Err(error),
        }
        let found = state.programme_id_count().await?;
        progress
            .parsing(
                (38 + (((index + 1) as f64 / windows.len() as f64) * 22.0) as i64).min(60),
                Some(format!(
                    "Fetching changed schedules: batch {}/{} ({} programs found)",
                    index + 1,
                    windows.len(),
                    commas(found)
                )),
            )
            .await;
    }
    state.commit_schedule_md5().await?;
    state.select_programs().await?;
    if !state.has_programs().await? {
        return Ok(Outcome::NoSchedules);
    }
    let needed = state.needed_program_count().await?;
    let batches = ((needed + 4999) / 5000).max(1);
    let mut index = 0i64;
    let mut after = String::new();
    if needed == 0 {
        progress
            .parsing(
                80,
                Some("Program metadata unchanged - using cached data.".into()),
            )
            .await;
    }
    loop {
        let page = state.needed_programs(&after).await?;
        if page.is_empty() {
            break;
        }
        after = page.last().ok_or(Error::Unsupported)?.clone();
        progress
            .parsing(
                (60 + ((index as f64 / batches as f64) * 20.0) as i64).min(79),
                Some(format!(
                    "Fetching program data: batch {} of {} ({} of {} programs)...",
                    index + 1,
                    batches,
                    commas((index * 5000) as u64),
                    commas(needed as u64)
                )),
            )
            .await;
        let success =
            match request_retry(source, &mut token, "/programs", &json!(page), stopped).await {
                Ok(reply) => match reply.value {
                    None => {
                        state.stage_programs(page, None).await?;
                        false
                    }
                    Some(payload) => {
                        if !payload.is_array() {
                            return Err(Error::Unsupported);
                        }
                        state.stage_programs(page, Some(payload)).await?;
                        true
                    }
                },
                Err(error) if known_failure(&error) => {
                    state.stage_programs(page, None).await?;
                    false
                }
                Err(error) => return Err(error),
            };
        index += 1;
        if success {
            let count = state.downloaded_program_count().await?;
            progress
                .parsing(
                    (60 + ((index as f64 / batches as f64) * 20.0) as i64).min(80),
                    Some(format!(
                        "Fetching program details: batch {index}/{batches} ({} programs loaded)",
                        commas(count as u64)
                    )),
                )
                .await;
        }
    }
    progress.parsing(80, None).await;
    // Source builds all replacement records BEFORE entering its atomic block.
    // Keep this complete list in SQL, projecting one actual airing at a time.
    state.capture_cached_programmes().await?;
    let mut after = 0;
    loop {
        if stopped() {
            return Err(Error::Stopped);
        }
        let page = state.airing_page(after).await?;
        if page.is_empty() {
            break;
        }
        after = page.last().ok_or(Error::Unsupported)?.0;
        for (_, epg, station, airing, metadata, cached, failed) in page {
            if let Some(row) = programme::project(
                epg,
                &station,
                &airing,
                metadata.as_ref(),
                cached.as_ref(),
                failed,
                country.as_deref(),
            )? {
                state.stage_projection(row.value()).await?;
            }
        }
    }
    // Preserve surgical atomicity and Source batch1000. No whole descendant
    // Programme list or repeated provider metadata is retained in host memory.
    let projected = state.projected_count().await?;
    progress.parsing(88, None).await;
    state.begin_surgical_write().await?;
    let written = async {
        let mut after = 0;
        let mut total = 0u64;
        loop {
            if stopped() {
                return Err(Error::Stopped);
            }
            let Some((high, count)) = state.insert_projected_batch(after).await? else {
                break;
            };
            after = high;
            total += count;
            let original_offset = ((total.saturating_sub(1)) / 1000) * 1000;
            progress
                .parsing(
                    (88 + (((original_offset + 1000) as f64 / projected.max(1) as f64) * 10.0)
                        as i64)
                        .min(98),
                    None,
                )
                .await;
        }
        Ok::<u64, Error>(total)
    }
    .await;
    state.settle_surgical_write(written.is_ok()).await?;
    let programmes = written?;
    state.commit_program_md5().await?;
    Ok(Outcome::Written {
        programmes,
        stations: state.mapped_station_count().await? as u64,
        skipped_unmapped: state.skipped_unmapped().await?,
        token,
    })
}
fn commas(n: u64) -> String {
    let raw = n.to_string();
    let mut out = String::new();
    for (i, c) in raw.chars().enumerate() {
        if i > 0 && (raw.len() - i).is_multiple_of(3) {
            out.push(',')
        }
        out.push(c)
    }
    out
}
#[cfg(test)]
mod controls {
    use super::*;
    #[test]
    fn offline_in_any_actual_status_entry_wins() {
        assert!(system_status(&json!({"systemStatus":[{"status":" Online "},"OFFLINE"]})).unwrap());
        assert!(!system_status(&json!({"systemStatus":["",null,{"status":"Online"}]})).unwrap());
        assert!(!system_status(&json!({"systemStatus":12})).unwrap());
    }
}
