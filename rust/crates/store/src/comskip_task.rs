//! Manual comskip slice. Original Dispatcharr/Comskip authors retain credit.
use crate::{AuthService, StoreError};
use dispatcharr_contracts::{BoxFuture, UpdateEventSink};
use serde_json::{json, Value};
use sqlx::Row;
use std::{path::PathBuf, sync::Arc};
pub type Owner = Arc<dyn Send + Sync>;
pub const SETTINGS: &str = "SELECT value FROM core_coresettings WHERE key='dvr_settings'";
pub const SELECT:&str="SELECT CASE WHEN octet_length(custom_properties::text)<=65536 THEN custom_properties ELSE NULL END AS properties,COALESCE(octet_length(custom_properties::text),0)<=65536 AS bounded FROM dispatcharr_channels_recording WHERE id=$1";
pub const UPDATE: &str =
    "UPDATE dispatcharr_channels_recording SET custom_properties=$2 WHERE id=$1";
pub struct Work {
    pub recording: i64,
    pub file: PathBuf,
    pub custom_ini: Option<PathBuf>,
    pub hw: Option<String>,
    pub title: String,
    properties: Value,
}
pub enum Plan {
    Terminal(&'static str),
    Work(Work),
}
pub struct Outcome {
    pub result: &'static str,
    pub properties: Value,
    pub status: &'static str,
    pub extra: Value,
}
/// Implemented by the real owned media process. No caller can supply an exit code.
pub trait Media: Send + Sync {
    fn process(&self, work: Work, owner: Owner) -> BoxFuture<'_, Result<Outcome, StoreError>>;
}
fn unsupported() -> StoreError {
    StoreError::Unsupported("Manual comskip supports bounded completed-file mark mode only")
}
pub async fn preflight(
    auth: &AuthService,
    recording: i64,
    owner: Owner,
) -> Result<Plan, StoreError> {
    if recording <= 0 || recording > i64::from(i32::MAX) {
        return Err(unsupported());
    }
    let session = auth.borrow_task_sql_session(owner).await?;
    let result = session
        .run(move |c| {
            Box::pin(async move {
                let row = sqlx::query(SELECT)
                    .bind(recording)
                    .fetch_optional(&mut *c)
                    .await?;
                let Some(row) = row else {
                    return Ok(Plan::Terminal("not_found"));
                };
                if !row.try_get::<bool, _>("bounded")? {
                    return Err(unsupported());
                }
                let raw: Option<Value> = row.try_get("properties")?;
                let properties = raw.filter(Value::is_object).unwrap_or_else(|| json!({}));
                let Some(file) = properties
                    .get("file_path")
                    .and_then(Value::as_str)
                    .filter(|v| !v.is_empty())
                else {
                    return Ok(Plan::Terminal("no_file"));
                };
                if file.len() > 4096 {
                    return Err(unsupported());
                }
                let file = PathBuf::from(file);
                match std::fs::metadata(&file) {
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        return Ok(Plan::Terminal("no_file"))
                    }
                    Err(_) => return Err(unsupported()),
                    Ok(m) if !m.is_file() => return Err(unsupported()),
                    Ok(_) => {}
                }
                if properties
                    .get("comskip")
                    .and_then(|v| v.get("status"))
                    .and_then(Value::as_str)
                    == Some("completed")
                {
                    return Ok(Plan::Terminal("already_processed"));
                }
                let settings: Option<Value> =
                    sqlx::query_scalar(SETTINGS).fetch_optional(&mut *c).await?;
                let settings = settings.unwrap_or_else(|| json!({}));
                if settings.get("comskip_mode").and_then(Value::as_str) != Some("mark") {
                    return Err(unsupported());
                }
                if !matches!(
                    properties.get("status").and_then(Value::as_str),
                    Some("completed" | "stopped")
                ) {
                    return Err(unsupported());
                }
                let hw = match settings
                    .get("comskip_hw_accel")
                    .and_then(Value::as_str)
                    .unwrap_or("none")
                {
                    "none" | "" => None,
                    "cuvid" => Some("--cuvid".into()),
                    "hwassist" | "qsv" => Some("--hwassist".into()),
                    _ => return Err(unsupported()),
                };
                let custom_ini = match settings.get("comskip_custom_path") {
                    None | Some(Value::Null) => None,
                    Some(Value::String(s)) if s.is_empty() => None,
                    Some(Value::String(s)) if s.len() <= 4096 => Some(PathBuf::from(s)),
                    _ => return Err(unsupported()),
                };
                let title = properties
                    .get("program")
                    .and_then(|v| v.get("title"))
                    .and_then(Value::as_str)
                    .filter(|v| !v.is_empty())
                    .map(str::to_owned)
                    .or_else(|| file.file_name().and_then(|v| v.to_str()).map(str::to_owned))
                    .ok_or_else(unsupported)?;
                Ok(Plan::Work(Work {
                    recording,
                    file,
                    custom_ini,
                    hw,
                    title,
                    properties,
                }))
            })
        })
        .await;
    // Physical close must succeed even when business reads failed.
    session.close().await?;
    result?
}
async fn notice(
    events: &Arc<dyn UpdateEventSink>,
    recording: i64,
    status: &str,
    extra: Value,
    owner: Owner,
) {
    let mut payload =
        json!({"success":true,"type":"comskip_status","status":status,"recording_id":recording});
    if let Some(extra) = extra.as_object() {
        for (k, v) in extra {
            payload[k] = v.clone();
        }
    }
    let _ = events
        .send_retained("updates", json!({"type":"update","data":payload}), owner)
        .await;
}
pub async fn execute(
    auth: &AuthService,
    events: Arc<dyn UpdateEventSink>,
    plan: Plan,
    media: Arc<dyn Media>,
    owner: Owner,
) -> Result<Value, StoreError> {
    let Plan::Work(work) = plan else {
        if let Plan::Terminal(result) = plan {
            return Ok(json!(result));
        }
        unreachable!()
    };
    let recording = work.recording;
    let mut cp = work.properties.clone();
    // Media emits started ONLY after real executable availability has been observed.
    let outcome = media.process(work, owner.clone()).await?;
    cp["comskip"] = outcome.properties;
    let session = auth.borrow_task_sql_session(owner.clone()).await?;
    let updated = session
        .run(move |c| {
            Box::pin(async move {
                sqlx::query(UPDATE)
                    .bind(recording)
                    .bind(cp)
                    .execute(c)
                    .await
                    .map(|_| ())
                    .map_err(StoreError::from)
            })
        })
        .await;
    session.close().await?;
    // Source catches settled SQL failure / disappeared row and still sends status.
    let _ = updated?;
    notice(&events, recording, outcome.status, outcome.extra, owner).await;
    Ok(json!(outcome.result))
}
pub async fn started(events: &Arc<dyn UpdateEventSink>, work: &Work, owner: Owner) {
    notice(
        events,
        work.recording,
        "started",
        json!({"title":work.title}),
        owner,
    )
    .await;
}
/// Source reads every parseable interval, clamps each endpoint independently,
/// sorts lexically and counts even zero/negative individual intervals.
pub fn intervals(edl: &str, duration: f64) -> Vec<(f64, f64)> {
    let mut ranges = Vec::new();
    for line in edl.lines() {
        let mut p = line.split_whitespace();
        if let (Some(a), Some(b)) = (p.next(), p.next()) {
            if let (Ok(a), Ok(b)) = (a.parse::<f64>(), b.parse::<f64>()) {
                if a.is_finite() && b.is_finite() {
                    ranges.push((a.max(0.0), b.min(duration)));
                }
            }
        }
    }
    ranges.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.total_cmp(&b.1)));
    ranges
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_clamps_but_does_not_merge_or_drop_negative_ranges() {
        assert_eq!(
            intervals("-2 3 0\n2 5\n8 4\nnot a range\n9 30", 10.0),
            vec![(0.0, 3.0), (2.0, 5.0), (8.0, 4.0), (9.0, 10.0)]
        );
    }
    #[test]
    fn source_half_second_threshold_is_strict() {
        let v = intervals("1 1.5", 3.0);
        assert_eq!(v.iter().map(|(a, b)| b - a).sum::<f64>(), 0.5);
    }
}
