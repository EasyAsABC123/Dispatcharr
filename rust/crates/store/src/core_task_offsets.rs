//! Actual offset-change task effects and save-signal scheduling.
//! Original Dispatcharr/Django authors retain application/algorithm credit.
use crate::{
    recording_http::{RecordingEffectError, RecordingJobs},
    Store, StoreError,
};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Utc};
use dispatcharr_contracts::UpdateEventSink;
use serde_json::{json, Value};
use sqlx::{Connection, Row};
use std::{str::FromStr, sync::Arc};
use tokio::sync::oneshot;
pub struct Report {
    pub value: Value,
    pub children: Vec<String>,
}
fn unsupported() -> StoreError {
    StoreError::Unsupported("Offset task domain or physical capacity is unsupported")
}
fn effect(error: RecordingEffectError) -> StoreError {
    match error {
        RecordingEffectError::Unsupported => unsupported(),
        RecordingEffectError::SettledFailure => {
            StoreError::Unsupported("Settled recording signal failed")
        }
        RecordingEffectError::Unsettled => {
            StoreError::Unsupported("Recording signal physical outcome is unknown")
        }
    }
}
/// Int conversion returns the actual Python integer value, including values
/// whose timedelta overflows. Invalid values alone fall back to zero.
fn integer(raw: Option<&Value>) -> Value {
    let text = match raw {
        Some(Value::Bool(v)) => return json!(i64::from(*v)),
        Some(Value::Number(v)) => {
            let text = v.to_string();
            if !text.contains(['.', 'e', 'E']) {
                return Value::Number(v.clone());
            }
            match v.as_f64().filter(|v| v.is_finite()) {
                Some(v) => format!("{:.0}", v.trunc()),
                None => return json!(0),
            }
        }
        Some(Value::String(v)) => match dispatcharr_ingest_core::source_integer_literal(v) {
            Some(v) => v,
            None => return json!(0),
        },
        _ => return json!(0),
    };
    serde_json::Number::from_str(&text)
        .map(Value::Number)
        .unwrap_or_else(|_| json!(0))
}
fn when(raw: &Value) -> Option<(DateTime<Utc>, bool)> {
    let text = raw.as_str()?;
    if let Ok(value) = DateTime::parse_from_rfc3339(text) {
        return (1..=9999)
            .contains(&value.year())
            .then_some((value.to_utc(), true));
    }
    // Django parse_datetime accepts the space separator and naive ISO values;
    // the pinned Source default timezone is UTC. Invalid values are skipped.
    for pattern in ["%Y-%m-%d %H:%M:%S%.f%:z", "%Y-%m-%d %H:%M:%S%.f%z"] {
        if let Ok(value) = DateTime::parse_from_str(text, pattern) {
            return (1..=9999)
                .contains(&value.year())
                .then_some((value.to_utc(), true));
        }
    }
    for pattern in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"] {
        if let Ok(value) = NaiveDateTime::parse_from_str(text, pattern) {
            return (1..=9999)
                .contains(&value.year())
                .then_some((value.and_utc(), false));
        }
    }
    NaiveDate::parse_from_str(text, "%Y-%m-%d")
        .ok()?
        .and_hms_opt(0, 0, 0)
        .filter(|v| (1..=9999).contains(&v.year()))
        .map(|v| (v.and_utc(), false))
}
fn shifted(time: DateTime<Utc>, minutes: &Value, subtract: bool) -> DateTime<Utc> {
    minutes
        .as_i64()
        .filter(|v| *v > 0)
        .and_then(chrono::Duration::try_minutes)
        .and_then(|delta| {
            if subtract {
                time.checked_sub_signed(delta)
            } else {
                time.checked_add_signed(delta)
            }
        })
        .filter(|v| (1..=9999).contains(&v.year()))
        .unwrap_or(time)
}
pub async fn preflight(store: &Store) -> Result<(), StoreError> {
    let mut connection = store.pool().acquire().await?;
    let observed = sqlx::query("SELECT id FROM dispatcharr_channels_recording LIMIT 0")
        .execute(&mut *connection)
        .await;
    let closed = connection.close().await;
    observed?;
    closed?;
    Ok(())
}
/// One physically owned pool session holds the Source-order cursor AND every
/// row/save-signal SQL callback. There is no second checkout or pool-size change.
/// Cursor membership is frozen once, so shifted dates cannot skip later rows.
pub async fn execute(
    store: Store,
    jobs: Arc<dyn RecordingJobs>,
    events: Arc<dyn UpdateEventSink>,
    parent: Option<uuid::Uuid>,
    owner: Arc<dyn Send + Sync>,
) -> Result<Report, StoreError> {
    // Reject an unsupported recording domain before settings, SQL or revocation.
    jobs.admit_scheduling(owner.clone()).await.map_err(effect)?;
    let _offset_effects = jobs.lock_offset_effects().await.map_err(effect)?;
    let now = Utc::now();
    let settings = match jobs.dvr_settings(owner.clone()).await {
        Ok(v) => v,
        Err(RecordingEffectError::SettledFailure) => json!({}),
        Err(e) => return Err(effect(e)),
    };
    let pre = integer(settings.get("pre_offset_minutes"));
    let post = integer(settings.get("post_offset_minutes"));
    let mut connection = store.pool().acquire().await?;
    let observed=async{
        sqlx::query("DECLARE rust_offset_rows NO SCROLL CURSOR WITH HOLD FOR SELECT id::bigint,start_time,end_time FROM dispatcharr_channels_recording WHERE start_time>$1").bind(now).execute(&mut*connection).await?;
        let mut changed=0usize;let mut scanned=0usize;let mut children=Vec::new();let mut child_bytes=0usize;
        loop{
            let page=sqlx::query("FETCH FORWARD 256 FROM rust_offset_rows").fetch_all(&mut*connection).await?;
            if page.is_empty(){break;}
            for selected in page{
                scanned+=1;let id:i64=selected.try_get("id")?;
                // Project only fields consumed by the Source task/signal. Large
                // unrelated JSON fields never become a fabricated empty object.
                let row=sqlx::query(r#"SELECT start_time,end_time,task_id,jsonb_typeof(custom_properties->'program')='object' AS program_object,CASE WHEN COALESCE(octet_length((custom_properties->'program'->'start_time')::text),0)<=256 THEN custom_properties->'program'->'start_time' END AS base_start,CASE WHEN COALESCE(octet_length((custom_properties->'program'->'end_time')::text),0)<=256 THEN custom_properties->'program'->'end_time' END AS base_end,(COALESCE(octet_length((custom_properties->'program'->'start_time')::text),0)<=256 AND COALESCE(octet_length((custom_properties->'program'->'end_time')::text),0)<=256) AS date_values_fit,COALESCE(custom_properties->'status'='"recording"'::jsonb,false) AS recording FROM dispatcharr_channels_recording WHERE id=$1::bigint"#).bind(id).fetch_optional(&mut*connection).await?;
                let Some(row)=row else{continue};
                if row.try_get::<Option<bool>,_>("program_object")?!=Some(true){continue;}
                if !row.try_get::<bool,_>("date_values_fit")?{return Err(unsupported());}
                let base_start:Option<Value>=row.try_get("base_start")?;let base_end:Option<Value>=row.try_get("base_end")?;
                let Some((start,start_aware))=base_start.as_ref().and_then(when)else{continue};
                let Some((end,end_aware))=base_end.as_ref().and_then(when)else{continue};
                let start=shifted(start,&pre,true);let end=shifted(end,&post,false);
                let old_start:DateTime<Utc>=row.try_get("start_time")?;let old_end:DateTime<Utc>=row.try_get("end_time")?;
                if start==old_start&&end==old_end&&start_aware&&end_aware{continue;}
                let old_key:Option<String>=row.try_get("task_id")?;
                let recording:bool=row.try_get("recording")?;
                let revoke=old_key.as_ref().filter(|v|!v.is_empty()).filter(|_|!recording);
                if let Some(key)=revoke{
                    match jobs.revoke_on(key.clone(),&mut connection,owner.clone()).await{Ok(())=>{},Err(RecordingEffectError::SettledFailure)=>continue,Err(e)=>return Err(effect(e))}
                }
                let update=sqlx::query("UPDATE dispatcharr_channels_recording SET start_time=$2,end_time=$3 WHERE id=$1::bigint").bind(id).bind(start).bind(end).execute(&mut*connection).await;
                match update{Ok(v)if v.rows_affected()==1=>{},Ok(_)=>continue,Err(error)if error.as_database_error().is_some()=>continue,Err(error)=>return Err(error.into())}
                match jobs.schedule_offset_save_on(id, parent, revoke.is_some(), &mut connection, owner.clone()).await {
                    Ok(ids) if parent.is_some() => {
                        for id in ids {
                            let bytes = serde_json::to_vec(&json!([[id, null], null])).map_err(|_| unsupported())?.len() + 1;
                            child_bytes = child_bytes.checked_add(bytes).ok_or_else(unsupported)?;
                            if child_bytes > 240000 {
                                return Err(StoreError::Unsupported("Offset descendants exceed existing backend wire budget; scalable encoding is unqualified"));
                            }
                            children.push(id);
                        }
                    }
                    Ok(_) | Err(RecordingEffectError::SettledFailure) => {}
                    Err(e) => return Err(effect(e)),
                }
                changed+=1;
            }
        }
        sqlx::query("CLOSE rust_offset_rows").execute(&mut*connection).await?;
        Ok::<_,StoreError>(Report{value:json!({"changed":changed,"scanned":scanned,"pre":pre,"post":post}),children})
    }.await;
    // The task supervisor owns this future. Always retire the actual session,
    // including its HOLD cursor, before propagating an incomplete SQL outcome.
    let ready = connection.ping().await;
    let closed = connection.close().await;
    let report = observed?;
    ready?;
    closed?;
    retained_event(events,json!({"type":"update","data":{"success":true,"type":"recordings_refreshed","rescheduled":report.value["changed"]}}),owner).await?;
    Ok(report)
}
struct Receipt(Option<oneshot::Sender<()>>, Arc<dyn Send + Sync>);
impl Drop for Receipt {
    fn drop(&mut self) {
        if let Some(done) = self.0.take() {
            let _ = done.send(());
        }
        let _ = &self.1;
    }
}
pub(crate) async fn retained_event(
    events: Arc<dyn UpdateEventSink>,
    event: Value,
    owner: Arc<dyn Send + Sync>,
) -> Result<(), StoreError> {
    let (done, settled) = oneshot::channel();
    let receipt = Arc::new(Receipt(Some(done), owner));
    let sent = events
        .send_retained("updates", event, receipt.clone())
        .await;
    drop(receipt);
    settled
        .await
        .map_err(|_| StoreError::Unsupported("Event physical receipt lost"))?;
    match sent {
        Ok(()) | Err(dispatcharr_contracts::UpdateEventError::Unavailable) => Ok(()),
        Err(_) => Err(StoreError::Unsupported("Event physical completion unknown")),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn integer_offsets_and_overflow_preserve_result_values() {
        assert_eq!(integer(Some(&json!(true))), json!(1));
        assert_eq!(integer(Some(&json!(" +２_０ "))), json!(20));
        assert_eq!(integer(Some(&json!("invalid"))), json!(0));
        let date: DateTime<Utc> = "2026-11-14T12:00:00+00:00".parse().unwrap();
        assert_eq!(shifted(date, &json!(i64::MAX), true), date);
        assert_eq!(
            shifted(date, &json!(2), false),
            date + chrono::Duration::minutes(2)
        );
        assert!(when(&json!("not a date")).is_none());
    }
}
