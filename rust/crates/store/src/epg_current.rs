//! Current-programme selection from original Dispatcharr CurrentProgramsAPIView.
//! Original authors retain all application credit. Component only; no HTTP wiring.
use crate::{EpgReadResource, Store, StoreError};
use chrono::{DateTime, Utc};
use futures_util::TryStreamExt;
use serde_json::{json, Value};
use sqlx::{Postgres, QueryBuilder, Row};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub enum CurrentProgramSelection {
    Channels(Option<Vec<Uuid>>),
    EpgData(Vec<i64>),
}
fn input(message: &str) -> StoreError {
    StoreError::StatusBody(400, json!({"error":message}))
}
fn unhandled() -> StoreError {
    // Source's outer Django500 body needs a separate packaged HTTP witness.
    StoreError::StatusBody(
        500,
        json!({"detail":"Original current-programme view raises an unhandled input exception."}),
    )
}
pub(super) fn capacity() -> StoreError {
    StoreError::StatusBody(503, json!({"detail":"Guide read capacity unavailable."}))
}
fn integer(value: &Value) -> Result<Option<i64>, StoreError> {
    match value {
        Value::Bool(value) => Ok(Some(i64::from(*value))),
        Value::String(value) => dispatcharr_ingest_core::source_integer_literal(value)
            .map(|raw| raw.parse().ok())
            .ok_or_else(|| input("epg_data_ids must contain valid integers")),
        Value::Number(value) if value.is_f64() => {
            let raw = value
                .as_f64()
                .filter(|v| v.is_finite())
                .ok_or_else(unhandled)?;
            Ok((raw >= i64::MIN as f64 && raw < (i64::MAX as f64)).then_some(raw.trunc() as i64))
        }
        Value::Number(value) => Ok(value.as_i64()),
        _ => Err(input("epg_data_ids must contain valid integers")),
    }
}
fn channel_uuid(value: &Value) -> Result<Option<Uuid>, StoreError> {
    match value {
        Value::Null => Ok(None),
        Value::Bool(value) => Ok(Some(Uuid::from_u128(u128::from(*value)))),
        Value::Number(value) if !value.is_f64() => value
            .to_string()
            .parse::<u128>()
            .map(Uuid::from_u128)
            .map(Some)
            .map_err(|_| unhandled()),
        Value::String(value) => Uuid::parse_str(value).map(Some).map_err(|_| unhandled()),
        _ => Err(unhandled()),
    }
}
impl CurrentProgramSelection {
    /// Preserve Python int conversion (not DRF IntegerField) and validate the
    /// complete EPG ID array before retaining its first50 original positions.
    /// UUID grammar expansion beyond the captured cases remains a separate gate.
    pub fn from_json(body: &Value) -> Result<Self, StoreError> {
        let body = body.as_object().ok_or_else(unhandled)?;
        let channels = body.get("channel_uuids").filter(|v| !v.is_null());
        let epgs = body.get("epg_data_ids").filter(|v| !v.is_null());
        if channels.is_some() && epgs.is_some() {
            return Err(input(
                "Provide either channel_uuids or epg_data_ids, not both",
            ));
        }
        if let Some(epgs) = epgs {
            let epgs = epgs
                .as_array()
                .ok_or_else(|| input("epg_data_ids must be an array of integers or null"))?;
            let parsed = epgs.iter().map(integer).collect::<Result<Vec<_>, _>>()?;
            return Ok(Self::EpgData(
                parsed.into_iter().take(50).flatten().collect(),
            ));
        }
        let ids = channels
            .map(|channels| {
                channels
                    .as_array()
                    .ok_or_else(|| input("channel_uuids must be an array of strings or null"))?
                    .iter()
                    .map(channel_uuid)
                    .collect::<Result<Vec<_>, _>>()
                    .map(|values| values.into_iter().flatten().collect())
            })
            .transpose()?;
        Ok(Self::Channels(ids))
    }
}
// Input guards run inside SQL before the client decoder receives large fields.
// These are the same explicit row bounds as programme reads, not a claim that
// the original imposes the bound. No HTTP/global job lease is provided here.
fn columns() -> String {
    let size="coalesce(octet_length(p.title),0)::bigint+coalesce(octet_length(p.sub_title),0)+coalesce(octet_length(p.description),0)+coalesce(octet_length(p.tvg_id),0)+coalesce(octet_length(p.custom_properties::text),0)";
    format!("p.id::bigint AS id,p.epg_id::bigint AS relation_id,p.start_time,p.end_time,({size}>1048576) AS oversized,CASE WHEN {size}<=1048576 THEN p.title ELSE '' END AS title,CASE WHEN {size}<=1048576 THEN p.sub_title END AS sub_title,CASE WHEN {size}<=1048576 THEN p.description END AS description,CASE WHEN {size}<=1048576 THEN p.tvg_id END AS tvg_id,CASE WHEN {size}<=1048576 THEN p.custom_properties END AS custom_properties")
}
fn project(row: sqlx::postgres::PgRow, maximum: usize) -> Result<Vec<u8>, StoreError> {
    if row.try_get::<bool, _>("oversized")? {
        return Err(capacity());
    }
    encode_programme(row, maximum)
}
pub(super) fn encode_programme(
    row: sqlx::postgres::PgRow,
    maximum: usize,
) -> Result<Vec<u8>, StoreError> {
    let value = EpgReadResource::Programs.project(row, false, "")?;
    let mut bytes = crate::epg_program_http::BoundedJson::new(maximum);
    serde_json::to_writer(&mut bytes, &value).map_err(|_| capacity())?;
    Ok(bytes.into_data())
}
pub(super) struct Reply {
    bytes: crate::epg_program_http::BoundedJson,
    rows: usize,
    maximum_rows: usize,
}
impl Reply {
    pub(super) fn new(maximum_rows: usize, maximum_bytes: usize) -> Result<Self, StoreError> {
        use std::io::Write;
        let mut bytes = crate::epg_program_http::BoundedJson::new(maximum_bytes);
        bytes.write_all(b"[").map_err(|_| capacity())?;
        Ok(Self {
            bytes,
            rows: 0,
            maximum_rows,
        })
    }
    pub(super) fn append(
        &mut self,
        base: &[u8],
        field: &str,
        value: &Value,
    ) -> Result<(), StoreError> {
        use std::io::Write;
        if self.rows >= self.maximum_rows || base.last() != Some(&b'}') {
            return Err(capacity());
        }
        if self.rows != 0 {
            self.bytes.write_all(b",").map_err(|_| capacity())?;
        }
        self.bytes
            .write_all(&base[..base.len() - 1])
            .map_err(|_| capacity())?;
        self.bytes.write_all(b",").map_err(|_| capacity())?;
        serde_json::to_writer(&mut self.bytes, field).map_err(|_| capacity())?;
        self.bytes.write_all(b":").map_err(|_| capacity())?;
        serde_json::to_writer(&mut self.bytes, value).map_err(|_| capacity())?;
        self.bytes.write_all(b"}").map_err(|_| capacity())?;
        self.rows += 1;
        Ok(())
    }
    pub(super) fn finish(mut self) -> Result<Vec<u8>, StoreError> {
        use std::io::Write;
        self.bytes.write_all(b"]").map_err(|_| capacity())?;
        Ok(self.bytes.into_data())
    }
}
fn check_deadline(expires: std::time::Instant) -> Result<(), StoreError> {
    if std::time::Instant::now() >= expires {
        Err(capacity())
    } else {
        Ok(())
    }
}
fn retain_bytes(used: &mut usize, bytes: Option<&[u8]>, maximum: usize) -> Result<(), StoreError> {
    *used = used
        .checked_add(bytes.map_or(0, <[u8]>::len))
        .filter(|v| *v <= maximum)
        .ok_or_else(capacity)?;
    Ok(())
}
impl Store {
    /// Bounded serialized DB-selection component. Caller must retain worker
    /// admission and final byte-allocation ownership before HTTP wiring. The
    /// transaction drains rollback before a completed call releases SQL work.
    /// Real EPG entries without stored rows require the unfinished byte index;
    /// they are never silently omitted as an empty result.
    pub async fn current_programmes_db(
        &self,
        selection: CurrentProgramSelection,
        now: DateTime<Utc>,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<Vec<u8>, StoreError> {
        use sqlx::Acquire;
        if max_rows == 0 || max_rows > 50000 || !(2..=128 * 1024 * 1024).contains(&max_bytes) {
            return Err(capacity());
        }
        let expires = std::time::Instant::now() + std::time::Duration::from_secs(2);
        let mut connection = self.pool().acquire().await?;
        check_deadline(expires)?;
        let mut tx = connection.begin().await?;
        sqlx::query("SELECT set_config('statement_timeout','2000ms',true)")
            .execute(&mut *tx)
            .await?;
        let result = select_current(&mut tx, selection, now, max_rows, max_bytes, expires).await;
        tx.rollback().await?;
        result
    }
}
async fn select_current(
    conn: &mut sqlx::PgConnection,
    selection: CurrentProgramSelection,
    now: DateTime<Utc>,
    max_rows: usize,
    max_bytes: usize,
    expires: std::time::Instant,
) -> Result<Vec<u8>, StoreError> {
    check_deadline(expires)?;
    let mut output = Reply::new(max_rows, max_bytes)?;
    let mut retained = 0;
    match selection {
        CurrentProgramSelection::Channels(ids) => {
            if ids.as_ref().is_some_and(Vec::is_empty) {
                return output.finish();
            }
            // Exact original channel projection: all columns are bounded
            // varchar/numeric/UUID/date. Do not narrow the unordered join plan.
            let mut query=QueryBuilder::<Postgres>::new("SELECT c.*,COALESCE(o.epg_data_id,c.epg_data_id) AS effective_epg_data_id FROM dispatcharr_channels_channel c LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id WHERE NOT (COALESCE(o.epg_data_id,c.epg_data_id) IS NULL)");
            if let Some(ids) = ids {
                if ids.len() > max_rows {
                    return Err(capacity());
                }
                let mut seen = HashSet::new();
                query.push(" AND c.uuid IN (");
                let mut separated = query.separated(",");
                for id in ids {
                    if seen.insert(id) {
                        separated.push_bind(id);
                    }
                }
                separated.push_unseparated(")");
            }
            let mut channels = query.build().persistent(false).fetch(&mut *conn);
            let mut selected = Vec::new();
            while let Some(row) = channels.try_next().await? {
                check_deadline(expires)?;
                if selected.len() >= max_rows {
                    return Err(capacity());
                }
                selected.push((
                    row.try_get::<Uuid, _>("uuid")?,
                    row.try_get::<i64, _>("effective_epg_data_id")?,
                ));
            }
            drop(channels);
            let sql=format!("SELECT {} FROM epg_programdata p JOIN epg_epgdata e ON e.id=p.epg_id WHERE p.end_time>$1 AND p.epg_id=$2 AND p.start_time<=$1 ORDER BY p.id ASC LIMIT 1",columns());
            let mut by_epg = HashMap::<i64, Option<Vec<u8>>>::new();
            for (uuid, id) in selected {
                check_deadline(expires)?;
                if let std::collections::hash_map::Entry::Vacant(entry) = by_epg.entry(id) {
                    let row = sqlx::query(&sql)
                        .bind(now)
                        .bind(id)
                        .fetch_optional(&mut *conn)
                        .await?;
                    let value = row.map(|row| project(row, max_bytes)).transpose()?;
                    retain_bytes(&mut retained, value.as_deref(), max_bytes)?;
                    entry.insert(value);
                }
                if let Some(base) = by_epg.get(&id).and_then(Option::as_ref) {
                    output.append(base, "channel_uuid", &json!(uuid.to_string()))?;
                }
            }
        }
        CurrentProgramSelection::EpgData(ids) => {
            if ids.is_empty() {
                return output.finish();
            }
            if ids.len() > 50 {
                return Err(capacity());
            }
            let mut query=QueryBuilder::<Postgres>::new("SELECT d.id::bigint AS id,s.source_type FROM epg_epgdata d LEFT JOIN epg_epgsource s ON s.id=d.epg_source_id WHERE d.id IN (");
            let mut separated = query.separated(",");
            let mut seen = HashSet::new();
            for id in ids {
                if seen.insert(id) {
                    separated.push_bind(id);
                }
            }
            separated.push_unseparated(")");
            let entries = query
                .build()
                .persistent(false)
                .fetch_all(&mut *conn)
                .await?;
            let ids = entries
                .iter()
                .map(|row| row.get::<i64, _>("id"))
                .collect::<Vec<_>>();
            let sql=format!("SELECT {} FROM epg_programdata p JOIN epg_epgdata e ON e.id=p.epg_id WHERE p.epg_id=ANY($1) AND p.start_time<=$2 AND p.end_time>$2",columns());
            let mut rows = sqlx::query(&sql)
                .bind(ids)
                .bind(now)
                .persistent(false)
                .fetch(&mut *conn);
            let mut first = HashMap::new();
            while let Some(row) = rows.try_next().await? {
                check_deadline(expires)?;
                let id = row.try_get::<i64, _>("relation_id")?;
                if let std::collections::hash_map::Entry::Vacant(entry) = first.entry(id) {
                    let value = project(row, max_bytes)?;
                    retain_bytes(&mut retained, Some(&value), max_bytes)?;
                    entry.insert(value);
                }
            }
            drop(rows);
            for entry in entries {
                check_deadline(expires)?;
                let id = entry.get::<i64, _>("id");
                if let Some(base) = first.remove(&id) {
                    output.append(&base, "epg_data_id", &json!(id))?;
                } else if entry.get::<Option<String>, _>("source_type").as_deref() != Some("dummy")
                {
                    return Err(StoreError::Unsupported(
                        "Current programme byte-offset fallback is not yet implemented",
                    ));
                }
            }
        }
    }
    output.finish()
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reply_serialization_bounds_escaping_and_repeated_programmes() {
        let mut reply = Reply::new(2, 128).unwrap();
        reply
            .append(br#"{"id":7}"#, "channel_uuid", &json!("owned"))
            .unwrap();
        assert!(reply
            .append(br#"{"id":7}"#, "channel_uuid", &json!("\0".repeat(40)))
            .is_err());
        let bytes = reply.bytes.into_data();
        assert!(bytes.len() <= 128);
        assert!(bytes.capacity() <= 128);
        let mut reply = Reply::new(2, 128).unwrap();
        for name in ["first", "second"] {
            reply
                .append(br#"{"id":7}"#, "channel_uuid", &json!(name))
                .unwrap();
        }
        let value: Value = serde_json::from_slice(&reply.finish().unwrap()).unwrap();
        assert_eq!(
            value,
            json!([{"id":7,"channel_uuid":"first"},{"id":7,"channel_uuid":"second"}])
        );
    }
    #[test]
    fn retained_first_programme_bytes_cannot_exceed_output_budget() {
        let mut used = 127;
        assert!(retain_bytes(&mut used, Some(&[0, 0]), 128).is_err());
        assert_eq!(used, 127);
        retain_bytes(&mut used, Some(&[0]), 128).unwrap();
        assert_eq!(used, 128);
    }
    #[test]
    fn expired_batch_deadline_rejects_the_next_query() {
        assert!(matches!(
            check_deadline(std::time::Instant::now() - std::time::Duration::from_millis(1)),
            Err(StoreError::StatusBody(503, _))
        ));
    }
    #[test]
    fn original_int_conversion_precedes_the_fifty_id_slice() {
        let values = vec![json!(1); 50];
        let mut body = json!({"epg_data_ids":values});
        body["epg_data_ids"]
            .as_array_mut()
            .unwrap()
            .push(json!("bad"));
        assert!(matches!(
            CurrentProgramSelection::from_json(&body),
            Err(StoreError::StatusBody(400, _))
        ));
        let parsed =
            CurrentProgramSelection::from_json(&json!({"epg_data_ids":[true,"١","1_0",1.9,1e100]}))
                .unwrap();
        assert!(matches!(parsed,CurrentProgramSelection::EpgData(ids) if ids==[1,1,10,1]));
    }
    #[test]
    fn null_and_empty_id_families_retain_original_branch_meaning() {
        assert!(matches!(
            CurrentProgramSelection::from_json(&json!({})).unwrap(),
            CurrentProgramSelection::Channels(None)
        ));
        assert!(
            matches!(CurrentProgramSelection::from_json(&json!({"channel_uuids":[]})).unwrap(),CurrentProgramSelection::Channels(Some(ids)) if ids.is_empty())
        );
        assert!(matches!(
            CurrentProgramSelection::from_json(&json!({"channel_uuids":[],"epg_data_ids":[]})),
            Err(StoreError::StatusBody(400, _))
        ));
        assert!(matches!(
            CurrentProgramSelection::from_json(&Value::Null),
            Err(StoreError::StatusBody(500, _))
        ));
    }
}
