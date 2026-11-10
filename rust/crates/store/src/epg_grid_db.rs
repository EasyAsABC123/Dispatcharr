//! Original Dispatcharr guide visibility and stored-row component.
//! Original authors retain full application credit. No HTTP attachment.
use crate::{
    channels::{scope, truthy},
    grid_encode_batch, grid_program_value, Store, StoreError, User,
};
use chrono::{DateTime, SecondsFormat, Utc};
use futures_util::TryStreamExt;
use serde_json::{json, Value};
use sqlx::{Postgres, QueryBuilder, Row};
use std::time::{Duration, Instant};
use uuid::Uuid;

#[derive(Debug)]
pub struct GridChannel {
    pub id: i64,
    pub uuid: Uuid,
    pub name: String,
    pub epg_id: Option<i64>,
    pub source_type: Option<String>,
    pub source_properties: Value,
}
fn capacity() -> StoreError {
    StoreError::StatusBody(503, json!({"detail":"Guide read capacity unavailable."}))
}
fn deadline(expires: Instant) -> Result<(), StoreError> {
    if Instant::now() >= expires {
        Err(capacity())
    } else {
        Ok(())
    }
}
// Conservative retained DOM charge, including per-node/map overhead, so tiny
// JSON values cannot evade the aggregate bound through serialized byte size.
fn json_charge(value: &Value) -> usize {
    let nested = match value {
        Value::Array(values) => values.iter().map(json_charge).sum(),
        Value::Object(values) => values
            .iter()
            .map(|(k, v)| 128 + 2 * k.len() + json_charge(v))
            .sum(),
        Value::String(value) => 2 * value.len(),
        _ => 32,
    };
    128 + nested
}
fn integer(row: &sqlx::postgres::PgRow, name: &str) -> Result<Option<i64>, StoreError> {
    match row.try_get::<Option<i64>, _>(name) {
        Ok(v) => Ok(v),
        Err(_) => Ok(row.try_get::<Option<i32>, _>(name)?.map(i64::from)),
    }
}
fn visibility(query: &mut QueryBuilder<'_, Postgres>, user: &User, profile: Option<i64>) {
    query.push(" AND NOT c.hidden_from_output");
    if let Some(profile) = profile {
        if user.user_level < 10 {
            query.push(" AND c.user_level<=").push_bind(user.user_level);
            if truthy(user.custom_properties.get("hide_adult_content")) {
                query.push(" AND NOT c.is_adult");
            }
            query.push(" AND (NOT EXISTS(SELECT 1 FROM accounts_user_channel_profiles up WHERE up.user_id=").push_bind(user.id)
                .push(") OR EXISTS(SELECT 1 FROM accounts_user_channel_profiles up WHERE up.user_id=").push_bind(user.id)
                .push(" AND up.channelprofile_id=").push_bind(profile).push("))");
        }
        query.push(" AND EXISTS(SELECT 1 FROM dispatcharr_channels_channelprofilemembership m WHERE m.channel_id=c.id AND m.enabled AND m.channel_profile_id=").push_bind(profile).push(")");
    } else {
        scope(query, user);
    }
}
impl Store {
    /// Caller owns admission before querying; retained metadata is capped at
    /// 16MiB and50K rows. Overflow is explicit rather than silently truncated.
    /// Each worker drains rollback before returning, including SQL errors.
    pub async fn grid_visible_channels(
        &self,
        user: &User,
        profile: Option<&str>,
    ) -> Result<Vec<GridChannel>, StoreError> {
        let profile = match profile {
            Some(v) => match v.parse::<i64>() {
                Ok(v) if v > 0 => Some(v),
                _ => return Ok(Vec::new()),
            },
            None => None,
        };
        let expires = Instant::now() + Duration::from_secs(2);
        let mut tx = self.pool().begin().await?;
        let result = async {
            sqlx::query("SET LOCAL statement_timeout='2000ms'")
                .execute(&mut *tx)
                .await?;
            sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                .execute(&mut *tx)
                .await?;
            // The bounded guard is a separate SELECT. Acquire its phase's
            // relations before that SELECT can pin the repeatable-read snapshot,
            // matching Source visibility after a real relation-lock wait.
            let mut tables = "dispatcharr_channels_channel, dispatcharr_channels_channeloverride, epg_epgdata, epg_epgsource".to_owned();
            if user.user_level < 10 {
                tables.push_str(", accounts_user_channel_profiles");
            }
            if profile.is_some() || user.user_level < 10 {
                tables.push_str(", dispatcharr_channels_channelprofilemembership");
            }
            sqlx::query(&format!("LOCK TABLE {tables} IN ACCESS SHARE MODE"))
                .execute(&mut *tx)
                .await?;
            // Preserve Source projection widths and join planning. A bounded
            // EXISTS preflight in the same snapshot prevents large text/JSON
            // values crossing the subsequent client boundary.
            let source=include_str!("../datasets/source-grid-channel-query.sql");
            let from=source.split_once("\nFROM ").ok_or_else(capacity)?.1;
            let mut guard=QueryBuilder::new(format!("SELECT EXISTS(SELECT 1 FROM {from}"));
            visibility(&mut guard,user,profile);
            guard.push(" AND (coalesce(octet_length(bs.custom_properties::text),0)>65536 OR coalesce(octet_length(os.custom_properties::text),0)>65536 OR coalesce(octet_length(bs.last_message),0)>65536 OR coalesce(octet_length(os.last_message),0)>65536)) AS oversized");
            if guard.build().fetch_one(&mut *tx).await?.try_get::<bool,_>("oversized")? {return Err(capacity());}
            let mut query=QueryBuilder::new(source);
            visibility(&mut query,user,profile);
            let mut stream = query.build().persistent(false).fetch(&mut *tx);
            let mut rows = Vec::new();
            let mut bytes = 0usize;
            while let Some(row) = stream.try_next().await? {
                deadline(expires)?;
                if rows.len() >= 50000 {return Err(capacity());}
                let override_epg = integer(&row,"oe_id")?;
                let source = if override_epg.is_some() { "os" } else { "bs" };
                let value = GridChannel {
                    id: integer(&row,"c_id")?.ok_or_else(capacity)?,
                    uuid: row.try_get("c_uuid")?,
                    name: row.try_get("effective_name")?,
                    epg_id: integer(&row,"effective_epg_data_id")?,
                    source_type: row.try_get(format!("{source}_source_type").as_str())?,
                    source_properties: row
                        .try_get::<Option<Value>, _>(
                            format!("{source}_custom_properties").as_str(),
                        )?
                        .unwrap_or(Value::Null),
                };
                bytes = bytes
                    .checked_add(
                        value.name.len()
                            + value.source_type.as_ref().map_or(0, String::len)
                            + json_charge(&value.source_properties)
                            + 256,
                    )
                    .filter(|n| *n <= 16 * 1024 * 1024)
                    .ok_or_else(capacity)?;
                rows.push(value);
            }
            Ok(rows)
        }
        .await;
        tx.rollback().await?;
        result
    }
    /// Stored programmes only, with Source overlap predicates and unordered
    /// scan order. Returns original500-row frames; no fake dummy rows are added.
    /// The caller owns admission and eventual response-frame lifetimes.
    pub async fn grid_stored_frames(
        &self,
        epg_ids: &[i64],
        lookback: DateTime<Utc>,
        cutoff: DateTime<Utc>,
    ) -> Result<Vec<Vec<u8>>, StoreError> {
        if epg_ids.len() > 50000 {
            return Err(capacity());
        }
        if epg_ids.is_empty() {
            return Ok(Vec::new());
        }
        let expires = Instant::now() + Duration::from_secs(2);
        let mut tx = self.pool().begin().await?;
        sqlx::query("SET LOCAL statement_timeout='2000ms'")
            .execute(&mut *tx)
            .await?;
        let result=async {
            let size="coalesce(octet_length(p.title),0)::bigint+coalesce(octet_length(p.sub_title),0)+coalesce(octet_length(p.description),0)+coalesce(octet_length(p.tvg_id),0)+coalesce(octet_length(p.custom_properties::text),0)";
            let sql=format!("SELECT p.id::bigint AS id,p.start_time,p.end_time,({size}>1048576) AS oversized,CASE WHEN {size}<=1048576 THEN jsonb_build_object('title',p.title,'sub_title',p.sub_title,'description',p.description,'tvg_id',p.tvg_id,'season',p.custom_properties->'season','episode',p.custom_properties->'episode','flag_new',p.custom_properties->'new','flag_live',p.custom_properties->'live','flag_premiere',p.custom_properties->'premiere','premiere_text',p.custom_properties->'premiere_text') END AS value FROM epg_programdata p WHERE p.epg_id=ANY($1) AND p.end_time>$2 AND p.start_time<$3 LIMIT 50001");
            let mut stream=sqlx::query(&sql).bind(epg_ids).bind(lookback).bind(cutoff).persistent(false).fetch(&mut *tx);
            let mut frames=Vec::new();let mut batch=Vec::new();let mut first=true;let mut bytes=0usize;let mut count=0usize;let mut batch_charge=0usize;
            while let Some(row)=stream.try_next().await? {
                deadline(expires)?;count+=1;
                if count>50000||row.try_get::<bool,_>("oversized")? {return Err(capacity());}
                let mut value:Value=row.try_get("value")?;
                value["id"]=row.try_get::<i64,_>("id")?.into();
                for name in ["start_time","end_time"] {
                    let time:DateTime<Utc>=row.try_get(name)?;
                    value[name]=json!({"datetime":time.to_rfc3339_opts(SecondsFormat::Micros,false)});
                }
                let projected=grid_program_value(&value).map_err(|_|capacity())?;
                batch_charge=batch_charge.checked_add(json_charge(&projected)).filter(|n|*n<=16*1024*1024).ok_or_else(capacity)?;
                batch.push(projected);
                if batch.len()==500 {
                    let (frame,next)=grid_encode_batch(&batch,first,16*1024*1024).map_err(|_|capacity())?;
                    bytes=bytes.checked_add(frame.len()).filter(|n|*n<=32*1024*1024).ok_or_else(capacity)?;
                    frames.push(frame);first=next;batch.clear();batch_charge=0;
                }
            }
            if !batch.is_empty() {
                let (frame,_)=grid_encode_batch(&batch,first,16*1024*1024).map_err(|_|capacity())?;
                if bytes+frame.len()>32*1024*1024 {return Err(capacity());}frames.push(frame);
            }
            Ok(frames)
        }.await;
        tx.rollback().await?;
        result
    }
}
