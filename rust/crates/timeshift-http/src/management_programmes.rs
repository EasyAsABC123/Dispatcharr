//! Original Dispatcharr programme/playhead lookup for Timeshift management.
//! All original application credit belongs to upstream authors/contributors.
use super::management_data::paused;
use dispatcharr_output_store::transport::RedisTransport;
use dispatcharr_store::{Store, StoreError};
use serde_json::{json, Value};
use sqlx::Row;
use std::collections::BTreeMap;

pub(crate) const FIND: &str = r#"
local cursor='0';local n=0
repeat
 local result=redis.call('SCAN',cursor,'MATCH','timeshift:channel:*:clients:'..ARGV[1],'COUNT',100);cursor=result[1]
 for _,key in ipairs(result[2]) do
  local id=string.match(key,'^timeshift:channel:(.+):clients:.+$');if id then return id end
  n=n+1;if n>50000 then return redis.error_reply('session scan limit') end
 end
until cursor=='0'
return nil
"#;
fn numeric(value: &Value) -> Option<f64> {
    match value {
        Value::Number(v) => v.as_f64(),
        Value::String(v) => v.trim().parse().ok(),
        Value::Bool(v) => Some(if *v { 1.0 } else { 0.0 }),
        _ => None,
    }
}
fn stamp(value: &str) -> String {
    let value = value.trim_end_matches('0').trim_end_matches('.');
    format!("{value}+00:00")
}
async fn programme(
    store: &Store,
    channel: uuid::Uuid,
    at: f64,
) -> Result<Option<Value>, StoreError> {
    let row=sqlx::query("SELECT p.title,COALESCE(p.sub_title,'') AS sub_title,COALESCE(p.description,'') AS description,to_char(p.start_time AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US') AS start_time,to_char(p.end_time AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US') AS end_time,EXTRACT(EPOCH FROM p.start_time)::float8 AS start,EXTRACT(EPOCH FROM p.end_time)::float8 AS end FROM epg_programdata p JOIN dispatcharr_channels_channel c ON c.epg_data_id=p.epg_id WHERE c.uuid=$1 AND p.start_time<=to_timestamp($2) AND p.end_time>to_timestamp($2) ORDER BY p.id LIMIT 1")
        .bind(channel).bind(at).fetch_optional(store.pool()).await?;
    row.map(|row| Ok(json!({"title":row.try_get::<String,_>("title")?,"sub_title":row.try_get::<String,_>("sub_title")?,"description":row.try_get::<String,_>("description")?,"start_time":stamp(&row.try_get::<String,_>("start_time")?),"end_time":stamp(&row.try_get::<String,_>("end_time")?),"start":row.try_get::<f64,_>("start")?,"end":row.try_get::<f64,_>("end")?}))).transpose()
}
async fn redis_position(
    transport: &RedisTransport,
    sid: &str,
    url_at: f64,
    epg_start: f64,
    now: f64,
) -> Option<f64> {
    let (id, _): (Option<String>, ()) = transport
        .command(FIND, &[], &[sid.as_bytes()], ())
        .await
        .ok()?;
    let key = format!("timeshift:channel:{}:clients:{sid}", id?);
    let (fields,_):(BTreeMap<String,String>,())=transport.command("if redis.call('HLEN',KEYS[1])>128 then return redis.error_reply('client field limit') end return redis.call('HGETALL',KEYS[1])",&[key],&[],()).await.ok()?;
    if fields.is_empty() {
        return None;
    }
    let elapsed = if fields.get("paused").is_some_and(|v| paused(v)) {
        0.0
    } else {
        fields
            .get("position_anchor_at")
            .and_then(|v| v.parse::<f64>().ok())
            .map_or(0.0, |v| (now - v).max(0.0))
    };
    let base = fields
        .get("playback_base_secs")
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(url_at - epg_start);
    Some((base + elapsed).max(0.0))
}
pub(crate) async fn programmes(
    store: &Store,
    transport: &RedisTransport,
    sessions: &[Value],
    now: f64,
) -> Result<Value, StoreError> {
    let mut out = Vec::new();
    for session in sessions
        .iter()
        .filter(|v| {
            ["channel_uuid", "programme_start", "session_id"]
                .iter()
                .all(|key| v.get(*key).is_some_and(truthy))
        })
        .take(50)
    {
        let channel_string = string(&session["channel_uuid"]);
        let mut entry = json!({"session_id":session["session_id"],"channel_uuid":channel_string,"programme_start":session["programme_start"]});
        let channel = uuid::Uuid::parse_str(&channel_string)
            .map_err(|_| StoreError::Unsupported("malformed programme channel UUID"))?;
        if let Some(at) = session["programme_start"]
            .as_str()
            .and_then(dispatcharr_timeshift_core::parse_timestamp)
        {
            let at = at.and_utc().timestamp() as f64;
            if let Some(mut info) = programme(store, channel, at).await? {
                let start = info["start"].as_f64().unwrap();
                let end = info["end"].as_f64().unwrap();
                let position = match session.get("position_secs").and_then(numeric) {
                    Some(value) => Some(value),
                    None => {
                        redis_position(transport, &string(&session["session_id"]), at, start, now)
                            .await
                    }
                };
                if let Some(position) = position {
                    let playhead = start + position.max(0.0);
                    if playhead >= end {
                        if let Some(next) = programme(store, channel, playhead).await? {
                            info = next;
                        }
                    }
                }
                let duration =
                    (info["end"].as_f64().unwrap() - info["start"].as_f64().unwrap()) as i64;
                info.as_object_mut().unwrap().remove("start");
                info.as_object_mut().unwrap().remove("end");
                entry
                    .as_object_mut()
                    .unwrap()
                    .extend(info.as_object().unwrap().clone());
                entry["duration_secs"] = json!(duration);
            }
        }
        out.push(entry);
    }
    Ok(json!({"sessions":out}))
}
pub(crate) fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(v) => *v,
        Value::Number(v) => v.as_f64() != Some(0.0),
        Value::String(v) => !v.is_empty(),
        Value::Array(v) => !v.is_empty(),
        Value::Object(v) => !v.is_empty(),
    }
}
pub(crate) fn string(value: &Value) -> String {
    match value {
        Value::String(v) => v.clone(),
        Value::Null => "None".into(),
        Value::Bool(v) => if *v { "True" } else { "False" }.into(),
        v => v.to_string(),
    }
}
