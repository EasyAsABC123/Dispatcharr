//! Original Timeshift admin statistics projection. Upstream Dispatcharr retains credit.
use dispatcharr_output_store::transport::RedisTransport;
use dispatcharr_store::Store;
use serde_json::{json, Map, Value};
use sqlx::Row;
use std::collections::{BTreeMap, HashMap};

pub(crate) fn empty(now: f64) -> Value {
    json!({"timeshift_sessions":[],"total_connections":0,"timestamp":now})
}
fn text<'a>(value: &'a Map<String, Value>, key: &str, default: &'a str) -> &'a str {
    value.get(key).and_then(Value::as_str).unwrap_or(default)
}
fn number(value: &str) -> Result<f64, ()> {
    value.parse::<f64>().map_err(|_| ())
}
fn nullable_number(value: &str) -> Option<f64> {
    number(value).ok().filter(|value| value.is_finite())
}
fn integer(value: &str) -> Result<i64, ()> {
    value.parse().map_err(|_| ())
}
fn source_bitrate(uptime: f64, bytes: i64) -> Value {
    if uptime > 0.0 && bytes > 0 {
        json!((bytes as f64 * 8.0 / uptime / 1000.0 * 100.0).round_ties_even() / 100.0)
    } else {
        json!(0)
    }
}
pub(crate) fn paused(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "yes" | "true"
    )
}
const SNAPSHOT: &str = r#"
local cursor='0';local scanned=0;local out={};local bytes=0
local function hash(key)
 if redis.call('HLEN',key)>128 then return nil end
 local raw=redis.call('HGETALL',key);local fields={}
 for i=1,#raw,2 do bytes=bytes+#raw[i]+#raw[i+1];if bytes>16777216 then return nil end;fields[raw[i]]=raw[i+1] end
 return fields
end
repeat
 local result=redis.call('SCAN',cursor,'MATCH','timeshift:channel:*:metadata','COUNT',100);cursor=result[1]
 for _,key in ipairs(result[2]) do
  scanned=scanned+1;if scanned>50000 then return redis.error_reply('stats scan limit') end
  local id=string.match(key,'^timeshift:channel:(.+):metadata$')
  if id and string.match(id,'^%d+_.+$') then
   local meta=hash(key);if not meta then return redis.error_reply('stats byte limit') end
   local clients=redis.call('SMEMBERS','timeshift:channel:'..id..':clients')
   if #clients>4096 then return redis.error_reply('stats client limit') end
   for _,client in ipairs(clients) do
    local data=hash('timeshift:channel:'..id..':clients:'..client)
    if not data then return redis.error_reply('stats byte limit') end
    if data.programme_start and data.programme_start~='' then
     out[#out+1]={id=id,client_id=client,metadata=meta,client=data}
     if #out>4096 then return redis.error_reply('stats connection limit') end
    end
   end
  end
 end
until cursor=='0'
return cjson.encode(out)
"#;

pub(crate) async fn stats(store: &Store, transport: &RedisTransport, now: f64) -> Value {
    project(store, transport, now, ())
        .await
        .unwrap_or_else(|_| empty(now))
}
pub(crate) async fn stats_guarded<G: Send + 'static>(
    store: &Store,
    transport: &RedisTransport,
    now: f64,
    guard: G,
) -> Value {
    project(store, transport, now, guard)
        .await
        .unwrap_or_else(|_| empty(now))
}
async fn project<G: Send + 'static>(
    store: &Store,
    transport: &RedisTransport,
    now: f64,
    guard: G,
) -> Result<Value, ()> {
    let (raw, _guard): (String, G) = transport
        .command(SNAPSHOT, &[], &[], guard)
        .await
        .map_err(|_| ())?;
    let raw: Value = serde_json::from_str(&raw).map_err(|_| ())?;
    let rows = raw.as_array().cloned().unwrap_or_default();
    let mut grouped = BTreeMap::<String, Value>::new();
    let mut channels = HashMap::<i64, Option<(String, Option<i64>)>>::new();
    let mut profiles = HashMap::<i64, Value>::new();
    let mut count = 0;
    for row in rows {
        let id = row["id"].as_str().ok_or(())?;
        let (channel_segment, sid) = id.split_once('_').ok_or(())?;
        let meta = row["metadata"].as_object().ok_or(())?;
        let client = row["client"].as_object().ok_or(())?;
        let Ok(channel_id) = integer(text(meta, "channel_id", channel_segment)) else {
            continue;
        };
        let channel = if let Some(channel) = channels.get(&channel_id) {
            channel.clone()
        } else {
            let result = sqlx::query("SELECT name,logo_id::bigint AS logo_id FROM dispatcharr_channels_channel WHERE id=$1")
                .bind(channel_id).fetch_optional(store.pool()).await.map_err(|_| ())?;
            let result = result
                .map(|row| {
                    Ok::<_, sqlx::Error>((
                        row.try_get::<String, _>("name")?,
                        row.try_get::<Option<i64>, _>("logo_id")?,
                    ))
                })
                .transpose()
                .map_err(|_| ())?;
            channels.insert(channel_id, result.clone());
            result
        };
        let mut name = text(meta, "channel_name", "Catch-up").to_owned();
        let mut logo = match text(meta, "logo_id", "") {
            "" => None,
            value => Some(integer(value)?),
        };
        if let Some((actual_name, actual_logo)) = channel {
            name = actual_name;
            if actual_logo.is_some() {
                logo = actual_logo;
            }
        }
        let profile_id = match text(meta, "m3u_profile", "") {
            "" => None,
            value => Some(integer(value)?),
        };
        let mut profile = json!({});
        if let Some(profile_id) = profile_id {
            if let Some(value) = profiles.get(&profile_id) {
                profile = value.clone()
            } else {
                if let Some(row)=sqlx::query("SELECT p.name AS profile_name,a.name AS account_name,a.id::bigint AS account_id FROM m3u_m3uaccountprofile p JOIN m3u_m3uaccount a ON a.id=p.m3u_account_id WHERE p.id=$1").bind(profile_id).fetch_optional(store.pool()).await.map_err(|_|())? {
                    profile=json!({"profile_name":row.try_get::<String,_>("profile_name").map_err(|_|())?,"account_name":row.try_get::<String,_>("account_name").map_err(|_|())?,"account_id":row.try_get::<i64,_>("account_id").map_err(|_|())?,"m3u_profile_id":profile_id});
                }
                profiles.insert(profile_id, profile.clone());
            }
        }
        let init = number(text(meta, "init_time", "0"))?;
        let uptime = if init > 0.0 { now - init } else { 0.0 };
        let bytes = integer(text(meta, "total_bytes", "0"))?;
        let bitrate = source_bitrate(uptime, bytes);
        let connected = text(client, "connected_at", "");
        let duration = if connected.is_empty() {
            0
        } else {
            nullable_number(connected).map_or(0, |at| (now - at) as i64)
        };
        let connected = if connected.is_empty() {
            None
        } else {
            Some(number(connected)?)
        };
        let session=grouped.entry(sid.to_owned()).or_insert_with(||json!({
            "session_id":sid,"stats_channel_id":id,"channel_id":channel_id,"channel_uuid":text(meta,"channel_uuid",""),"channel_name":name,"logo_id":logo,"programme_start":text(client,"programme_start",""),"position_anchor_at":nullable_number(text(client,"position_anchor_at","")),"playback_base_secs":nullable_number(text(client,"playback_base_secs","")),"paused":paused(text(client,"paused","")),"connection_count":0,"connections":[],
            "resolution":meta.get("resolution"),"source_fps":meta.get("source_fps"),"video_codec":meta.get("video_codec"),"audio_codec":meta.get("audio_codec"),"audio_channels":meta.get("audio_channels"),"stream_type":meta.get("stream_type")
        }));
        session["connection_count"] = json!(session["connection_count"].as_u64().unwrap_or(0) + 1);
        session["connections"].as_array_mut().ok_or(())?.push(json!({"client_id":row["client_id"],"session_id":sid,"ip_address":text(client,"ip_address","Unknown"),"user_agent":text(client,"user_agent","unknown"),"user_id":text(client,"user_id","0"),"username":text(client,"username","unknown"),"connected_at":connected,"duration":duration,"bytes_streamed":bytes,"avg_bitrate_kbps":bitrate,"m3u_profile":profile,"m3u_profile_id":profile_id}));
        count += 1;
    }
    Ok(
        json!({"timeshift_sessions":grouped.into_values().collect::<Vec<_>>(),"total_connections":count,"timestamp":now}),
    )
}

#[cfg(test)]
mod combined_stats_scalar_tests {
    use super::*;
    #[test]
    fn source_skipped_bitrate_is_int_but_computed_rounded_zero_remains_float() {
        assert_eq!(source_bitrate(0.0, 1880).as_i64(), Some(0));
        assert_eq!(source_bitrate(10.0, 0).as_i64(), Some(0));
        let computed = source_bitrate(1_000_000.0, 1);
        assert_eq!(computed.as_f64(), Some(0.0));
        assert!(computed.as_i64().is_none());
    }
}
