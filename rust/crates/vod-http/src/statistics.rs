//! Original VOD statistics projection, ported from apps/proxy/vod_proxy/views.py.
//! All application credit belongs to original Dispatcharr authors/contributors.
use dispatcharr_output_store::transport::RedisTransport;
use dispatcharr_store::Store;
use serde_json::{json, Map, Value};
use sqlx::Row;
use std::collections::HashMap;

#[derive(Debug)]
pub(crate) enum Error {
    Capacity,
    Invalid,
}
fn text<'a>(row: &'a Map<String, Value>, key: &str, default: &'a str) -> &'a str {
    row.get(key).and_then(Value::as_str).unwrap_or(default)
}
fn integer(value: &str) -> Result<i64, Error> {
    value.trim().parse().map_err(|_| Error::Invalid)
}
fn float(value: &str) -> Result<f64, Error> {
    value
        .trim()
        .parse::<f64>()
        .ok()
        .filter(|v| v.is_finite())
        .ok_or(Error::Invalid)
}
fn trunc(value: f64) -> Result<i64, Error> {
    if !value.is_finite() || value < i64::MIN as f64 || value >= i64::MAX as f64 {
        Err(Error::Invalid)
    } else {
        Ok(value as i64)
    }
}
async fn content(
    store: &Store,
    kind: &str,
    uuid: &str,
    row: &Map<String, Value>,
) -> Result<(String, Value), Error> {
    let valid_uuid = uuid::Uuid::parse_str(uuid).ok();
    let result = if let Some(uuid) = valid_uuid {
        match kind {
            "movie"=>sqlx::query("SELECT m.name,CASE WHEN octet_length(COALESCE(m.description,''))>65536 OR octet_length(COALESCE(l.url,''))>65536 THEN NULL ELSE jsonb_build_object('year',m.year,'rating',m.rating,'genre',m.genre,'duration_secs',m.duration_secs,'description',m.description,'logo_url',l.url,'tmdb_id',m.tmdb_id,'imdb_id',m.imdb_id) END AS metadata FROM vod_movie m LEFT JOIN vod_vodlogo l ON l.id=m.logo_id WHERE m.uuid=$1").bind(uuid).fetch_optional(store.pool()).await.ok().flatten(),
            "episode"=>sqlx::query("SELECT s.name||' - '||e.name AS name,CASE WHEN octet_length(COALESCE(e.description,''))>65536 OR octet_length(COALESCE(l.url,''))>65536 THEN NULL ELSE jsonb_build_object('series_name',s.name,'episode_name',e.name,'season_number',e.season_number,'episode_number',e.episode_number,'air_date',e.air_date::text,'rating',e.rating,'duration_secs',e.duration_secs,'description',e.description,'logo_url',l.url,'series_year',s.year,'series_genre',s.genre,'tmdb_id',e.tmdb_id,'imdb_id',e.imdb_id) END AS metadata FROM vod_episode e JOIN vod_series s ON s.id=e.series_id LEFT JOIN vod_vodlogo l ON l.id=s.logo_id WHERE e.uuid=$1").bind(uuid).fetch_optional(store.pool()).await.ok().flatten(),
            _=>None,
        }
    } else {
        None
    };
    let Some(result) = result else {
        return Ok(("Unknown".into(), json!({})));
    };
    let name: String = result.try_get("name").map_err(|_| Error::Invalid)?;
    let mut metadata: Value = result
        .try_get::<Option<Value>, _>("metadata")
        .map_err(|_| Error::Invalid)?
        .ok_or(Error::Capacity)?;
    if metadata["duration_secs"]
        .as_i64()
        .is_none_or(|duration| duration == 0)
    {
        metadata["duration_secs"] = if kind == "episode" {
            json!(2400)
        } else {
            // The original parses all three fields before estimating duration.
            let inputs = (|| {
                Ok::<_, Error>((
                    integer(text(row, "total_content_size", "0"))?,
                    integer(text(row, "last_seek_byte", "0"))?,
                    float(text(row, "last_seek_percentage", "0.0"))?,
                ))
            })();
            match inputs {
                Ok((size, _, percent)) => {
                    if size > 0 && percent > 0.0 {
                        json!(6000)
                    } else {
                        Value::Null
                    }
                }
                Err(_) => return Ok((name, json!({}))),
            }
        };
    }
    Ok((name, metadata))
}
async fn profile(store: &Store, row: &Map<String, Value>) -> Value {
    let id = row.get("m3u_profile_id").cloned().unwrap_or(Value::Null);
    if let Some(parsed) = id
        .as_str()
        .filter(|v| !v.is_empty())
        .and_then(|v| integer(v).ok())
    {
        let value=sqlx::query_scalar::<_,Value>("SELECT jsonb_build_object('profile_name',p.name,'account_name',a.name,'account_id',a.id,'max_streams',a.max_streams,'m3u_profile_id',p.id) FROM m3u_m3uaccountprofile p JOIN m3u_m3uaccount a ON a.id=p.m3u_account_id WHERE p.id=$1").bind(parsed).fetch_optional(store.pool()).await.ok().flatten();
        if let Some(value) = value {
            return value;
        }
    }
    if !text(row, "m3u_profile_name", "").is_empty() {
        json!({"profile_name":text(row,"m3u_profile_name","Unknown Profile"),"m3u_profile_id":id,"account_name":"Unknown Account"})
    } else {
        json!({})
    }
}
struct Projection<'a> {
    id: &'a str,
    kind: &'a str,
    uuid: &'a str,
    name: &'a str,
    metadata: &'a Value,
}
fn connection(
    row: &Map<String, Value>,
    context: Projection<'_>,
    profile: Value,
    now: f64,
) -> Result<Value, Error> {
    let Projection {
        id,
        kind,
        uuid,
        name,
        metadata,
    } = context;
    let known = integer(text(row, "position_seconds", "0"))?;
    let percentage = float(text(row, "last_seek_percentage", "0.0"))?;
    let seek_time = float(text(row, "last_seek_timestamp", "0.0"))?;
    let duration = metadata["duration_secs"].as_i64().filter(|v| *v != 0);
    let mut position = known;
    if let Some(duration) = duration {
        if percentage > 0.0 {
            if let Ok(seek_position) = trunc(percentage / 100.0 * duration as f64) {
                position = if seek_time > 0.0 {
                    seek_position
                        .saturating_add(trunc(now - seek_time)?)
                        .min(duration)
                } else {
                    seek_position
                };
            }
        } else if let Some(update) = row
            .get("last_position_update")
            .and_then(Value::as_str)
            .filter(|v| !v.is_empty())
            .and_then(|v| float(v).ok())
        {
            if let Ok(elapsed) = trunc(now - update) {
                position = known.saturating_add(elapsed).min(duration);
            }
        }
    }
    let duration = row
        .get("created_at")
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty())
        .and_then(|v| float(v).ok())
        .and_then(|at| trunc(now - at).ok())
        .or_else(|| {
            if !text(row, "last_activity", "").is_empty() && id.starts_with("vod_") {
                id.split('_')
                    .nth(1)
                    .and_then(|v| float(v).ok())
                    .and_then(|at| trunc(now - at / 1000.0).ok())
            } else {
                None
            }
        })
        .unwrap_or(0);
    Ok(
        json!({"content_type":kind,"content_uuid":uuid,"content_name":name,"content_metadata":metadata,"m3u_profile":profile,"client_id":id,"client_ip":text(row,"client_ip","Unknown"),"user_id":text(row,"user_id","0"),"user_agent":text(row,"client_user_agent","Unknown"),"connected_at":row.get("created_at"),"last_activity":row.get("last_activity"),"m3u_profile_id":row.get("m3u_profile_id"),"position_seconds":position,"last_known_position":known,"last_position_update":row.get("last_position_update"),"bytes_sent":integer(text(row,"bytes_sent","0"))?,"last_seek_byte":integer(text(row,"last_seek_byte","0"))?,"last_seek_percentage":percentage,"total_content_size":integer(text(row,"total_content_size","0"))?,"last_seek_timestamp":seek_time,"duration":duration}),
    )
}
const SNAPSHOT: &str = r#"
local cursor='0';local iterations=0;local rows={};local bytes=0;local scanned=0
repeat
 iterations=iterations+1;if iterations>10000 then return redis.error_reply('VOD stats scan limit') end
 local result=redis.call('SCAN',cursor,'MATCH','vod_persistent_connection:*','COUNT',100);cursor=result[1]
 for _,key in ipairs(result[2]) do
  scanned=scanned+1;if scanned>50000 then return redis.error_reply('VOD stats scan limit') end
  local kind=redis.call('TYPE',key);if type(kind)=='table' then kind=kind.ok end
  if kind=='hash' then
   local names=redis.call('HKEYS',key);if #names>128 then return redis.error_reply('VOD stats field limit') end
   local data={}
   for _,name in ipairs(names) do
    bytes=bytes+#name+redis.call('HSTRLEN',key,name)
    if bytes>16777216 then return redis.error_reply('VOD stats byte limit') end
    data[name]=redis.call('HGET',key,name)
   end
   if #names>0 then
    rows[#rows+1]={session_id=string.sub(key,string.len('vod_persistent_connection:')+1),row=data}
    if #rows>4096 then return redis.error_reply('VOD stats connection limit') end
   end
  end
 end
until cursor=='0'
if #rows==0 then return '[]' end
local encoded=cjson.encode(rows)
if #encoded>16777216 then return redis.error_reply('VOD stats encoded limit') end
return encoded
"#;
pub(crate) async fn snapshot<G: Send + 'static>(
    store: &Store,
    transport: &RedisTransport,
    now: f64,
    guard: G,
) -> Result<Value, Error> {
    let (raw, _guard): (String, G) = transport
        .command(SNAPSHOT, &[], &[], guard)
        .await
        .map_err(|_| Error::Capacity)?;
    let rows: Vec<Value> = serde_json::from_str(&raw).map_err(|_| Error::Invalid)?;
    project(store, &rows, now).await
}
pub(crate) async fn project(store: &Store, rows: &[Value], now: f64) -> Result<Value, Error> {
    if rows.len() > 4096 || !now.is_finite() {
        return Err(Error::Capacity);
    }
    let mut groups: Vec<Value> = Vec::new();
    let mut indexes = HashMap::<String, usize>::new();
    let mut count = 0;
    for input in rows {
        let Some(row) = input["row"].as_object().filter(|row| !row.is_empty()) else {
            continue;
        };
        let Some(id) = input["session_id"].as_str() else {
            continue;
        };
        let kind = text(row, "content_obj_type", "unknown");
        let uuid = text(row, "content_uuid", "unknown");
        let (name, metadata) = content(store, kind, uuid, row).await?;
        let profile = profile(store, row).await;
        let Ok(connection) = connection(
            row,
            Projection {
                id,
                kind,
                uuid,
                name: &name,
                metadata: &metadata,
            },
            profile,
            now,
        ) else {
            continue;
        };
        let key = format!("{kind}:{uuid}");
        let index=*indexes.entry(key).or_insert_with(||{groups.push(json!({"content_type":kind,"content_name":name,"content_uuid":uuid,"content_metadata":metadata,"connection_count":0,"connections":[]}));groups.len()-1});
        groups[index]["connection_count"] =
            json!(groups[index]["connection_count"].as_u64().unwrap_or(0) + 1);
        groups[index]["connections"]
            .as_array_mut()
            .ok_or(Error::Invalid)?
            .push(connection);
        count += 1;
    }
    let value = json!({"vod_connections":groups,"total_connections":count,"timestamp":now});
    if serde_json::to_vec(&value)
        .map_err(|_| Error::Invalid)?
        .len()
        > 16 * 1024 * 1024
    {
        return Err(Error::Capacity);
    }
    Ok(value)
}

#[cfg(test)]
#[tokio::test]
#[ignore = "requires disposable original VOD producer ORM fixture"]
async fn original_vod_statistics_match_real_orm_source() {
    assert_eq!(
        std::env::var("VOD_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let database = std::env::var("DATABASE_TEST_URL").unwrap();
    assert!(database.contains("playback-fixture-only@"));
    let store = Store::connect(&database, 8).await.unwrap();
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("VOD_PRODUCER_ORACLE").unwrap()).unwrap(),
    )
    .unwrap();
    let records = &fixture["database_records"];
    let logo = &records["logo"];
    sqlx::query("INSERT INTO vod_vodlogo(id,name,url) VALUES($1,$2,$3) ON CONFLICT(id) DO UPDATE SET name=EXCLUDED.name,url=EXCLUDED.url")
        .bind(logo["id"].as_i64().unwrap()).bind(logo["name"].as_str().unwrap()).bind(logo["url"].as_str().unwrap()).execute(store.pool()).await.unwrap();
    let mut original = HashMap::new();
    for (kind,table,statement) in [
        ("movie","vod_movie","UPDATE vod_movie SET year=($1->>'year')::int,rating=$1->>'rating',genre=$1->>'genre',description=$1->>'description',tmdb_id=$1->>'tmdb_id',imdb_id=$1->>'imdb_id',logo_id=($1->>'logo_id')::bigint,duration_secs=($1->>'duration_secs')::int WHERE id=$2"),
        ("episode","vod_episode","UPDATE vod_episode SET air_date=($1->>'air_date')::date,rating=$1->>'rating',description=$1->>'description',tmdb_id=$1->>'tmdb_id',imdb_id=$1->>'imdb_id',season_number=($1->>'season_number')::int,episode_number=($1->>'episode_number')::int WHERE id=$2"),
        ("series","vod_series","UPDATE vod_series SET year=($1->>'year')::int,genre=$1->>'genre',logo_id=($1->>'logo_id')::bigint WHERE id=$2"),
    ] {
        let id=records[kind]["id"].as_i64().unwrap();
        let before:Value=sqlx::query_scalar(&format!("SELECT to_jsonb(t) FROM {table} t WHERE id=$1")).bind(id).fetch_one(store.pool()).await.unwrap();
        sqlx::query(statement).bind(&records[kind]["values"]).bind(id).execute(store.pool()).await.unwrap();
        original.insert(kind,(statement,id,before));
    }
    let redis_url = std::env::var("REDIS_TEST_URL").unwrap();
    assert!(redis_url.contains("playback-fixture-only@"));
    let transport = RedisTransport::connect(
        &redis_url,
        dispatcharr_output_store::transport::TransportConfig {
            max_operations: 1,
            max_queued_operations: 1,
            command_timeout: std::time::Duration::from_secs(3),
        },
    )
    .await
    .unwrap();
    let mut redis = redis::Client::open(redis_url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let now = fixture["clock"].as_f64().unwrap();
    let mut count = 0;
    for case in fixture["stats_cases"].as_array().unwrap() {
        sqlx::query("UPDATE vod_movie SET duration_secs=$1 WHERE id=$2")
            .bind(case["movie_duration_secs"].as_i64().map(|v| v as i32))
            .bind(records["movie"]["id"].as_i64().unwrap())
            .execute(store.pool())
            .await
            .unwrap();
        let rows = [case.clone()];
        let actual = project(&store, &rows, now).await.unwrap();
        assert_eq!(
            actual, case["expected"],
            "Source VOD stats case {}",
            case["name"]
        );
        let key = format!(
            "vod_persistent_connection:{}",
            case["session_id"].as_str().unwrap()
        );
        let fields: Vec<(String, String)> = case["row"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(key, value)| (key.clone(), value.as_str().unwrap().into()))
            .collect();
        redis::AsyncCommands::hset_multiple::<_, _, _, ()>(&mut redis, &key, &fields)
            .await
            .unwrap();
        let actual = snapshot(&store, &transport, now, ()).await.unwrap();
        redis::AsyncCommands::del::<_, usize>(&mut redis, &key)
            .await
            .unwrap();
        assert_eq!(
            actual, case["expected"],
            "Source realRedis VOD stats case {}",
            case["name"]
        );
        count += 1;
    }
    for (_, (statement, id, value)) in original {
        sqlx::query(statement)
            .bind(value)
            .bind(id)
            .execute(store.pool())
            .await
            .unwrap();
    }
    sqlx::query("DELETE FROM vod_vodlogo WHERE id=$1")
        .bind(logo["id"].as_i64().unwrap())
        .execute(store.pool())
        .await
        .unwrap();
    assert_eq!(
        snapshot(&store, &transport, now, ()).await.unwrap(),
        json!({"vod_connections":[],"total_connections":0,"timestamp":now})
    );
    assert_eq!(count, 19);
    println!(
        "{}",
        json!({"actual_source_vod_stats_cases":count,"native_lifecycle_publication_verified":false})
    );
}
