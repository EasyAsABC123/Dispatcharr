//! Original XC short/simple EPG projection. Dispatcharr authors retain credit.
//! App actions stay guarded until actual original/native HTTP gates pass.
use crate::{epg_hash, numbers, project::JsonBytes};
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{DateTime, Duration, Timelike, Utc};
use dispatcharr_contracts::PlaybackRequest;
use dispatcharr_store::{AuthService, Store, User};
use futures_util::TryStreamExt;
use serde_json::{json, Value};
use sqlx::{Postgres, QueryBuilder, Row};
use std::sync::Arc;
use tokio::sync::Semaphore;

#[derive(Clone)]
pub struct Limits {
    pub max_programmes: usize,
    pub max_category_channels: usize,
    pub max_metadata_bytes: usize,
    pub max_response_bytes: usize,
    pub max_number_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_programmes: 50_000,
            max_category_channels: 50_000,
            max_metadata_bytes: 8 * 1024 * 1024,
            max_response_bytes: 16 * 1024 * 1024,
            max_number_bytes: 128 * 1024 * 1024,
        }
    }
}
#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    NotFound,
    InvalidLimit,
    Capacity,
    Unsupported,
    Backend,
}
impl Error {
    pub fn source_class(&self) -> &'static str {
        match self {
            Self::NotFound => "Http404",
            Self::InvalidLimit => "ValueError",
            Self::Capacity => "Capacity",
            Self::Unsupported => "Unsupported",
            Self::Backend => "DatabaseError",
        }
    }
}
fn backend(_: impl std::fmt::Debug) -> Error {
    Error::Backend
}
fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(v) => *v,
        Value::Number(v) => v.as_f64() != Some(0.0),
        Value::String(v) => !v.is_empty(),
        Value::Array(v) => !v.is_empty(),
        Value::Object(v) => !v.is_empty(),
    }
}
struct Channel {
    id: i64,
    number: Option<f64>,
    name: String,
    group: Option<i64>,
    epg: Option<i64>,
    source_type: Option<String>,
    properties: Option<Value>,
    stored: bool,
    catchup: bool,
    catchup_days: i32,
}
pub struct Context<'a> {
    pub store: &'a Store,
    pub auth: &'a AuthService,
    pub user: &'a User,
    pub request: &'a PlaybackRequest,
    pub short: bool,
    pub now: DateTime<Utc>,
    pub limits: &'a Limits,
    pub number_memory: Arc<Semaphore>,
}
async fn channel(ctx: &Context<'_>, pk: i64) -> Result<Channel, Error> {
    if ctx.user.user_level < 10
        && truthy(&ctx.user.custom_properties)
        && !ctx.user.custom_properties.is_object()
    {
        return Err(Error::Backend);
    }
    let metadata_limit = (ctx.limits.max_metadata_bytes / 128).min(65536).to_string();
    let mut query=QueryBuilder::<Postgres>::new("SELECT c.id::bigint AS id,COALESCE(o.channel_number,c.channel_number) AS number,COALESCE(o.name,c.name) AS name,COALESCE(o.channel_group_id,c.channel_group_id)::bigint AS group_id,COALESCE(o.epg_data_id,c.epg_data_id)::bigint AS epg_id,es.source_type,CASE WHEN es.source_type='dummy' AND octet_length(es.custom_properties::text)<=65536 THEN es.custom_properties END AS properties,(es.source_type='dummy' AND COALESCE(octet_length(es.custom_properties::text),0)>65536) AS large_properties,EXISTS(SELECT 1 FROM epg_programdata p WHERE p.epg_id=COALESCE(o.epg_data_id,c.epg_data_id)) AS stored,c.is_catchup,COALESCE(c.catchup_days,0) AS catchup_days FROM dispatcharr_channels_channel c LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id LEFT JOIN epg_epgdata e ON e.id=COALESCE(o.epg_data_id,c.epg_data_id) LEFT JOIN epg_epgsource es ON es.id=e.epg_source_id WHERE c.id=".replace("65536",&metadata_limit));
    query.push_bind(pk);
    Store::append_xc_epg_visibility(&mut query, ctx.user);
    let row = query
        .build()
        .fetch_optional(ctx.store.pool())
        .await
        .map_err(backend)?
        .ok_or(Error::NotFound)?;
    if row
        .try_get::<Option<bool>, _>("large_properties")
        .map_err(backend)?
        .unwrap_or(false)
    {
        return Err(Error::Capacity);
    }
    Ok(Channel {
        id: row.try_get("id").map_err(backend)?,
        number: row.try_get("number").map_err(backend)?,
        name: row.try_get("name").map_err(backend)?,
        group: row.try_get("group_id").map_err(backend)?,
        epg: row.try_get("epg_id").map_err(backend)?,
        source_type: row.try_get("source_type").map_err(backend)?,
        properties: row.try_get("properties").map_err(backend)?,
        stored: row.try_get("stored").map_err(backend)?,
        catchup: row.try_get("is_catchup").map_err(backend)?,
        catchup_days: row.try_get("catchup_days").map_err(backend)?,
    })
}
async fn category_number(ctx: &Context<'_>, channel: &Channel) -> Result<String, Error> {
    let fallback = || {
        channel.number.map_or(Ok("0".to_owned()), |value| {
            numbers::integer(value).map_err(|_| Error::Backend)
        })
    };
    let Some(group) = channel.group else {
        return fallback();
    };
    let mut rows=sqlx::query("SELECT c.id::bigint AS id,COALESCE(o.channel_number,c.channel_number) AS number FROM dispatcharr_channels_channel c LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id WHERE c.channel_group_id=$1 AND NOT c.hidden_from_output ORDER BY COALESCE(o.channel_number,c.channel_number) LIMIT $2").bind(group).bind((ctx.limits.max_category_channels+1)as i64).fetch(ctx.store.pool());
    let mut values = Vec::new();
    let mut count = 0usize;
    while let Some(row) = rows.try_next().await.map_err(backend)? {
        count += 1;
        if count > ctx.limits.max_category_channels
            || count.saturating_mul(64) > ctx.limits.max_metadata_bytes
        {
            return Err(Error::Capacity);
        }
        let number: Option<f64> = row.try_get("number").map_err(backend)?;
        if number.is_some() {
            values.push((row.try_get("id").map_err(backend)?, number));
        }
    }
    let bytes = numbers::reservation(values.len()).map_err(|_| Error::Capacity)?;
    if bytes > ctx.limits.max_number_bytes {
        return Err(Error::Capacity);
    }
    let _memory = ctx
        .number_memory
        .clone()
        .try_acquire_many_owned(bytes as u32)
        .map_err(|_| Error::Capacity)?;
    let mut assigned = numbers::assign(&values, bytes).map_err(|_| Error::Backend)?;
    assigned.remove(&channel.id).map_or_else(fallback, Ok)
}
fn timestamp(value: DateTime<Utc>) -> i64 {
    // Python int(datetime.timestamp()) truncates the rounded f64, including
    // negative fractional epochs. DateTime.timestamp alone rounds downward.
    (value.timestamp() as f64 + f64::from(value.timestamp_subsec_micros()) / 1_000_000.0).trunc()
        as i64
}
struct Programme<'a> {
    id: String,
    epg: String,
    number: &'a str,
    stream: &'a str,
    title: &'a str,
    description: &'a str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    archive: Option<Duration>,
    now: DateTime<Utc>,
    short: bool,
}
fn append(
    out: &mut JsonBytes,
    p: Programme<'_>,
    count: &mut usize,
    max: usize,
) -> Result<(), Error> {
    if *count >= max {
        return Err(Error::Capacity);
    }
    if *count > 0 {
        out.raw(b", ").map_err(|_| Error::Capacity)?
    }
    let mut row = json!({"id":p.id,"epg_id":p.epg,"title":STANDARD.encode(p.title),"lang":"","start":p.start.format("%Y-%m-%d %H:%M:%S").to_string(),"end":p.end.format("%Y-%m-%d %H:%M:%S").to_string(),"description":STANDARD.encode(p.description),"channel_id":p.number,"start_timestamp":timestamp(p.start).to_string(),"stop_timestamp":timestamp(p.end).to_string(),"stream_id":p.stream,"has_archive":i32::from(p.archive.is_some_and(|window|p.end<p.now&&p.end>p.now-window))});
    if !p.short {
        row["now_playing"] = json!(i32::from(p.start <= p.now && p.now <= p.end));
    }
    out.value(&row).map_err(|_| Error::Capacity)?;
    *count += 1;
    Ok(())
}
pub async fn render(ctx: Context<'_>) -> Result<Vec<u8>, Error> {
    let mut out = JsonBytes::new(ctx.limits.max_response_bytes).map_err(|_| Error::Capacity)?;
    write(&ctx, &mut out).await?;
    Ok(out.finish())
}
pub(crate) async fn write(ctx: &Context<'_>, out: &mut JsonBytes) -> Result<(), Error> {
    let limits = ctx.limits;
    if limits.max_programmes == 0
        || limits.max_programmes > 1_000_000
        || limits.max_category_channels == 0
        || limits.max_category_channels > 50_000
        || limits.max_metadata_bytes < 1024
        || limits.max_metadata_bytes > 32 * 1024 * 1024
        || limits.max_response_bytes == 0
        || limits.max_response_bytes > 64 * 1024 * 1024
        || limits.max_number_bytes > 128 * 1024 * 1024
    {
        return Err(Error::Capacity);
    }
    let raw = ctx
        .request
        .parameter("stream_id")
        .filter(|v| !v.is_empty())
        .ok_or(Error::NotFound)?;
    let pk = dispatcharr_ingest_core::source_integer_literal(raw)
        .and_then(|v| v.parse::<i64>().ok())
        .ok_or(Error::NotFound)?;
    let channel = channel(ctx, pk).await?;
    let number = category_number(ctx, &channel).await?;
    let limit = dispatcharr_ingest_core::source_integer_literal(
        ctx.request.parameter("limit").unwrap_or("4"),
    )
    .ok_or(Error::InvalidLimit)?;
    let policy = dispatcharr_store::source_epg_history_policy(
        &ctx.auth.source_epg_settings().await.map_err(backend)?,
    )
    .map_err(backend)?;
    let window = dispatcharr_output_http::epg_window::resolve(
        &ctx.request.query,
        Some(&ctx.user.custom_properties),
        policy,
    )
    .map_err(backend)?;
    if truthy(&ctx.user.custom_properties) && !ctx.user.custom_properties.is_object() {
        return Err(Error::Backend);
    }
    let allowed = ctx.user.custom_properties.get("catchup_enabled") != Some(&Value::Bool(false))
        && ctx
            .auth
            .system_settings()
            .await
            .map_err(backend)?
            .get("catchup_enabled")
            != Some(&Value::Bool(false));
    let archive =
        (channel.catchup && allowed).then(|| Duration::days(i64::from(channel.catchup_days)));
    let lookback = ctx.now - Duration::days(window.prev_days);
    let cutoff = (window.forward_days > 0).then(|| ctx.now + Duration::days(window.forward_days));
    out.raw(b"{\"epg_listings\": [")
        .map_err(|_| Error::Capacity)?;
    let mut count = 0usize;
    if channel.epg.is_none() || (channel.source_type.as_deref() == Some("dummy") && !channel.stored)
    {
        if channel.properties.as_ref().is_some_and(truthy) {
            return Err(Error::Unsupported);
        }
        let maximum = if ctx.short {
            if limit.starts_with('-') {
                0
            } else {
                limit.parse::<usize>().unwrap_or(usize::MAX)
            }
        } else {
            usize::MAX
        };
        let start = ctx
            .now
            .with_minute(0)
            .and_then(|v| v.with_second(0))
            .and_then(|v| v.with_nanosecond(0))
            .ok_or(Error::Backend)?;
        let dummy = dispatcharr_output_http::epg_dummy::standard(
            raw,
            &channel.name,
            start,
            if window.forward_days > 0 {
                window.forward_days as u32
            } else {
                3
            },
            Some(if ctx.short { ctx.now } else { lookback }),
            cutoff,
        )
        .map_err(|_| Error::Capacity)?;
        for p in dummy.take(maximum) {
            append(
                out,
                Programme {
                    id: epg_hash::synthetic_id(raw, timestamp(p.start_time)).map_err(|error| {
                        match error {
                            epg_hash::Error::Capacity => Error::Capacity,
                            epg_hash::Error::UnsupportedSeed => Error::Unsupported,
                        }
                    })?,
                    epg: channel.epg.unwrap_or(0).to_string(),
                    number: &number,
                    stream: raw,
                    title: &p.title,
                    description: &p.description,
                    start: p.start_time,
                    end: p.end_time,
                    archive,
                    now: ctx.now,
                    short: ctx.short,
                },
                &mut count,
                limits.max_programmes,
            )?;
        }
    } else {
        let requested = if ctx.short {
            let value = limit.parse::<i64>().map_err(|_| Error::InvalidLimit)?;
            if value < 0 {
                return Err(Error::InvalidLimit);
            }
            value as usize
        } else {
            limits.max_programmes + 1
        };
        let maximum = requested.min(limits.max_programmes + 1);
        let row_bytes = (limits.max_metadata_bytes / 4) as i64;
        let mut rows=sqlx::query("SELECT id::bigint AS id,start_time,end_time,CASE WHEN octet_length(title)<=$4 THEN title END AS title,CASE WHEN octet_length(description)<=$4 THEN description END AS description,(COALESCE(octet_length(title),0)>$4 OR COALESCE(octet_length(description),0)>$4) AS large_metadata FROM epg_programdata WHERE epg_id=$1 AND end_time>$2 AND ($3::timestamptz IS NULL OR start_time<$3) ORDER BY start_time LIMIT $5").bind(channel.epg).bind(if ctx.short{ctx.now}else{lookback}).bind(if ctx.short{None}else{cutoff}).bind(row_bytes).bind(maximum as i64).fetch(ctx.store.pool());
        while let Some(row) = rows.try_next().await.map_err(backend)? {
            if row.try_get::<bool, _>("large_metadata").map_err(backend)? {
                return Err(Error::Capacity);
            }
            let id: i64 = row.try_get("id").map_err(backend)?;
            let title: Option<String> = row.try_get("title").map_err(backend)?;
            let description: Option<String> = row.try_get("description").map_err(backend)?;
            append(
                out,
                Programme {
                    id: id.to_string(),
                    epg: channel.epg.unwrap_or(0).to_string(),
                    number: &number,
                    stream: raw,
                    title: title.as_deref().unwrap_or(""),
                    description: description.as_deref().unwrap_or(""),
                    start: row.try_get("start_time").map_err(backend)?,
                    end: row.try_get("end_time").map_err(backend)?,
                    archive,
                    now: ctx.now,
                    short: ctx.short,
                },
                &mut count,
                limits.max_programmes,
            )?;
        }
    }
    out.raw(b"]}").map_err(|_| Error::Capacity)
}

#[cfg(test)]
mod tests {
    use super::*;
    use dispatcharr_store::{AuthConfig, NotificationCache};
    #[tokio::test]
    #[ignore = "requires actual original-schema PostgreSQL/Redis and original XC EPG corpus"]
    async fn actual_original_seven_hundred_fifty_epg_cases() {
        let database = std::env::var("DATABASE_TEST_URL").expect("owned fixture");
        let redis = std::env::var("REDIS_TEST_URL").expect("owned fixture");
        assert!(
            database.contains("playback-fixture-only") && redis.contains("playback-fixture-only")
        );
        let store = Store::connect(&database, 2).await.unwrap();
        let auth = AuthService::new(
            store.clone(),
            AuthConfig::new("playback-fixture-only".into()).unwrap(),
        )
        .with_notification_cache(NotificationCache::connect(&redis).await.unwrap());
        let corpus: Value = serde_json::from_slice(
            &std::fs::read(std::env::var("XC_EPG_SOURCE").expect("actual original helper corpus"))
                .unwrap(),
        )
        .unwrap();
        let cases = corpus["cases"].as_array().unwrap();
        assert_eq!(cases.len(), 750);
        let limits = Limits::default();
        let number_memory = Arc::new(Semaphore::new(128 * 1024 * 1024));
        for (index, case) in cases.iter().enumerate() {
            let user = store
                .user_by_id(case["user_id"].as_i64().unwrap())
                .await
                .unwrap()
                .unwrap();
            let request = PlaybackRequest {
                channel_id: String::new(),
                peer_ip: "127.0.0.1".parse().unwrap(),
                headers: Vec::new(),
                query: case["params"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|pair| {
                        (
                            pair[0].as_str().unwrap().to_owned(),
                            pair[1].as_str().unwrap().to_owned(),
                        )
                    })
                    .collect(),
            };
            let result = render(Context {
                store: &store,
                auth: &auth,
                user: &user,
                request: &request,
                short: case["short"].as_bool().unwrap(),
                now: case["now"].as_str().unwrap().parse().unwrap(),
                limits: &limits,
                number_memory: number_memory.clone(),
            })
            .await;
            if let Some(error) = case.get("error") {
                assert_eq!(
                    result.unwrap_err().source_class(),
                    error.as_str().unwrap(),
                    "case {index}"
                );
                continue;
            }
            let bytes = result.unwrap_or_else(|error| panic!("case {index} returned {error:?}"));
            let synthetic = case["synthetic_indices"].as_array().unwrap();
            if synthetic.is_empty() {
                assert_eq!(
                    std::str::from_utf8(&bytes).unwrap(),
                    case["comparison_json"].as_str().unwrap(),
                    "real-row bytes case {index}"
                );
            } else {
                let mut value: Value = serde_json::from_slice(&bytes).unwrap();
                for at in synthetic {
                    let row = &mut value["epg_listings"][at.as_u64().unwrap() as usize];
                    let id = row["id"].as_str().unwrap().parse::<u64>().unwrap();
                    assert!(id <= 1u64 << 63);
                    row["id"] = json!("<process-random-synthetic>");
                }
                let mut normalized = JsonBytes::new(limits.max_response_bytes).unwrap();
                normalized.value(&value).unwrap();
                assert_eq!(
                    std::str::from_utf8(&normalized.finish()).unwrap(),
                    case["comparison_json"].as_str().unwrap(),
                    "synthetic-ID normalized bytes case {index}"
                );
            }
            assert_eq!(number_memory.available_permits(), 128 * 1024 * 1024);
        }
    }

    #[tokio::test]
    #[ignore = "requires actual original-schema PostgreSQL/Redis and original XC EPG corpus"]
    async fn real_epg_caps_reject_instead_of_silently_truncating() {
        let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 2)
            .await
            .unwrap();
        let redis = std::env::var("REDIS_TEST_URL").unwrap();
        assert!(redis.contains("playback-fixture-only"));
        let auth = AuthService::new(
            store.clone(),
            AuthConfig::new("playback-fixture-only".into()).unwrap(),
        )
        .with_notification_cache(NotificationCache::connect(&redis).await.unwrap());
        let source: Value = serde_json::from_slice(
            &std::fs::read(std::env::var("XC_EPG_SOURCE").unwrap()).unwrap(),
        )
        .unwrap();
        let case = source["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| {
                c["role"] == "standard"
                    && c["error"].is_null()
                    && c["short"] == true
                    && c["synthetic_indices"]
                        .as_array()
                        .is_some_and(|v| v.is_empty())
                    && c["expected"]["epg_listings"]
                        .as_array()
                        .is_some_and(|v| v.len() > 1)
            })
            .unwrap();
        let user = store
            .user_by_id(case["user_id"].as_i64().unwrap())
            .await
            .unwrap()
            .unwrap();
        let request = PlaybackRequest {
            channel_id: String::new(),
            peer_ip: "127.0.0.1".parse().unwrap(),
            headers: Vec::new(),
            query: case["params"]
                .as_array()
                .unwrap()
                .iter()
                .map(|pair| {
                    (
                        pair[0].as_str().unwrap().into(),
                        pair[1].as_str().unwrap().into(),
                    )
                })
                .collect(),
        };
        let now = case["now"].as_str().unwrap().parse().unwrap();
        let memory = Arc::new(Semaphore::new(128 * 1024 * 1024));
        let limited = Limits {
            max_programmes: 1,
            ..Limits::default()
        };
        let error = render(Context {
            store: &store,
            auth: &auth,
            user: &user,
            request: &request,
            short: false,
            now,
            limits: &limited,
            number_memory: memory.clone(),
        })
        .await
        .unwrap_err();
        assert_eq!(error, Error::Capacity);
        let bytes = Limits {
            max_response_bytes: 64,
            ..Limits::default()
        };
        assert_eq!(
            render(Context {
                store: &store,
                auth: &auth,
                user: &user,
                request: &request,
                short: true,
                now,
                limits: &bytes,
                number_memory: memory.clone()
            })
            .await
            .unwrap_err(),
            Error::Capacity
        );
        let numbers = Limits {
            max_number_bytes: 8192,
            ..Limits::default()
        };
        assert_eq!(
            render(Context {
                store: &store,
                auth: &auth,
                user: &user,
                request: &request,
                short: true,
                now,
                limits: &numbers,
                number_memory: memory.clone()
            })
            .await
            .unwrap_err(),
            Error::Capacity
        );
        let pk = case["expected"]["epg_listings"][0]["id"]
            .as_str()
            .unwrap()
            .parse::<i64>()
            .unwrap();
        let previous: Option<String> =
            sqlx::query_scalar("SELECT description FROM epg_programdata WHERE id=$1")
                .bind(pk)
                .fetch_one(store.pool())
                .await
                .unwrap();
        sqlx::query("UPDATE epg_programdata SET description=$1 WHERE id=$2")
            .bind("bounded".repeat(200))
            .bind(pk)
            .execute(store.pool())
            .await
            .unwrap();
        let row = Limits {
            max_metadata_bytes: 4096,
            ..Limits::default()
        };
        let result = render(Context {
            store: &store,
            auth: &auth,
            user: &user,
            request: &request,
            short: true,
            now,
            limits: &row,
            number_memory: memory.clone(),
        })
        .await;
        sqlx::query("UPDATE epg_programdata SET description=$1 WHERE id=$2")
            .bind(previous)
            .bind(pk)
            .execute(store.pool())
            .await
            .unwrap();
        assert_eq!(result.unwrap_err(), Error::Capacity);
        assert_eq!(memory.available_permits(), 128 * 1024 * 1024);
    }
}
