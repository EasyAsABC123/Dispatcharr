//! Bounded original XMLTV channel/guide SQL inputs; original author credit.
use chrono::{DateTime, Utc};
use dispatcharr_ingest_core::XmltvProgramme;
use dispatcharr_store::{Store, StoreError, User};
use futures_util::{Stream, TryStreamExt};
use serde_json::{json, Value};
use sqlx::{Postgres, QueryBuilder, Row};
use std::pin::Pin;
#[derive(Clone)]
pub struct Limits {
    pub max_channels: usize,
    pub max_metadata_bytes: usize,
    pub max_programme_metadata_bytes: usize,
    pub max_programmes: usize,
}
impl Limits {
    pub fn validate(&self) -> Result<(), StoreError> {
        if self.max_channels == 0
            || self.max_channels > 100_000
            || self.max_programmes == 0
            || self.max_programmes > 10_000_000
            || self.max_metadata_bytes > 256 * 1024 * 1024
            || self.max_programme_metadata_bytes == 0
            || self.max_programme_metadata_bytes > 1024 * 1024
            || self.max_metadata_bytes
                < self
                    .max_channels
                    .saturating_mul(std::mem::size_of::<Channel>())
        {
            return Err(capacity());
        }
        Ok(())
    }
}
// Charge containers conservatively, including IndexMap buckets and unused vector
// capacity. Encoded JSON length alone misses arrays of many tiny scalar nodes.
fn json_memory(value: &Value) -> Result<usize, StoreError> {
    let mut pending = vec![(value, 0usize)];
    let mut total = 0usize;
    let mut nodes = 0usize;
    while let Some((value, depth)) = pending.pop() {
        nodes += 1;
        if nodes > 10_000 || depth > 32 {
            return Err(capacity());
        }
        total = total.checked_add(128).ok_or_else(capacity)?;
        match value {
            Value::String(text) => {
                total = total.checked_add(text.capacity()).ok_or_else(capacity)?
            }
            Value::Number(number) => {
                total = total
                    .checked_add(number.to_string().len())
                    .ok_or_else(capacity)?
            }
            Value::Array(values) => {
                if values.len() > 10_000usize.saturating_sub(nodes + pending.len()) {
                    return Err(capacity());
                }
                total = total
                    .checked_add(
                        values
                            .capacity()
                            .saturating_mul(std::mem::size_of::<Value>()),
                    )
                    .ok_or_else(capacity)?;
                pending.extend(values.iter().map(|value| (value, depth + 1)));
            }
            Value::Object(values) => {
                if values.len() > 10_000usize.saturating_sub(nodes + pending.len()) {
                    return Err(capacity());
                }
                for (key, value) in values {
                    total = total.checked_add(key.capacity()).ok_or_else(capacity)?;
                    pending.push((value, depth + 1));
                }
            }
            _ => {}
        }
    }
    Ok(total)
}
pub struct Channel {
    pub id: i64,
    pub number: Option<f64>,
    pub name: String,
    pub logo: Option<(i64, String)>,
    pub tvg_id: Option<String>,
    pub station: Option<String>,
    pub epg_id: Option<i64>,
    pub source_type: Option<String>,
    pub source_properties: Option<Value>,
}
pub enum Channels {
    Rows(Vec<Channel>),
    MissingProfile,
}
pub fn requires_custom_adapter(channel: &Channel) -> bool {
    channel
        .source_properties
        .as_ref()
        .is_some_and(|value| match value {
            Value::Object(properties) => !properties.is_empty(),
            Value::Null => false,
            _ => true,
        })
}
fn capacity() -> StoreError {
    StoreError::StatusBody(
        503,
        json!({"detail":"XMLTV resource capacity unavailable."}),
    )
}
pub async fn channels(
    store: &Store,
    user: Option<&User>,
    profile: Option<&str>,
    limits: &Limits,
) -> Result<Channels, StoreError> {
    limits.validate()?;
    let profile = if user.is_none() {
        if let Some(profile) = profile {
            let id = sqlx::query_scalar::<_, i64>(
                "SELECT id::bigint FROM dispatcharr_channels_channelprofile WHERE name=$1",
            )
            .bind(profile)
            .fetch_optional(store.pool())
            .await?;
            match id {
                Some(id) => Some(id),
                None => return Ok(Channels::MissingProfile),
            }
        } else {
            None
        }
    } else {
        None
    };
    let sql="SELECT c.id::bigint AS id,COALESCE(o.channel_number,c.channel_number) AS number,COALESCE(o.name,c.name) AS name,COALESCE(o.tvg_id,c.tvg_id) AS tvg_id,COALESCE(o.tvc_guide_stationid,c.tvc_guide_stationid) AS station,l.id::bigint AS logo_id,l.url AS logo_url,COALESCE(o.epg_data_id,c.epg_data_id)::bigint AS epg_id,es.source_type,CASE WHEN octet_length(es.custom_properties::text)<=$1 THEN es.custom_properties END AS source_properties,COALESCE(octet_length(es.custom_properties::text)>$1,false) AS large_metadata FROM dispatcharr_channels_channel c LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id LEFT JOIN dispatcharr_channels_logo l ON l.id=COALESCE(o.logo_id,c.logo_id) LEFT JOIN epg_epgdata e ON e.id=COALESCE(o.epg_data_id,c.epg_data_id) LEFT JOIN epg_epgsource es ON es.id=e.epg_source_id WHERE NOT c.hidden_from_output".replace("$1",&limits.max_programme_metadata_bytes.min(65536).to_string());
    let mut query = QueryBuilder::<Postgres>::new(sql);
    dispatcharr_output_http::reader::append_output_visibility(&mut query, user, profile);
    query
        .push(" ORDER BY COALESCE(o.channel_number,c.channel_number) LIMIT ")
        .push_bind((limits.max_channels.saturating_add(1)) as i64);
    let built = query.build();
    let mut rows = built.fetch(store.pool());
    let mut result = Vec::new();
    let mut size = 0usize;
    while let Some(row) = rows.try_next().await? {
        if result.len() >= limits.max_channels || row.try_get::<bool, _>("large_metadata")? {
            return Err(capacity());
        }
        let name: String = row.try_get("name")?;
        let tvg_id: Option<String> = row.try_get("tvg_id")?;
        let station: Option<String> = row.try_get("station")?;
        let source_type: Option<String> = row.try_get("source_type")?;
        let properties: Option<Value> = row.try_get("source_properties")?;
        let logo_id: Option<i64> = row.try_get("logo_id")?;
        let logo = if let Some(id) = logo_id {
            Some((id, row.try_get::<String, _>("logo_url")?))
        } else {
            None
        };
        size = size
            .checked_add(
                name.capacity()
                    + tvg_id.as_ref().map_or(0, String::capacity)
                    + station.as_ref().map_or(0, String::capacity)
                    + source_type.as_ref().map_or(0, String::capacity)
                    + logo.as_ref().map_or(0, |(_, value)| value.capacity()),
            )
            .ok_or_else(capacity)?;
        if let Some(value) = &properties {
            size = size.checked_add(json_memory(value)?).ok_or_else(capacity)?;
        }
        if result.len() == result.capacity() {
            let next_capacity = result
                .capacity()
                .max(4)
                .saturating_mul(2)
                .min(limits.max_channels);
            if size
                .checked_add(next_capacity.saturating_mul(std::mem::size_of::<Channel>()))
                .is_none_or(|n| n > limits.max_metadata_bytes)
            {
                return Err(capacity());
            }
            result
                .try_reserve_exact(next_capacity - result.len())
                .map_err(|_| capacity())?;
        }
        if size
            .checked_add(
                result
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Channel>()),
            )
            .is_none_or(|n| n > limits.max_metadata_bytes)
        {
            return Err(capacity());
        }
        result.push(Channel {
            id: row.try_get("id")?,
            number: row.try_get("number")?,
            name,
            logo,
            tvg_id,
            station,
            epg_id: row.try_get("epg_id")?,
            source_type,
            source_properties: properties,
        });
    }
    Ok(Channels::Rows(result))
}
pub struct ProgrammeRow {
    pub id: i64,
    pub row: XmltvProgramme,
}
pub fn programmes<'a>(
    store: &'a Store,
    epg_ids: Vec<i64>,
    lookback: DateTime<Utc>,
    cutoff: Option<DateTime<Utc>>,
    limits: &'a Limits,
) -> Pin<Box<dyn Stream<Item = Result<ProgrammeRow, StoreError>> + Send + 'a>> {
    Box::pin(async_stream::try_stream! {
        limits.validate()?;
        if epg_ids.len()>limits.max_channels { Err(capacity())?; }
        let mut rows=sqlx::query("SELECT id::bigint AS id,epg_id::bigint AS epg_id,start_time,end_time,title,CASE WHEN octet_length(sub_title)<=$4 THEN sub_title END AS sub_title,CASE WHEN octet_length(description)<=$4 THEN description END AS description,CASE WHEN octet_length(custom_properties::text)<=$4 THEN custom_properties END AS custom_properties,(COALESCE(octet_length(sub_title),0)>$4 OR COALESCE(octet_length(description),0)>$4 OR COALESCE(octet_length(custom_properties::text),0)>$4) AS large_metadata FROM epg_programdata WHERE epg_id=ANY($1) AND end_time>=$2 AND ($3::timestamptz IS NULL OR start_time<$3) ORDER BY epg_id,start_time,id LIMIT $5").bind(epg_ids).bind(lookback).bind(cutoff).bind(limits.max_programme_metadata_bytes as i64).bind(limits.max_programmes.saturating_add(1)as i64).fetch(store.pool());
        let mut count=0usize;
        while let Some(row)=rows.try_next().await?{count+=1;if count>limits.max_programmes||row.try_get::<bool,_>("large_metadata")?{Err(capacity())?}
            yield ProgrammeRow{id:row.try_get("id")?,row:XmltvProgramme{epg_id:row.try_get("epg_id")?,start_time:row.try_get("start_time")?,end_time:row.try_get("end_time")?,title:row.try_get("title")?,sub_title:row.try_get("sub_title")?,description:row.try_get("description")?,tvg_id:String::new(),custom_properties:row.try_get("custom_properties")?}};
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tiny_json_nodes_and_container_capacity_are_charged() {
        let values = Value::Array(vec![Value::Null; 1000]);
        assert!(json_memory(&values).unwrap() > 100_000);
        assert!(json_memory(&Value::Array(vec![Value::Null; 10_001])).is_err());
        let mut deep = Value::Null;
        for _ in 0..34 {
            deep = Value::Array(vec![deep]);
        }
        assert!(json_memory(&deep).is_err());
    }
}
