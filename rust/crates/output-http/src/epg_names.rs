//! Source resolve_pattern_match_name over actual channelstream join ordering.
//! Original Dispatcharr authors retain all application/algorithm credit.
use dispatcharr_store::{Store, StoreError};
use futures_util::TryStreamExt;
use serde_json::{json, Value};
use sqlx::Row;
fn unsupported() -> StoreError {
    StoreError::StatusBody(
        501,
        json!({"detail":"Source stream-name index type is unsupported."}),
    )
}
fn capacity() -> StoreError {
    StoreError::StatusBody(
        503,
        json!({"detail":"EPG stream-name read capacity unavailable."}),
    )
}
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum StreamIndex {
    Integer(i64),
    Float(f64),
}
/// Source subtracts before testing bounds, and indexes the Python list only
/// inside bounds. A float OUTSIDE bounds genuinely falls back; an in-bounds
/// float raises TypeError. The SQL snapshot supplies that actual length.
pub fn stream_offset(properties: Option<&Value>) -> Result<Option<StreamIndex>, StoreError> {
    let Some(properties) = properties else {
        return Ok(None);
    };
    if properties.get("name_source").and_then(Value::as_str) != Some("stream") {
        return Ok(None);
    }
    let Some(value) = properties.get("stream_index") else {
        return Ok(Some(StreamIndex::Integer(0)));
    };
    let offset = match value {
        Value::Bool(_) => StreamIndex::Integer(0),
        Value::Number(value) => {
            let text = value.to_string();
            if text.contains(['.', 'e', 'E']) {
                let offset = value.as_f64().ok_or_else(unsupported)? - 1.0;
                if !offset.is_finite() || offset < 0.0 {
                    StreamIndex::Integer(0)
                } else {
                    StreamIndex::Float(offset)
                }
            } else if let Some(value) = value.as_i64() {
                StreamIndex::Integer(if value > 0 { value - 1 } else { 0 })
            } else if let Some(value) = value.as_u64() {
                StreamIndex::Integer(
                    value
                        .checked_sub(1)
                        .and_then(|v| i64::try_from(v).ok())
                        .unwrap_or(0),
                )
            } else {
                StreamIndex::Integer(0)
            }
        }
        _ => return Err(unsupported()),
    };
    Ok(Some(offset))
}
/// One query sees every selected name at the SAME PostgreSQL snapshot, like
/// Source's one ordered prefetch. A LATERAL lookup transfers at most one name
/// per requested channel instead of allocating every Stream model. No tie id
/// is invented: both lookups order ONLY by actual ChannelStream.order.
/// Returns aligned optional parse names; None means keep the display name.
pub async fn resolve(
    store: &Store,
    inputs: &[(i64, Option<&Value>)],
    budget: usize,
) -> Result<Vec<Option<String>>, StoreError> {
    let base = inputs
        .len()
        .checked_mul(192)
        .filter(|n| *n <= budget)
        .ok_or_else(capacity)?;
    let mut output = vec![None; inputs.len()];
    let mut positions = Vec::new();
    let mut ids = Vec::new();
    let mut offsets = Vec::new();
    let mut floating = Vec::new();
    let mut float_flags = Vec::new();
    for (index, (id, properties)) in inputs.iter().enumerate() {
        if let Some(offset) = stream_offset(*properties)? {
            positions.push(index);
            ids.push(*id);
            let (integer, float, flag) = match offset {
                StreamIndex::Integer(value) => (value, 0.0, false),
                StreamIndex::Float(value) => (0, value, true),
            };
            offsets.push(integer);
            floating.push(float);
            float_flags.push(flag);
        }
    }
    if ids.is_empty() {
        return Ok(output);
    }
    let mut rows=sqlx::query("WITH desired AS (SELECT id,offset_value,float_value,is_float,ordinal FROM unnest($1::bigint[],$2::bigint[],$3::double precision[],$4::boolean[]) WITH ORDINALITY AS u(id,offset_value,float_value,is_float,ordinal)) SELECT desired.ordinal::bigint AS ordinal,CASE WHEN octet_length(COALESCE(selected.name,first_stream.name))<=65536 THEN COALESCE(selected.name,first_stream.name) END AS name,COALESCE(octet_length(COALESCE(selected.name,first_stream.name))>65536,false) AS oversized,(desired.is_float AND desired.float_value>=0 AND desired.float_value<count_streams.total) AS invalid_list_index FROM desired LEFT JOIN LATERAL (SELECT s.name FROM dispatcharr_channels_channelstream cs JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id WHERE cs.channel_id=desired.id ORDER BY cs.\"order\" LIMIT 1 OFFSET desired.offset_value) selected ON true LEFT JOIN LATERAL (SELECT s.name FROM dispatcharr_channels_channelstream cs JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id WHERE cs.channel_id=desired.id ORDER BY cs.\"order\" LIMIT 1) first_stream ON true LEFT JOIN LATERAL (SELECT count(*) AS total FROM dispatcharr_channels_channelstream cs JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id WHERE desired.is_float AND cs.channel_id=desired.id) count_streams ON true").bind(&ids).bind(&offsets).bind(&floating).bind(&float_flags).fetch(store.pool());
    let mut bytes = base;
    let mut seen = vec![false; ids.len()];
    let mut count = 0usize;
    while let Some(row) = rows.try_next().await? {
        if row.try_get::<bool, _>("invalid_list_index")? {
            return Err(unsupported());
        }
        if row.try_get::<bool, _>("oversized")? {
            return Err(capacity());
        }
        let ordinal: i64 = row.try_get("ordinal")?;
        let ordinal = ordinal
            .checked_sub(1)
            .and_then(|v| usize::try_from(v).ok())
            .filter(|v| *v < positions.len())
            .ok_or_else(capacity)?;
        if seen[ordinal] {
            return Err(capacity());
        }
        seen[ordinal] = true;
        count += 1;
        let name: Option<String> = row.try_get("name")?;
        if let Some(name) = &name {
            bytes = bytes
                .checked_add(name.capacity())
                .filter(|n| *n <= budget)
                .ok_or_else(capacity)?;
        }
        output[positions[ordinal]] = name;
    }
    if count != positions.len() {
        return Err(capacity());
    }
    Ok(output)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_index_subtraction_and_list_type_boundary_are_not_string_coercion() {
        for (index, expected) in [
            (json!(true), 0),
            (json!(false), 0),
            (json!(-2), 0),
            (json!(0), 0),
            (json!(1), 0),
            (json!(2), 1),
            (json!(9223372036854775808u64), i64::MAX),
            (json!(18446744073709551615u64), 0),
        ] {
            assert_eq!(
                stream_offset(Some(&json!({"name_source":"stream","stream_index":index}))).unwrap(),
                Some(StreamIndex::Integer(expected))
            );
        }
        for index in [Value::Null, json!("2"), json!([]), json!({})] {
            assert!(matches!(
                stream_offset(Some(&json!({"name_source":"stream","stream_index":index}))),
                Err(StoreError::StatusBody(501, _))
            ));
        }
        assert_eq!(
            stream_offset(Some(&json!({"name_source":"stream","stream_index":1.5}))).unwrap(),
            Some(StreamIndex::Float(0.5))
        );
        assert_eq!(
            stream_offset(Some(&json!({"name_source":"stream","stream_index":0.5}))).unwrap(),
            Some(StreamIndex::Integer(0))
        );
        assert_eq!(
            stream_offset(Some(&json!({"name_source":"stream"}))).unwrap(),
            Some(StreamIndex::Integer(0))
        );
        assert_eq!(
            stream_offset(Some(
                &json!({"name_source":"channel","stream_index":"ignored"})
            ))
            .unwrap(),
            None
        );
    }
}
