//! Original Dispatcharr guide-grid helper primitives; original authors credited.
//! Component only: no guide HTTP, window parser, SQL visibility or regex claim.
use crate::{channels::truthy, username::lower_source};
use chrono::{DateTime, Datelike, Duration, SecondsFormat, Timelike, Utc};
use num_bigint::BigInt;
use serde_json::Value;
use std::{io::Write, str::FromStr};

pub fn grid_channel_profile(params: &[(String, String)]) -> Result<Option<String>, String> {
    let Some((_, raw)) = params
        .iter()
        .rev()
        .find(|(key, _)| key == "channel_profile_id")
    else {
        return Ok(None);
    };
    if raw.is_empty() || lower_source(raw) == "all" {
        return Ok(None);
    }
    let value = dispatcharr_ingest_core::source_integer_literal(raw)
        .and_then(|v| BigInt::from_str(&v).ok())
        .ok_or_else(|| format!("Invalid integer for channel_profile_id: {raw}."))?;
    if value < BigInt::from(1) {
        return Err(format!("Invalid channel_profile_id: {raw}."));
    }
    Ok(Some(value.to_string()))
}
fn hour(value: DateTime<Utc>) -> DateTime<Utc> {
    value
        .with_minute(0)
        .unwrap()
        .with_second(0)
        .unwrap()
        .with_nanosecond(0)
        .unwrap()
}
fn days(span: Duration) -> Result<i64, String> {
    // Match timedelta.total_seconds()/86400 followed by Python math.ceil.
    let seconds = span.num_microseconds().ok_or("date value out of range")? as f64 / 1_000_000.0;
    Ok(if seconds <= 0.0 {
        1
    } else {
        (seconds / 86400.0).ceil().max(1.0) as i64
    })
}
pub fn grid_dummy_span(
    now: DateTime<Utc>,
    lookback: DateTime<Utc>,
    cutoff: DateTime<Utc>,
) -> Result<(DateTime<Utc>, i64), String> {
    let truncated = hour(now);
    let default = now - Duration::hours(1);
    let base = if lookback >= truncated {
        hour(lookback)
    } else if lookback < default {
        truncated
            .checked_sub_signed(Duration::days(days(truncated - lookback)?))
            .filter(|v| (1..=9999).contains(&v.year()))
            .ok_or("date value out of range")?
    } else {
        truncated
    };
    let span = if lookback >= truncated || lookback < default {
        cutoff - base
    } else {
        cutoff - now
    };
    Ok((base, days(span)?))
}
pub fn grid_program_value(row: &Value) -> Result<Value, String> {
    let mut value = serde_json::Map::new();
    for name in [
        "id",
        "start_time",
        "end_time",
        "title",
        "sub_title",
        "description",
        "tvg_id",
    ] {
        value.insert(
            name.into(),
            row.get(name)
                .ok_or_else(|| format!("Missing grid field: {name}"))?
                .clone(),
        );
    }
    for name in ["season", "episode"] {
        value.insert(name.into(), row.get(name).cloned().unwrap_or(Value::Null));
    }
    for (name, source) in [
        ("is_new", "flag_new"),
        ("is_live", "flag_live"),
        ("is_premiere", "flag_premiere"),
    ] {
        value.insert(name.into(), truthy(row.get(source)).into());
    }
    // Source grid uses str(nonstring), unlike the programme detail serializer.
    // JSON's spelling has the same literal 'finale' substrings for JSON values;
    // preserve direct strings and use Source Unicode lower for the comparison.
    let premiere = row
        .get("premiere_text")
        .filter(|v| !v.is_null())
        .map(|v| {
            v.as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| v.to_string())
        })
        .unwrap_or_default();
    value.insert(
        "is_finale".into(),
        lower_source(&premiere).contains("finale").into(),
    );
    Ok(Value::Object(value))
}
fn json_program(row: &Value) -> Result<Value, String> {
    let mut row = row.clone();
    for name in ["start_time", "end_time"] {
        if let Some(raw) = row
            .get(name)
            .and_then(|v| v.get("datetime"))
            .and_then(Value::as_str)
        {
            let time =
                DateTime::parse_from_rfc3339(raw).map_err(|_| "Invalid tagged grid datetime")?;
            let format = if time.nanosecond() == 0 {
                SecondsFormat::Secs
            } else {
                SecondsFormat::Millis
            };
            let time = time
                .with_nanosecond(time.nanosecond() / 1_000_000 * 1_000_000)
                .ok_or("Invalid grid timestamp")?;
            row[name] = time.to_rfc3339_opts(format, true).into();
        }
    }
    Ok(row)
}
/// Encode at most500 grid programmes under an explicit frame byte bound. This
/// preserves original batch entities; callers separately own queue/body leases.
pub fn grid_encode_batch(
    programs: &[Value],
    first: bool,
    max_bytes: usize,
) -> Result<(Vec<u8>, bool), String> {
    if programs.len() > 500 || max_bytes == 0 || max_bytes > 16 * 1024 * 1024 {
        return Err("Grid batch capacity".into());
    }
    if programs.is_empty() {
        return Ok((Vec::new(), first));
    }
    let mut out = crate::epg_program_http::BoundedJson::new(max_bytes);
    if !first {
        out.write_all(b",").map_err(|_| "Grid batch capacity")?;
    }
    for (index, row) in programs.iter().enumerate() {
        if index != 0 {
            out.write_all(b",").map_err(|_| "Grid batch capacity")?;
        }
        let row = json_program(row)?;
        let encoded =
            crate::json_compat::compact_json_bytes(&row).map_err(|_| "Invalid grid JSON")?;
        out.write_all(&encoded).map_err(|_| "Grid batch capacity")?;
    }
    Ok((out.into_data(), false))
}
#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use serde_json::json;
    use sha2::{Digest, Sha256};
    fn date(value: &Value) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value.as_str().unwrap())
            .unwrap()
            .with_timezone(&Utc)
    }
    fn tagged(value: DateTime<Utc>) -> Value {
        json!({"datetime":value.to_rfc3339_opts(if value.nanosecond()==0 {SecondsFormat::Secs} else {SecondsFormat::Micros},false)})
    }
    #[test]
    fn actual_original_grid_profile_span_row_encoding_and_batch_boundaries() {
        let corpus: Value = serde_json::from_str(include_str!(
            "../datasets/source-grid-helper-components.json"
        ))
        .unwrap();
        let mut compared = 0;
        for case in corpus["cases"].as_array().unwrap() {
            let input = &case["inputs"];
            let expected = &case["expected"];
            let actual = match case["kind"].as_str().unwrap() {
                "channel_profile" => {
                    let params = if input["missing"] == true {
                        vec![]
                    } else {
                        vec![(
                            "channel_profile_id".into(),
                            input["raw"].as_str().unwrap().into(),
                        )]
                    };
                    match grid_channel_profile(&params) {
                        Ok(None) => json!({"value":null}),
                        Ok(Some(id)) => {
                            json!({"value":serde_json::from_str::<Value>(&id).unwrap()})
                        }
                        Err(message) => json!({"error":"GridWindowError","message":message}),
                    }
                }
                "dummy_span" => {
                    let (start, days) = grid_dummy_span(
                        date(&input["now"]),
                        date(&input["start"]),
                        date(&input["end"]),
                    )
                    .unwrap();
                    json!({"value":[tagged(start),days]})
                }
                "stored_grid_row" => json!({"value":grid_program_value(&input["row"]).unwrap()}),
                "grid_batch" => {
                    let (bytes, first) = grid_encode_batch(
                        input["programs"].as_array().unwrap(),
                        input["first"].as_bool().unwrap(),
                        16 * 1024 * 1024,
                    )
                    .unwrap();
                    json!({"value":[{"bytes_base64":base64::engine::general_purpose::STANDARD.encode(bytes)},first]})
                }
                "grid_chunk_boundaries" => {
                    let count = input["count"].as_u64().unwrap() as usize;
                    let rows = vec![input["sample"].clone(); count];
                    let mut chunks = vec![b"{\"data\":[".to_vec()];
                    let mut first = true;
                    for batch in rows.chunks(500) {
                        let (bytes, next) =
                            grid_encode_batch(batch, first, 16 * 1024 * 1024).unwrap();
                        first = next;
                        if !bytes.is_empty() {
                            chunks.push(bytes)
                        }
                    }
                    chunks.push(b"]}".to_vec());
                    let body = chunks.concat();
                    json!({"body_sha256":format!("{:x}",Sha256::digest(&body)),"body_bytes":body.len(),"chunks":chunks.iter().map(|chunk|json!({"bytes":chunk.len(),"sha256":format!("{:x}",Sha256::digest(chunk))})).collect::<Vec<_>>(),"decoded_programs":count})
                }
                _ => panic!("unexpected helper family"),
            };
            assert_eq!(
                &actual, expected,
                "case{compared} kind{} inputs{}",
                case["kind"], input
            );
            compared += 1;
        }
        assert_eq!(compared, 304);
    }
    #[test]
    fn frame_capacity_rejects_large_escaped_values_and_count_overflow() {
        let row = json!({"title":"\0".repeat(100)});
        assert!(grid_encode_batch(std::slice::from_ref(&row), true, 128).is_err());
        assert!(grid_encode_batch(&vec![row; 501], true, 16 * 1024 * 1024).is_err());
    }
}
