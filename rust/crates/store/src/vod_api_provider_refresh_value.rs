//! Source-derived provider refresh value conversion, isolated/unwired draft.
//! Original Dispatcharr authors retain application/algorithm credit.
use super::vod_api_projection::truthy;
use num_bigint::BigInt;
use serde_json::{Map, Value};
#[derive(Debug, PartialEq)]
pub(super) enum Fault {
    Type,
    Capacity,
}
fn trim(value: &str) -> &str {
    value.trim_matches(|c: char| c.is_whitespace() || matches!(c, '\u{1c}'..='\u{1f}'))
}
pub(super) fn text(value: &Value) -> Result<String, Fault> {
    match value {
        Value::String(text) => Ok(text.clone()),
        Value::Number(number)
            if matches!(
                number.to_string().as_str(),
                "NaN" | "Infinity" | "-Infinity"
            ) =>
        {
            Ok(match number.to_string().as_str() {
                "NaN" => "nan",
                "Infinity" => "inf",
                _ => "-inf",
            }
            .into())
        }
        _ => crate::python_action::collection_text(value, 256 * 1024).ok_or(Fault::Capacity),
    }
}
pub(super) fn string(value: &Value) -> Result<Option<String>, Fault> {
    if let Some(value) = value.as_str() {
        return Ok((!trim(value).is_empty()).then(|| trim(value).to_owned()));
    }
    if let Some(values) = value.as_array() {
        for value in values {
            if let Some(value) = value.as_str() {
                if !trim(value).is_empty() {
                    return Ok(Some(trim(value).to_owned()));
                }
            } else if !value.is_null() {
                let value = text(value)?;
                if !trim(&value).is_empty() {
                    return Ok(Some(trim(&value).to_owned()));
                }
            }
        }
    }
    Ok(None)
}
pub(super) fn should_update(existing: &Value, new: &Value) -> Result<bool, Fault> {
    Ok(string(new)?.is_some() && string(existing)?.is_none())
}
pub(super) fn integer(value: &str) -> Option<BigInt> {
    dispatcharr_ingest_core::source_integer_literal(value)
        .and_then(|s| BigInt::parse_bytes(s.as_bytes(), 10))
}
pub(super) fn duration_seconds(value: &Value) -> Option<i64> {
    let seconds = match value {
        Value::String(value) => integer(value)?.to_string().parse::<i64>().ok()?,
        Value::Number(value) => {
            let raw = value.to_string();
            if raw.contains(['.', 'e', 'E']) {
                let n = value.as_f64()?;
                if !n.is_finite() || n.fract() != 0.0 {
                    return None;
                }
                n as i64
            } else {
                value.as_i64()?
            }
        }
        _ => return None,
    };
    (seconds > 0 && seconds <= 172_800).then_some(seconds)
}
pub(super) fn duration(value: &Value) -> Result<Option<i64>, Fault> {
    let map = value.as_object().ok_or(Fault::Type)?;
    if let Some(value) = map.get("duration_secs") {
        if let Some(seconds) = duration_seconds(value) {
            return Ok(Some(seconds));
        }
    }
    let Some(value) = map.get("duration").filter(|v| !v.is_null() && **v != "") else {
        return Ok(None);
    };
    let raw = text(value)?;
    // int() accepts underscores/signs/space which Python str.isdigit does not.
    let digits = !raw.is_empty()
        && raw
            .chars()
            .all(|c| !c.is_whitespace() && !matches!(c, '+' | '-' | '_' | '\u{1c}'..='\u{1f}'));
    let parsed: Option<BigInt> = if digits && integer(&raw).is_some() {
        integer(&raw).map(|n| n * 60)
    } else {
        let parts = raw.split(':').collect::<Vec<_>>();
        match parts.as_slice() {
            [minutes, seconds] => integer(minutes)
                .zip(integer(seconds))
                .map(|(m, s)| m * 60 + s),
            [hours, minutes, seconds] => integer(hours)
                .zip(integer(minutes))
                .zip(integer(seconds))
                .map(|((h, m), s)| h * 3600 + m * 60 + s),
            _ => None,
        }
    };
    Ok(parsed
        .and_then(|n| n.to_string().parse::<i64>().ok())
        .filter(|n| *n > 0 && *n <= 172_800))
}
pub(super) fn rating(value: &Value) -> Result<Option<String>, Fault> {
    if !truthy(value) {
        return Ok(None);
    }
    let raw = text(value)?;
    let raw = trim(&raw).replace(',', ".");
    Ok(
        dispatcharr_ingest_core::source_float_literal(&raw).map(|n| {
            if n.is_nan() {
                "nan".into()
            } else if n.is_infinite() {
                if n.is_sign_negative() {
                    "-inf".into()
                } else {
                    "inf".into()
                }
            } else {
                crate::json_compat::float_string(n)
            }
        }),
    )
}
pub(super) fn clean(value: &Value) -> Result<Value, Fault> {
    if !truthy(value) {
        return Ok(Value::Null);
    }
    let mut result = Map::new();
    for (key, value) in value.as_object().ok_or(Fault::Type)? {
        if matches!(
            key.as_str(),
            "youtube_trailer" | "actors" | "director" | "cast" | "backdrop_path"
        ) {
            if let Some(text) = string(value)? {
                result.insert(
                    key.clone(),
                    if key == "backdrop_path" {
                        Value::Array(vec![text.into()])
                    } else {
                        text.into()
                    },
                );
            }
        } else if !value.is_null()
            && *value != ""
            && value
                .as_array()
                .is_none_or(|values| !values.is_empty() && !values.iter().all(Value::is_null))
        {
            result.insert(key.clone(), value.clone());
        }
    }
    Ok(if result.is_empty() {
        Value::Null
    } else {
        result.into()
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn original_refresh_helper_vectors() {
        let data: Value = serde_json::from_str(include_str!(
            "../datasets/source-provider-refresh-helpers-1.json"
        ))
        .unwrap();
        let rows = data["records"].as_array().unwrap();
        assert!(rows.len() > 500);
        for (index, row) in rows.iter().enumerate() {
            let args = row["args"].as_array().unwrap();
            let actual: Result<Value, Fault> = match row["function"].as_str().unwrap() {
                "normalize_duration_secs" => Ok(duration_seconds(&args[0]).into()),
                "normalize_rating" => rating(&args[0]).map(Into::into),
                "extract_duration_from_data" => duration(&args[0]).map(Into::into),
                "extract_string_from_array_or_string" => string(&args[0]).map(Into::into),
                "should_update_field" => should_update(&args[0], &args[1]).map(Into::into),
                "clean_custom_properties" => clean(&args[0]),
                _ => panic!("Unknown original function"),
            };
            if row["expected"].get("error_type").is_some() {
                assert!(
                    matches!(actual, Err(Fault::Type)),
                    "Source error case {index}: {row:?}, native {actual:?}"
                );
            } else {
                assert_eq!(
                    actual,
                    Ok(row["expected"]["value"].clone()),
                    "Source helper case {index}: {row:?}"
                );
            }
        }
    }
}

/// Python model-field comparisons occur before CharField/TextField prep; e.g.
/// False ==0 and1 ==1.0 can retain an earlier raw value on duplicate episodes.
pub(super) fn equal(left: &Value, right: &Value) -> bool {
    fn integral(value: &Value) -> Option<BigInt> {
        match value {
            Value::Bool(value) => Some(BigInt::from(u8::from(*value))),
            Value::Number(value) => {
                let raw = value.to_string();
                if raw.contains(['.', 'e', 'E']) {
                    let n = value.as_f64()?;
                    if !n.is_finite() || n.fract() != 0.0 {
                        return None;
                    }
                    BigInt::parse_bytes(format!("{:.0}", n.trunc()).as_bytes(), 10)
                } else {
                    BigInt::parse_bytes(raw.as_bytes(), 10)
                }
            }
            _ => None,
        }
    }
    let numeric = |v: &Value| matches!(v, Value::Bool(_) | Value::Number(_));
    if numeric(left) && numeric(right) {
        match (integral(left), integral(right)) {
            (Some(a), Some(b)) => return a == b,
            (Some(_), None) | (None, Some(_)) => return false,
            (None, None) => {}
        }
        return match (left.as_f64(), right.as_f64()) {
            (Some(a), Some(b)) if a.fract() != 0.0 && b.fract() != 0.0 => a == b,
            _ => false,
        };
    }
    match (left, right) {
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| equal(a, b))
        }
        (Value::Object(a), Value::Object(b)) => {
            a.len() == b.len()
                && a.iter()
                    .all(|(key, value)| b.get(key).is_some_and(|other| equal(value, other)))
        }
        _ => left == right,
    }
}
#[cfg(test)]
mod equality_tests {
    use super::*;
    #[test]
    fn original_python_field_equality() {
        let data: Value = serde_json::from_str(include_str!(
            "../datasets/source-provider-refresh-equality-1.json"
        ))
        .unwrap();
        let mut count = 0;
        for row in data["records"].as_array().unwrap() {
            if row["function"] != "provider_equal" {
                continue;
            }
            count += 1;
            assert_eq!(
                equal(&row["args"][0], &row["args"][1]),
                row["expected"]["value"].as_bool().unwrap(),
                "Source equality {row:?}"
            );
        }
        assert!(count >= 1600);
    }
}
