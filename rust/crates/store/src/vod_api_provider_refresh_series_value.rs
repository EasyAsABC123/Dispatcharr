//! Isolated/unwired Series refresh metadata/date conversion. Reuses the pinned
//! CPython C ISO parser; Django DateField timezone is the Source UTC setting.
//! Original Dispatcharr/CPython authors retain credit and license notices.
use super::{vod_api_projection::truthy, vod_api_provider_refresh_value as source};
use chrono::{Datelike, Duration, NaiveDate};
use serde_json::Value;
use std::sync::LazyLock;
fn trim(text: &str) -> &str {
    text.trim_matches(|c: char| c.is_whitespace() || matches!(c, '\u{1c}'..='\u{1f}'))
}
fn digit(c: char) -> Option<char> {
    dispatcharr_ingest_core::source_integer_literal(&c.to_string())
        .filter(|v| v.len() == 1)
        .and_then(|v| v.chars().next())
        .filter(char::is_ascii_digit)
}
fn normalized(text: &str) -> String {
    text.chars().map(|c| digit(c).unwrap_or(c)).collect()
}
pub(super) fn title_year(title: &str) -> Option<i64> {
    static PATTERNS: LazyLock<[regex::Regex; 3]> = LazyLock::new(|| {
        [
            r"\(([0-9]{4})\)",
            r"[\s\x1c-\x1f]-[\s\x1c-\x1f]([0-9]{4})",
            r"[\s\x1c-\x1f]([0-9]{4})(?:\n)?\z",
        ]
        .map(|p| regex::Regex::new(p).unwrap())
    });
    let text = normalized(title);
    for pattern in PATTERNS.iter() {
        if let Some(found) = pattern.captures(&text) {
            let year = found[1].parse::<i64>().ok()?;
            if (1900..=2030).contains(&year) {
                return Some(year);
            }
        }
    }
    None
}
pub(super) fn year(data: &Value) -> Option<i64> {
    let map = data.as_object()?;
    if let Some(value) = map.get("year").filter(|v| truthy(v)) {
        let year = match value {
            Value::String(v) => source::integer(v).and_then(|v| v.to_string().parse::<i64>().ok()),
            Value::Number(v) => v
                .as_f64()
                .filter(|v| v.is_finite())
                .map(|v| v.trunc() as i64),
            Value::Bool(v) => Some(i64::from(*v)),
            _ => None,
        };
        if let Some(year) = year.filter(|y| (1900..=2030).contains(y)) {
            return Some(year);
        }
    }
    for key in ["releaseDate", "release_date"] {
        if let Some(text) = map
            .get(key)
            .and_then(Value::as_str)
            .filter(|v| !trim(v).is_empty())
        {
            if let Some(year) = source::integer(trim(text.split('-').next().unwrap_or("")))
                .and_then(|n| n.to_string().parse::<i64>().ok())
                .filter(|y| (1900..=2030).contains(y))
            {
                return Some(year);
            }
        }
    }
    title_year(map.get("name").and_then(Value::as_str)?)
}
fn fallback_date(raw: &str) -> Option<NaiveDate> {
    let mut parts = raw.split('-');
    let year_raw = parts.next()?;
    let month_raw = parts.next()?;
    let day_raw = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    let year: Vec<_> = year_raw.chars().take(5).collect();
    if year.len() != 4 {
        return None;
    }
    let year = year
        .into_iter()
        .map(digit)
        .collect::<Option<String>>()?
        .parse::<i32>()
        .ok()?;
    let month = month_raw;
    if !(1..=2).contains(&month.len()) || !month.bytes().all(|v| v.is_ascii_digit()) {
        return None;
    }
    let month = month.parse::<u32>().ok()?;
    let day: Vec<_> = day_raw.chars().take(3).collect();
    let day = match day.as_slice() {
        [one] if matches!(one, '1'..='9') => one.to_digit(10)?,
        [first, last] if matches!(first, '1' | '2') => {
            first.to_digit(10)? * 10 + digit(*last)?.to_digit(10)?
        }
        ['0', last] if matches!(last, '1'..='9') => last.to_digit(10)?,
        ['3', last] if matches!(last, '0' | '1') => 30 + last.to_digit(10)?,
        [' ', last] if matches!(last, '1'..='9') => last.to_digit(10)?,
        _ => return None,
    };
    (1..=9999)
        .contains(&year)
        .then(|| NaiveDate::from_ymd_opt(year, month, day))
        .flatten()
}
/// This is DateField preparation, NOT a replacement ISO grammar. Root's pinned
/// C parser handles datetime.fromisoformat; strptime fallback is date-only.
pub(super) fn date_field(data: &Value) -> Result<Value, source::Fault> {
    let Some(map) = data.as_object() else {
        return Ok(Value::Null);
    };
    for key in ["air_date", "releasedate", "release_date"] {
        let Some(raw) = map
            .get(key)
            .and_then(Value::as_str)
            .filter(|v| !trim(v).is_empty())
        else {
            continue;
        };
        if let Some(stamp) = crate::epg_grid_iso::datetime(raw) {
            let date = stamp
                .local
                .checked_sub_signed(Duration::microseconds(stamp.offset_micros))
                .filter(|v| (1..=9999).contains(&v.year()))
                .ok_or(source::Fault::Type)?
                .date();
            return Ok(date.to_string().into());
        }
        if let Some(date) = fallback_date(raw) {
            return Ok(date.to_string().into());
        }
    }
    Ok(Value::Null)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn original_series_metadata_vectors() {
        let corpus: Value = serde_json::from_str(include_str!(
            "../datasets/source-provider-refresh-series-helpers-1.json"
        ))
        .unwrap();
        let mut count = 0;
        for row in corpus["records"].as_array().unwrap() {
            let actual = match row["function"].as_str().unwrap() {
                "extract_year_from_data" => Ok(year(&row["args"][0]).into()),
                "_coerce_provider_info" => Ok(
                    super::super::vod_api_provider_refresh_movie::details(&row["args"][0]),
                ),
                "episode_date_field" => date_field(&row["args"][0]),
                _ => continue,
            };
            count += 1;
            if row["expected"].get("error_type").is_some() {
                assert!(actual.is_err(), "Source error {row:?}, got {actual:?}");
            } else {
                assert_eq!(
                    actual,
                    Ok(row["expected"]["value"].clone()),
                    "Source Series vector {row:?}"
                );
            }
        }
        assert!(count > 100);
    }
}
