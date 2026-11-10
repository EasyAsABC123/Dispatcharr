//! Port of original Dispatcharr grid windows and Django date parsing.
//! Django's BSD license is retained in third-party/DJANGO-LICENSE.
//! Component only; HTTP visibility and streaming acceptance remain separate.
use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, Timelike, Weekday};
use num_bigint::BigInt;
use regex::Regex;
use std::{str::FromStr, sync::LazyLock};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GridWindowError {
    pub class: &'static str,
    pub message: String,
}
fn error(class: &'static str, message: impl Into<String>) -> GridWindowError {
    GridWindowError {
        class,
        message: message.into(),
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GridTimestamp {
    pub local: NaiveDateTime,
    pub offset_micros: i64,
}
impl GridTimestamp {
    fn add(&self, duration: Duration) -> Result<Self, GridWindowError> {
        let local = self
            .local
            .checked_add_signed(duration)
            .filter(|v| (1..=9999).contains(&v.year()))
            .ok_or_else(|| error("OverflowError", "date value out of range"))?;
        Ok(Self {
            local,
            offset_micros: self.offset_micros,
        })
    }
    fn utc_micros(&self) -> i64 {
        self.local.and_utc().timestamp_micros() - self.offset_micros
    }
    pub fn source_isoformat(&self) -> String {
        let mut value = self.local.format("%Y-%m-%dT%H:%M:%S").to_string();
        if self.local.nanosecond() != 0 {
            value.push_str(&format!(".{:06}", self.local.nanosecond() / 1000));
        }
        let offset = self.offset_micros.abs();
        value.push(if self.offset_micros < 0 { '-' } else { '+' });
        value.push_str(&format!(
            "{:02}:{:02}",
            offset / 3_600_000_000,
            offset / 60_000_000 % 60
        ));
        if offset % 60_000_000 != 0 {
            value.push_str(&format!(":{:02}", offset / 1_000_000 % 60));
            if offset % 1_000_000 != 0 {
                value.push_str(&format!(".{:06}", offset % 1_000_000));
            }
        }
        value
    }
}
static FALLBACK: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(concat!(
    r"\A(?P<date>[0-9]{4}-[0-9]{1,2}-[0-9]{1,2})",
    r"(?:[T ](?P<h>[0-9]{1,2}):(?P<m>[0-9]{1,2})(?::(?P<s>[0-9]{1,2})(?:[.,](?P<f>[0-9]{1,12}))?)?",
    r"[\s\x1c-\x1f]*(?P<zone>Z|[+-][0-9]{2}(?::?[0-9]{2})?)?)?\n?\z"
)).unwrap()
});
fn fraction(raw: &str) -> i64 {
    let digits: String = raw.chars().take(6).collect();
    digits.parse::<i64>().unwrap_or(0) * 10_i64.pow(6 - digits.len() as u32)
}
fn clock(raw: &str) -> Option<(u32, u32, u32, i64)> {
    let mut parts = raw.split(['.', ',']);
    let base = parts.next()?;
    let fraction = parts.next().map(fraction).unwrap_or(0);
    let digits = base.replace(':', "");
    let h = digits.get(..2)?.parse().ok()?;
    let m = digits.get(2..4).unwrap_or("0").parse().ok()?;
    let s = digits.get(4..6).unwrap_or("0").parse().ok()?;
    Some((h, m, s, fraction))
}
fn date(raw: &str) -> Result<NaiveDate, GridWindowError> {
    if raw.contains('W') {
        let digits = raw.replace('-', "");
        let year = digits[..4].parse().unwrap();
        if !(1..=9999).contains(&year) {
            return Err(error("ValueError", "Invalid ISO year"));
        }
        let week = digits[5..7].parse().unwrap();
        let weekday = digits.get(7..8).unwrap_or("1").parse::<usize>().unwrap();
        return NaiveDate::from_isoywd_opt(
            year,
            week,
            [
                Weekday::Mon,
                Weekday::Tue,
                Weekday::Wed,
                Weekday::Thu,
                Weekday::Fri,
                Weekday::Sat,
                Weekday::Sun,
            ][weekday - 1],
        )
        .filter(|value| (1..=9999).contains(&value.year()))
        .ok_or_else(|| error("ValueError", "Invalid week"));
    }
    let parts: Vec<i32> = if raw.contains('-') {
        raw.split('-').map(|v| v.parse().unwrap()).collect()
    } else {
        vec![
            raw[..4].parse().unwrap(),
            raw[4..6].parse().unwrap(),
            raw[6..8].parse().unwrap(),
        ]
    };
    let (y, m, d) = (parts[0], parts[1], parts[2]);
    if !(1..=9999).contains(&y) {
        return Err(error("ValueError", format!("year {y} is out of range")));
    }
    if !(1..=12).contains(&m) {
        return Err(error("ValueError", "month must be in 1..12"));
    }
    NaiveDate::from_ymd_opt(y, m as u32, d as u32)
        .ok_or_else(|| error("ValueError", "day is out of range for month"))
}
pub fn grid_parse_datetime(raw: &str, name: &str) -> Result<GridTimestamp, GridWindowError> {
    // Django first tries datetime.fromisoformat and catches every ValueError.
    // Only a matched fallback regex exposes constructor ValueError to callers.
    if let Some(value) = crate::epg_grid_iso::datetime(raw) {
        return Ok(value);
    }
    if let Some(value) = crate::epg_grid_iso::date(raw) {
        return Ok(value);
    }
    let normalized: String = raw
        .chars()
        .map(|c| {
            if c.is_ascii() {
                c
            } else {
                dispatcharr_ingest_core::source_integer_literal(&c.to_string())
                    .filter(|v| v.len() == 1)
                    .and_then(|v| v.chars().next())
                    .unwrap_or(c)
            }
        })
        .collect();
    if let Some(captures) = FALLBACK.captures(&normalized) {
        return captured_timestamp(&captures);
    }
    Err(error(
        "GridWindowError",
        format!("Invalid datetime for {name}: {raw}. Use ISO 8601 (e.g. 2026-02-14T18:00:00Z)."),
    ))
}
fn captured_timestamp(captures: &regex::Captures<'_>) -> Result<GridTimestamp, GridWindowError> {
    let day = date(&captures["date"])?;
    let (h, m, s, micro) = if let Some(raw) = captures.name("clock") {
        clock(raw.as_str()).unwrap()
    } else if let Some(h) = captures.name("h") {
        (
            h.as_str().parse().unwrap(),
            captures["m"].parse().unwrap(),
            captures
                .name("s")
                .map(|v| v.as_str().parse().unwrap())
                .unwrap_or(0),
            captures
                .name("f")
                .map(|v| fraction(v.as_str()))
                .unwrap_or(0),
        )
    } else {
        (0, 0, 0, 0)
    };
    for (value, max, name) in [(h, 23, "hour"), (m, 59, "minute"), (s, 59, "second")] {
        if value > max {
            return Err(error("ValueError", format!("{name} must be in 0..{max}")));
        }
    }
    let mut offset_micros = 0;
    if let Some(zone) = captures.name("zone").filter(|v| v.as_str() != "Z") {
        let raw = zone.as_str();
        let (h, m, s, f) = clock(&raw[1..]).unwrap();
        offset_micros = (i64::from(h) * 3600 + i64::from(m) * 60 + i64::from(s)) * 1_000_000 + f;
        if raw.starts_with('-') {
            offset_micros = -offset_micros;
        }
        if offset_micros.abs() >= 86_400_000_000 {
            let days = offset_micros.div_euclid(86_400_000_000);
            let seconds = offset_micros.rem_euclid(86_400_000_000) / 1_000_000;
            let micros = offset_micros.rem_euclid(1_000_000);
            let mut parts = Vec::new();
            if days != 0 {
                parts.push(format!("days={days}"));
            }
            if seconds != 0 {
                parts.push(format!("seconds={seconds}"));
            }
            if micros != 0 {
                parts.push(format!("microseconds={micros}"));
            }
            return Err(error("ValueError",format!("offset must be a timedelta strictly between -timedelta(hours=24) and timedelta(hours=24), not datetime.timedelta({}).",parts.join(", "))));
        }
    }
    Ok(GridTimestamp {
        local: day.and_hms_micro_opt(h, m, s, micro as u32).unwrap(),
        offset_micros,
    })
}
pub fn grid_resolve_window(
    params: &[(String, String)],
    now: GridTimestamp,
    history: i64,
) -> Result<(GridTimestamp, GridTimestamp), GridWindowError> {
    let get = |key: &str| {
        params
            .iter()
            .rev()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    };
    let absolute =
        get("start").is_some_and(|v| !v.is_empty()) || get("end").is_some_and(|v| !v.is_empty());
    let (start, end, validate) = if absolute {
        let start = match get("start").filter(|v| !v.is_empty()) {
            Some(v) => grid_parse_datetime(v, "start")?,
            None => now.add(Duration::hours(-1))?,
        };
        let end = match get("end").filter(|v| !v.is_empty()) {
            Some(v) => grid_parse_datetime(v, "end")?,
            None => start.add(Duration::hours(24))?,
        };
        (start, end, true)
    } else {
        let integer = |name: &str, max: i64, min: i64| -> Result<Option<i64>, GridWindowError> {
            get(name)
                .map(|raw| {
                    let value = dispatcharr_ingest_core::source_integer_literal(raw)
                        .and_then(|v| BigInt::from_str(&v).ok())
                        .ok_or_else(|| {
                            error(
                                "GridWindowError",
                                format!("Invalid integer for {name}: {raw}."),
                            )
                        })?;
                    Ok(value
                        .max(BigInt::from(min))
                        .min(BigInt::from(max))
                        .to_string()
                        .parse()
                        .unwrap())
                })
                .transpose()
        };
        let days = integer("days", 365, 1)?;
        let previous = integer("prev_days", history, 0)?;
        (
            now.add(
                previous
                    .map(|v| Duration::days(-v))
                    .unwrap_or(Duration::hours(-1)),
            )?,
            now.add(days.map(Duration::days).unwrap_or(Duration::hours(24)))?,
            days.is_some() || previous.is_some(),
        )
    };
    if validate {
        let span = end.utc_micros() - start.utc_micros();
        if span <= 0 {
            return Err(error("GridWindowError", "end must be after start."));
        }
        if span > (365 + history) * 86_400_000_000 {
            return Err(error(
                "GridWindowError",
                format!(
                    "Window is too large. Maximum span is {} days.",
                    365 + history
                ),
            ));
        }
    }
    Ok((start, end))
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    #[test]
    fn original_compiled_python_and_django_iso_grammar() {
        let corpus: Value =
            serde_json::from_str(include_str!("../datasets/source-grid-datetime-fuzz.json"))
                .unwrap();
        let cases = corpus["cases"].as_array().unwrap();
        assert_eq!(cases.len(), 7189);
        for case in cases {
            let actual = match grid_parse_datetime(
                case["raw"].as_str().unwrap(),
                case["name"].as_str().unwrap(),
            ) {
                Ok(value) => json!({"value":value.source_isoformat()}),
                Err(error) => json!({"error":error.class,"message":error.message}),
            };
            let mut expected = case.as_object().unwrap().clone();
            expected.remove("raw");
            expected.remove("name");
            assert_eq!(actual, Value::Object(expected), "raw={:?}", case["raw"]);
        }
    }
    #[test]
    fn original_grid_window_corpus() {
        let corpus: Value =
            serde_json::from_str(include_str!("../datasets/source-grid-windows.json")).unwrap();
        let cases = corpus["cases"].as_array().unwrap();
        assert_eq!(cases.len(), 557);
        for case in cases {
            let input = &case["inputs"];
            let params: Vec<(String, String)> =
                serde_json::from_value(input["params"].clone()).unwrap();
            let now = grid_parse_datetime(input["now"].as_str().unwrap(), "now").unwrap();
            let actual = match grid_resolve_window(
                &params,
                now,
                input["history_max_days"].as_i64().unwrap(),
            ) {
                Ok((start, end)) => {
                    json!({"value":[{"datetime":start.source_isoformat()},{"datetime":end.source_isoformat()}]})
                }
                Err(e) => json!({"error":e.class,"message":e.message}),
            };
            assert_eq!(actual, case["expected"], "input={input}");
        }
    }
}
