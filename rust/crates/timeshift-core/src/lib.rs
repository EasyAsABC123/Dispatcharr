//! Pure catch-up helpers ported from Dispatcharr apps/timeshift/helpers.py.
//! All original application credit belongs to upstream Dispatcharr authors
//! and contributors; see repository ATTRIBUTION.md and AGPL LICENSE.
use chrono::{DateTime, Datelike, NaiveDateTime, Timelike, Utc};

pub const DEFAULT_DURATION_MINUTES: u32 = 120;
pub const DURATION_BUFFER_MINUTES: u32 = 5;
pub const MAX_DURATION_MINUTES: u32 = 480;

pub fn normalize_timestamp(input: &str) -> Option<String> {
    let value = input.trim();
    if value.is_empty() {
        return None;
    }
    if value.bytes().all(|c| c.is_ascii_digit()) {
        let timestamp = match value.len() {
            10 => DateTime::from_timestamp(value.parse::<i64>().ok()?, 0),
            13 => DateTime::from_timestamp_millis(value.parse::<i64>().ok()?),
            _ => return None,
        }?;
        return Some(
            timestamp
                .naive_utc()
                .format("%Y-%m-%dT%H:%M:%S")
                .to_string(),
        );
    }
    if value.contains('T') {
        if let Ok(datetime) = DateTime::parse_from_rfc3339(value) {
            return valid_datetime(datetime.naive_utc())
                .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S").to_string());
        }
        for format in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%dT%H:%M", "%Y%m%dT%H%M%S%.f"] {
            if let Ok(datetime) = NaiveDateTime::parse_from_str(value, format) {
                return valid_datetime(datetime)
                    .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S").to_string());
            }
        }
        return None;
    }
    let bytes = value.as_bytes();
    if !matches!(bytes.len(), 16 | 19)
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || !matches!(bytes[10], b':' | b'_' | b' ')
        || !matches!(bytes[13], b'-' | b':')
        || (bytes.len() == 19 && !matches!(bytes[16], b'-' | b':'))
    {
        return None;
    }
    if [0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15]
        .iter()
        .any(|&i| !bytes[i].is_ascii_digit())
        || (bytes.len() == 19 && (!bytes[17].is_ascii_digit() || !bytes[18].is_ascii_digit()))
    {
        return None;
    }
    // Source normalizes the shape without checking calendar/time ranges.
    Some(format!(
        "{}T{}:{}:{}",
        &value[..10],
        &value[11..13],
        &value[14..16],
        if bytes.len() == 19 {
            &value[17..19]
        } else {
            "00"
        }
    ))
}
fn valid_datetime(value: NaiveDateTime) -> Option<NaiveDateTime> {
    if (1..=9999).contains(&value.year()) && value.nanosecond() < 1_000_000_000 {
        Some(value)
    } else {
        None
    }
}
pub fn parse_timestamp(value: &str) -> Option<NaiveDateTime> {
    valid_datetime(
        NaiveDateTime::parse_from_str(&normalize_timestamp(value)?, "%Y-%m-%dT%H:%M:%S").ok()?,
    )
}
#[derive(Clone, Copy)]
pub enum TimestampShape {
    ColonDash,
    ColonSeconds,
    Underscore,
    Sql,
}
pub fn format_timestamp(value: &str, shape: TimestampShape) -> String {
    let Some(datetime) = parse_timestamp(value) else {
        return value.into();
    };
    datetime
        .format(match shape {
            TimestampShape::ColonDash => "%Y-%m-%d:%H-%M",
            TimestampShape::ColonSeconds => "%Y-%m-%d:%H:%M:%S",
            TimestampShape::Underscore => "%Y-%m-%d_%H-%M",
            TimestampShape::Sql => "%Y-%m-%d %H:%M:%S",
        })
        .to_string()
}
pub fn provider_timestamp(value: &str, zone: Option<&str>) -> String {
    let Some(zone) = zone.filter(|zone| !zone.is_empty() && *zone != "UTC") else {
        return value.into();
    };
    let Some(datetime) = parse_timestamp(value) else {
        return value.into();
    };
    let Ok(zone) = zone.parse::<chrono_tz::Tz>() else {
        return value.into();
    };
    datetime
        .and_utc()
        .with_timezone(&zone)
        .format("%Y-%m-%d:%H-%M")
        .to_string()
}
pub fn client_duration_window(input: &str) -> Option<u32> {
    let value = input.trim();
    let unsigned = value.strip_prefix('+').unwrap_or(value);
    let bytes = unsigned.as_bytes();
    if bytes.is_empty()
        || bytes.iter().enumerate().any(|(i, &b)| {
            !(b.is_ascii_digit()
                || (b == b'_'
                    && i > 0
                    && i + 1 < bytes.len()
                    && bytes[i - 1].is_ascii_digit()
                    && bytes[i + 1].is_ascii_digit()))
        })
    {
        return None;
    }
    let digits = unsigned.replace('_', "");
    if digits.bytes().all(|b| b == b'0') {
        return None;
    }
    let minutes = digits.parse::<u128>().unwrap_or(u128::MAX);
    Some(
        minutes
            .saturating_add(DURATION_BUFFER_MINUTES as u128)
            .min(MAX_DURATION_MINUTES as u128) as u32,
    )
}
pub fn programme_age_days(input: &str, now: NaiveDateTime) -> Option<u64> {
    let elapsed = (now - parse_timestamp(input)?).num_seconds();
    if elapsed <= 0 {
        Some(0)
    } else {
        Some((elapsed as u64).div_ceil(86400))
    }
}
// Credentials deliberately have no Debug derive to avoid exposing provider URLs.
pub struct ProviderCredentials {
    pub server_url: String,
    pub username: String,
    pub password: String,
}
fn quote(value: &str) -> String {
    let mut result = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            result.push(byte as char);
        } else {
            use std::fmt::Write;
            let _ = write!(result, "%{byte:02X}");
        }
    }
    result
}
pub fn query_url(
    credentials: &ProviderCredentials,
    stream: &str,
    timestamp: &str,
    duration: u32,
) -> String {
    format!("{}/streaming/timeshift.php?username={}&password={}&stream={stream}&start={timestamp}&duration={duration}",credentials.server_url.trim_end_matches('/'),quote(&credentials.username),quote(&credentials.password))
}
pub fn path_url(
    credentials: &ProviderCredentials,
    stream: &str,
    timestamp: &str,
    duration: u32,
) -> String {
    format!(
        "{}/timeshift/{}/{}/{duration}/{timestamp}/{stream}.ts",
        credentials.server_url.trim_end_matches('/'),
        quote(&credentials.username),
        quote(&credentials.password)
    )
}
pub fn candidate_urls(
    credentials: &ProviderCredentials,
    stream: &str,
    timestamp: &str,
    duration: u32,
) -> Vec<String> {
    let colon = format_timestamp(timestamp, TimestampShape::ColonDash);
    let underscore = format_timestamp(timestamp, TimestampShape::Underscore);
    let seconds = format_timestamp(timestamp, TimestampShape::ColonSeconds);
    let sql = format_timestamp(timestamp, TimestampShape::Sql);
    vec![
        path_url(credentials, stream, &colon, duration),
        path_url(credentials, stream, &underscore, duration),
        path_url(credentials, stream, &seconds, duration),
        query_url(credentials, stream, &underscore, duration),
        query_url(credentials, stream, &sql, duration),
        query_url(credentials, stream, &colon, duration),
        query_url(credentials, stream, &seconds, duration),
    ]
}
pub fn utc_timestamp(value: i64) -> Option<NaiveDateTime> {
    DateTime::<Utc>::from_timestamp(value, 0).map(|value| value.naive_utc())
}

mod ranges;
pub use ranges::{
    downstream_length_headers, parse_client_range, parse_content_range, representation_length,
    ContentRange, LengthRequest, RangeOverflow,
};
