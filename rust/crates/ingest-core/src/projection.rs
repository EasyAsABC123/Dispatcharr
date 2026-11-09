//! M3U row projection from original process_m3u_batch_direct; no job/SQL side effects.
use crate::{stream_hash, Entry, StreamIdentity};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProjectedStream {
    pub stream_hash: String,
    pub name: String,
    pub url: String,
    pub logo_url: String,
    pub tvg_id: String,
    pub channel_group_id: i64,
    pub custom_properties: Value,
    pub is_adult: bool,
    pub stream_id: Option<i64>,
    pub stream_chno: Option<f64>,
    pub is_catchup: bool,
    pub catchup_days: i64,
    pub is_radio: bool,
}
pub struct ProjectionContext<'a> {
    pub account_id: i64,
    pub account_type: &'a str,
    pub groups: &'a BTreeMap<String, i64>,
    pub hash_keys: &'a [&'a str],
    pub max_name_characters: usize,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProjectionError {
    MissingUrl,
    IntegerRange,
}

// Decimal-zero ranges captured from pinned Python3.13.18 / Unicode15.1.0.
// Python int/float accept Unicode decimal digits, not all is_numeric characters.
pub(crate) const DECIMAL_ZEROS: &[u32] = &[
    48, 1632, 1776, 1984, 2406, 2534, 2662, 2790, 2918, 3046, 3174, 3302, 3430, 3558, 3664, 3792,
    3872, 4160, 4240, 6112, 6160, 6470, 6608, 6784, 6800, 6992, 7088, 7232, 7248, 42528, 43216,
    43264, 43472, 43504, 43600, 44016, 65296, 66720, 68912, 69734, 69872, 69942, 70096, 70384,
    70736, 70864, 71248, 71360, 71472, 71904, 72016, 72784, 73040, 73120, 73552, 92768, 92864,
    93008, 120782, 120792, 120802, 120812, 120822, 123200, 123632, 124144, 125264, 130032,
];
// Python int/float reject ASCII information separators accepted by str.strip.
// Non-ASCII whitespace is transformed by Python's Unicode numeric decoder.
pub(crate) fn numeric_whitespace(c: char) -> bool {
    matches!(c, '\t'..='\r' | ' ' | '\u{85}' | '\u{a0}' | '\u{1680}'
        | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}'
        | '\u{205f}' | '\u{3000}')
}
fn numeric_ascii(value: &str) -> Option<String> {
    let mapped = value
        .trim_matches(numeric_whitespace)
        .chars()
        .map(|character| {
            if character.is_ascii() {
                return character;
            }
            let code = character as u32;
            DECIMAL_ZEROS
                .iter()
                .find_map(|zero| {
                    code.checked_sub(*zero)
                        .filter(|digit| *digit < 10)
                        .map(|digit| (b'0' + digit as u8) as char)
                })
                .unwrap_or(character)
        })
        .collect::<String>();
    let bytes = mapped.as_bytes();
    for (index, byte) in bytes.iter().enumerate() {
        if *byte == b'_'
            && (index == 0
                || index + 1 == bytes.len()
                || !bytes[index - 1].is_ascii_digit()
                || !bytes[index + 1].is_ascii_digit())
        {
            return None;
        }
    }
    Some(mapped.replace('_', ""))
}
fn integer_literal(value: &str) -> Option<String> {
    let value = numeric_ascii(value)?;
    let digits = value.strip_prefix(['+', '-']).unwrap_or(&value);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) || digits.len() > 4300
    {
        return None;
    }
    let negative = value.starts_with('-');
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        Some("0".into())
    } else {
        Some(format!("{}{digits}", if negative { "-" } else { "" }))
    }
}
pub(crate) fn integer(value: &str) -> Result<Option<i64>, ProjectionError> {
    integer_literal(value)
        .map(|value| value.parse().map_err(|_| ProjectionError::IntegerRange))
        .transpose()
}
fn decimal(value: &str) -> Option<f64> {
    numeric_ascii(value)?.parse().ok()
}

/// Project one parser-produced entry after enabled-group selection. Ordered
/// account regex filters must run before this function; they are not replaced
/// with Rust regex semantics or silently ignored by a public refresh route.
pub fn project_entry(
    entry: &Entry,
    context: &ProjectionContext<'_>,
) -> Result<Option<ProjectedStream>, ProjectionError> {
    let url = entry.url.as_deref().ok_or(ProjectionError::MissingUrl)?;
    if url.chars().count() > 4096 {
        return Ok(None);
    }
    let group = entry
        .attributes
        .get("group-title")
        .map_or("Default Group", String::as_str);
    let Some(group_id) = context.groups.get(group) else {
        return Ok(None);
    };
    let name = entry
        .name
        .chars()
        .take(context.max_name_characters)
        .collect::<String>();
    let attrs = &entry.attributes;
    let attribute = |key: &str| attrs.get(key).map_or("", String::as_str);
    let xc = context.account_type == "XC";
    let stream_id = if xc {
        integer(attribute("stream_id"))?
    } else {
        None
    };
    let stream_chno = if xc {
        attrs.get("num")
    } else {
        attrs
            .get("tvg-chno")
            .or_else(|| attrs.get("channel-number"))
    }
    .and_then(|value| decimal(value));
    let catchup_days = integer(attribute("tv_archive_duration"))?.unwrap_or(0);
    let mut custom = attrs
        .iter()
        .map(|(key, value)| (key.clone(), Value::String(value.clone())))
        .collect::<Map<_, _>>();
    if let Some(options) = &entry.vlc_opts {
        custom.insert("vlc_opts".into(), serde_json::to_value(options).unwrap());
    }
    Ok(Some(ProjectedStream {
        stream_hash: stream_hash(
            &StreamIdentity {
                name: &name,
                url,
                tvg_id: attribute("tvg-id"),
                m3u_id: Some(context.account_id),
                group: Some(group),
                account_type: context.account_type,
                stream_id,
            },
            context.hash_keys,
        ),
        name,
        url: url.to_owned(),
        logo_url: attribute("tvg-logo").to_owned(),
        tvg_id: attribute("tvg-id").to_owned(),
        channel_group_id: *group_id,
        custom_properties: Value::Object(custom),
        is_adult: integer_literal(attribute("is_adult")).as_deref() == Some("1"),
        stream_id,
        stream_chno,
        is_catchup: matches!(attribute("tv_archive"), "1" | "True"),
        catchup_days,
        is_radio: matches!(attribute("radio").to_lowercase().as_str(), "1" | "true")
            || attribute("stream_type").to_lowercase() == "radio_streams",
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn numeric_conversion_preserves_python_decimal_digits_separators_and_zero_truthiness() {
        assert_eq!(integer(" +٠٠١_٢ "), Ok(Some(12)));
        assert_eq!(integer("-0"), Ok(Some(0)));
        assert_eq!(integer("1__2"), Ok(None));
        assert_eq!(integer("²"), Ok(None));
        assert_eq!(decimal("٢_٠.٥"), Some(20.5));
        assert_eq!(decimal("1_.2"), None);
    }
}
