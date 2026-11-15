//! Source core.utils.natural_sort_key, with bounded existing numeric helpers.
//! Original Dispatcharr/Python authors retain application/algorithm credit.
//! Uses pinned Source Unicode metadata; cross-application acceptance remains required.
use crate::StoreError;
use std::cmp::Ordering;
fn unsupported() -> StoreError {
    StoreError::Unsupported("Auto-sync natural-sort Unicode/type domain is not qualified")
}
fn number(raw: &str) -> Result<String, StoreError> {
    let canonical = dispatcharr_ingest_core::source_integer_literal(raw).ok_or_else(unsupported)?;
    // Stream.name's actual schema bound512 limits every numeric chunk. Length
    // plus canonical digits compares arbitrary-width positive Python integers
    // without narrowing to i64 or padding each number to512 bytes.
    if canonical.len() > 512 || canonical.starts_with('-') {
        return Err(unsupported());
    }
    Ok(format!("N{:03}{canonical}", canonical.len()))
}
fn text(raw: &str) -> Result<String, StoreError> {
    let language = super::auto_sync_language::Language::source()?;
    if !raw.is_empty() && raw.chars().all(|c| language.is_digit(c)) {
        // Non-decimal isdigit inputs reach Source int() and raise ValueError;
        // the final normal caught-error renderer still needs its grouped proof.
        return number(raw);
    }
    Ok(format!("T{}", language.lower(raw)))
}
pub(crate) fn key(name: &str) -> Result<Vec<String>, StoreError> {
    let mut result = Vec::new();
    let mut tail = name;
    loop {
        let Some(start) = tail.find(|c: char| c.is_ascii_digit()) else {
            result.push(text(tail)?);
            break;
        };
        result.push(text(&tail[..start])?);
        let digits = &tail[start..];
        let length = digits.bytes().take_while(u8::is_ascii_digit).count();
        result.push(number(&digits[..length])?);
        tail = &digits[length..];
    }
    Ok(result)
}
/// SQL uses encoded keys for an external stable sort. Validate adjacent keys
/// with actual Python list semantics before the sorted stream set is admitted;
/// incomparable int/str chunks remain explicit, never an invented total order.
pub(crate) fn compare(left: &[String], right: &[String]) -> Result<Ordering, StoreError> {
    for (left, right) in left.iter().zip(right) {
        if left.as_bytes().first() != right.as_bytes().first() {
            return Err(unsupported());
        }
        let order = left.cmp(right);
        if order != Ordering::Equal {
            return Ok(order);
        }
    }
    Ok(left.len().cmp(&right.len()))
}
#[cfg(test)]
mod controls {
    use super::*;
    #[test]
    fn actual_source_ascii_split_big_integer_and_zero_equality() {
        let mut names = [
            "PPV 10",
            "PPV 2",
            "PPV 0002",
            "PPV 000000000000000000000000000000000000000000000000000000000011",
        ];
        names.sort_by(|a, b| compare(&key(a).unwrap(), &key(b).unwrap()).unwrap());
        assert_eq!(
            names,
            [
                "PPV 2",
                "PPV 0002",
                "PPV 10",
                "PPV 000000000000000000000000000000000000000000000000000000000011"
            ]
        );
        assert_eq!(key("A000B").unwrap(), key("a0b").unwrap());
    }
    #[test]
    fn source_unicode_decimal_chunk_and_mixed_type_are_not_faked_as_text() {
        assert_eq!(key("١٢").unwrap(), vec!["N00212"]);
        assert!(compare(&key("١٢").unwrap(), &key("News").unwrap()).is_err());
        assert!(key("²").is_err());
        assert_eq!(key("News²").unwrap(), vec!["Tnews²"]);
    }
}
