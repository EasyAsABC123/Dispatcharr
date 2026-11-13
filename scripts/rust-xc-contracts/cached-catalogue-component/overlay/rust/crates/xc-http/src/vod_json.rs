//! Borrowed JSON projection using internal lexical spans, without a Value tree.
//! No shared serde_json feature is enabled. Original Dispatcharr authors retain
//! application/algorithm credit; Source spelling follows json.dumps.
use crate::{
    project::JsonBytes,
    vod_span::{self, RawValue},
};
use dispatcharr_store::StoreError;
use std::borrow::Cow;

#[derive(Debug)]
pub(crate) enum Error {
    Syntax,
    Nonfinite,
    SourceIntegerLimit,
    SourceObject,
    Capacity(StoreError),
}
impl From<StoreError> for Error {
    fn from(value: StoreError) -> Self {
        Self::Capacity(value)
    }
}

pub(crate) fn write(out: &mut JsonBytes, raw: &RawValue) -> Result<(), Error> {
    write_at(out, vod_span::parse(raw)?, 0)
}
fn write_at(out: &mut JsonBytes, raw: &RawValue, depth: usize) -> Result<(), Error> {
    if depth >= 128 {
        return Err(crate::reader::capacity().into());
    }
    let text = raw.trim_matches([' ', '\n', '\r', '\t']);
    match text.as_bytes().first().copied() {
        Some(b'{') => {
            out.raw(b"{")?;
            for (index, member) in vod_span::Object::new(text)?.enumerate() {
                let (key, value) = member?;
                if index > 0 {
                    out.raw(b", ")?;
                }
                out.string(&key)?;
                out.raw(b": ")?;
                write_at(out, value, depth + 1)?;
            }
            out.raw(b"}")?;
        }
        Some(b'[') => {
            out.raw(b"[")?;
            for (index, value) in vod_span::Array::new(text)?.enumerate() {
                if index > 0 {
                    out.raw(b", ")?;
                }
                write_at(out, value?, depth + 1)?;
            }
            out.raw(b"]")?;
        }
        Some(b'"') => out.string(&vod_span::decode_string(text)?)?,
        Some(b't' | b'f' | b'n') => out.raw(text.as_bytes())?,
        Some(b'-' | b'0'..=b'9') => {
            if text.bytes().any(|b| matches!(b, b'.' | b'e' | b'E')) {
                let value =
                    dispatcharr_ingest_core::source_float_literal(text).ok_or(Error::Syntax)?;
                let encoded =
                    dispatcharr_store::source_finite_float_string(value).ok_or(Error::Nonfinite)?;
                out.raw(encoded.as_bytes())?;
            } else {
                if text.strip_prefix('-').unwrap_or(text).len() > 4300 {
                    return Err(Error::SourceIntegerLimit);
                }
                out.raw(if text == "-0" { b"0" } else { text.as_bytes() })?;
            }
        }
        _ => return Err(Error::Syntax),
    }
    Ok(())
}
pub(crate) fn string(raw: &RawValue) -> Result<Option<Cow<'_, str>>, Error> {
    let raw = raw.trim_matches([' ', '\n', '\r', '\t']);
    if !raw.starts_with('"') {
        return Ok(None);
    }
    Ok(Some(vod_span::decode_string(raw)?))
}

/// Source's JSON decoder applies the default integer digit limit even to
/// properties that the DTO later ignores. Scan already validated JSON without
/// allocating a tree or interpreting digits inside strings.
pub(crate) fn validate_source_integer_limits(raw: &RawValue) -> Result<(), Error> {
    let bytes = raw.as_bytes();
    let mut at = 0;
    let mut depth = 0usize;
    while at < bytes.len() {
        if bytes[at] == b'"' {
            at += 1;
            while at < bytes.len() {
                match bytes[at] {
                    b'\\' => at += 2,
                    b'"' => {
                        at += 1;
                        break;
                    }
                    _ => at += 1,
                }
            }
        } else if matches!(bytes[at], b'{' | b'[') {
            depth += 1;
            if depth > 128 {
                return Err(crate::reader::capacity().into());
            }
            at += 1;
        } else if matches!(bytes[at], b'}' | b']') {
            depth = depth.checked_sub(1).ok_or(Error::Syntax)?;
            at += 1;
        } else if matches!(bytes[at], b'-' | b'0'..=b'9') {
            let began = at;
            while at < bytes.len()
                && matches!(bytes[at], b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9')
            {
                at += 1;
            }
            let token = &bytes[began..at];
            if !token.iter().any(|b| matches!(b, b'.' | b'e' | b'E'))
                && token.len() - usize::from(token.first() == Some(&b'-')) > 4300
            {
                return Err(Error::SourceIntegerLimit);
            }
        } else {
            at += 1;
        }
    }
    Ok(())
}

/// Python truthiness over an already validated, borrowed JSON value. This
/// performs no normalization of an emitted value and builds no Value tree.
pub(crate) fn truthy(raw: &RawValue) -> Result<bool, Error> {
    let text = raw.trim_matches([' ', '\n', '\r', '\t']);
    match text {
        "null" | "false" | "\"\"" => Ok(false),
        _ if text.starts_with('[') || text.starts_with('{') => Ok(!text[1..text.len() - 1]
            .trim_matches([' ', '\n', '\r', '\t'])
            .is_empty()),
        _ if text.starts_with('-') || text.as_bytes().first().is_some_and(u8::is_ascii_digit) => {
            if text.bytes().any(|b| matches!(b, b'.' | b'e' | b'E')) {
                Ok(
                    dispatcharr_ingest_core::source_float_literal(text).ok_or(Error::Syntax)?
                        != 0.0,
                )
            } else {
                Ok(text.bytes().any(|b| matches!(b, b'1'..=b'9')))
            }
        }
        _ => Ok(true),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn projected(text: &str, maximum: usize) -> Result<String, Error> {
        let raw = crate::vod_span::parse(text).map_err(|_| Error::Syntax)?;
        let mut out = JsonBytes::new(maximum).map_err(Error::Capacity)?;
        write(&mut out, raw)?;
        Ok(String::from_utf8(out.finish()).unwrap())
    }
    #[test]
    fn streaming_order_spacing_unicode_and_numeric_spelling() {
        assert_eq!(
            projected(r#"{"z":[true,null,"東😀"],"a":-0,"f":1e-6}"#, 1024).unwrap(),
            r#"{"z": [true, null, "\u6771\ud83d\ude00"], "a": 0, "f": 1e-06}"#
        );
        assert_eq!(
            projected(r#"{"a\"b":"x\\y"}"#, 1024).unwrap(),
            r#"{"a\"b": "x\\y"}"#
        );
        assert_eq!(
            projected("[-0.0,1.0,1e16]", 1024).unwrap(),
            "[-0.0, 1.0, 1e+16]"
        );
    }
    #[test]
    fn no_private_number_key_reinterpretation() {
        assert_eq!(
            projected(r#"{"$serde_json::private::Number":"12"}"#, 1024).unwrap(),
            r#"{"$serde_json::private::Number": "12"}"#
        );
        assert_eq!(
            projected(r#"{"$serde_json::private::RawValue":"[1]"}"#, 1024).unwrap(),
            r#"{"$serde_json::private::RawValue": "[1]"}"#
        );
    }
    #[test]
    fn nonfinite_and_capacity_are_distinct_failures() {
        assert!(matches!(projected("1e999", 100), Err(Error::Nonfinite)));
        match projected(r#"["abcdefgh",123]"#, 5).unwrap_err() {
            Error::Capacity(error) => assert!(matches!(error, StoreError::StatusBody(503, _))),
            error => panic!("expected a typed capacity error, got {error:?}"),
        }
        assert!(matches!(
            projected(&"1".repeat(4301), 5000),
            Err(Error::SourceIntegerLimit)
        ));
    }
    #[test]
    fn ignored_integer_limit_is_checked_without_string_false_positives() {
        let text = format!(r#"{{"ignored":{}}}"#, "1".repeat(4301));
        let raw = crate::vod_span::parse(&text).unwrap();
        assert!(matches!(
            validate_source_integer_limits(raw),
            Err(Error::SourceIntegerLimit)
        ));
        let text = format!(
            r#"{{"quoted":"{}","x":-0,"escaped":"a\\\"12"}}"#,
            "1".repeat(4301)
        );
        let raw = crate::vod_span::parse(&text).unwrap();
        assert!(validate_source_integer_limits(raw).is_ok());
    }
    #[test]
    fn json_python_falsey_values() {
        for text in [
            "null", "false", "0", "-0", "0.0", "1e-999", "\"\"", "[ ]", "{ }",
        ] {
            assert!(
                !truthy(crate::vod_span::parse(text).unwrap()).unwrap(),
                "{text}"
            );
        }
        for text in ["true", "1", "-1", "\"0\"", "[null]", "{\"x\":null}"] {
            assert!(
                truthy(crate::vod_span::parse(text).unwrap()).unwrap(),
                "{text}"
            );
        }
    }
}
