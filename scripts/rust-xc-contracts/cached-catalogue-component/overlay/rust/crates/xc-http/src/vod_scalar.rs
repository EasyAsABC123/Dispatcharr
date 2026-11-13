//! Borrowed Source Python scalar str()/float() for XC info fields.
//! Composite repr and nonfinite values remain explicit unqualified grammar.
//! Original Dispatcharr/CPython authors retain algorithm credit.
use crate::vod_span::{self, RawValue};
use crate::{vod_json, vod_lists::Error};
use std::borrow::Cow;

pub(crate) fn string(raw: &RawValue) -> Result<Cow<'_, str>, Error> {
    if let Some(value) = vod_json::string(raw)? {
        return Ok(value);
    }
    let text = raw.trim_matches([' ', '\n', '\r', '\t']);
    match text {
        "null" => Ok(Cow::Borrowed("None")),
        "true" => Ok(Cow::Borrowed("True")),
        "false" => Ok(Cow::Borrowed("False")),
        "-0" => Ok(Cow::Borrowed("0")),
        _ if text.starts_with(['{', '[']) => Err(Error::Unsupported),
        _ if !text.contains(['.', 'e', 'E']) => {
            if text.trim_start_matches('-').len() > 4300 {
                return Err(Error::Source);
            }
            Ok(Cow::Borrowed(text))
        }
        _ => {
            let number =
                dispatcharr_ingest_core::source_float_literal(text).ok_or(Error::Source)?;
            dispatcharr_store::source_finite_float_string(number)
                .map(Cow::Owned)
                .ok_or(Error::Unsupported)
        }
    }
}
pub(crate) fn float(raw: &RawValue) -> Result<f64, Error> {
    let text = raw.trim_matches([' ', '\n', '\r', '\t']);
    let value = match text {
        "true" => 1.0,
        "false" => 0.0,
        "null" => return Err(Error::Source),
        _ if text.starts_with(['{', '[']) => return Err(Error::Source),
        _ => {
            let value = vod_json::string(raw)?;
            let value = value.as_deref().unwrap_or(text);
            dispatcharr_ingest_core::source_float_literal(value).ok_or(Error::Source)?
        }
    };
    if value.is_finite() {
        Ok(value)
    } else {
        Err(Error::Unsupported)
    }
}
pub(crate) fn duration(seconds: Option<i32>) -> String {
    let seconds = i64::from(seconds.unwrap_or(0));
    format!(
        "{:02}:{:02}:{:02}",
        seconds.div_euclid(3600),
        seconds.rem_euclid(3600) / 60,
        seconds.rem_euclid(60)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    fn raw(s: &str) -> &RawValue {
        vod_span::parse(s).unwrap()
    }
    #[test]
    fn scalar_representations_preserve_source_types() {
        for (input, expected) in [
            ("null", "None"),
            ("true", "True"),
            ("false", "False"),
            ("-0", "0"),
            ("123", "123"),
            ("1.0", "1.0"),
            (r#""crew""#, "crew"),
        ] {
            assert_eq!(string(raw(input)).unwrap(), expected);
        }
        assert_eq!(float(raw("true")).unwrap(), 1.0);
        assert_eq!(float(raw(r#""7.50""#)).unwrap(), 7.5);
        assert!(matches!(float(raw(r#""NaN""#)), Err(Error::Unsupported)));
        assert!(matches!(string(raw("[]")), Err(Error::Unsupported)));
    }
    #[test]
    fn integer_duration_uses_python_floor_division() {
        for (seconds, text) in [
            (None, "00:00:00"),
            (Some(120), "00:02:00"),
            (Some(-1), "-1:59:59"),
            (Some(360000), "100:00:00"),
        ] {
            assert_eq!(duration(seconds), text);
        }
    }
}
