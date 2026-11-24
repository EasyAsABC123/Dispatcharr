//! JSON sizing/equality rules used by the original Python app-data helpers.
use num_bigint::BigInt;
use serde_json::{Number, Value};
use std::str::FromStr;

pub(crate) enum DefaultJsonError {
    Invalid,
    Capacity,
}

/// Match Python JSON's default number formatting, separators, ASCII escaping,
/// and object member order.
pub(crate) fn default_json_bytes(value: &Value, limit: usize) -> Result<Vec<u8>, DefaultJsonError> {
    let raw = compact_json_bytes(value).map_err(|_| DefaultJsonError::Invalid)?;
    if raw.len() > limit {
        return Err(DefaultJsonError::Capacity);
    }
    let raw = std::str::from_utf8(&raw).map_err(|_| DefaultJsonError::Invalid)?;
    let mut output = Vec::new();
    let (mut quoted, mut escaped) = (false, false);
    for ch in raw.chars() {
        if ch > '\u{7e}' {
            for unit in ch.encode_utf16(&mut [0; 2]) {
                output.extend_from_slice(format!("\\u{unit:04x}").as_bytes());
            }
        } else {
            output.push(ch as u8);
        }
        if quoted {
            if escaped {
                escaped = false
            } else if ch == '\\' {
                escaped = true
            } else if ch == '"' {
                quoted = false
            }
        } else if ch == '"' {
            quoted = true
        } else if matches!(ch, ',' | ':') {
            output.push(b' ')
        }
        if output.len() > limit {
            return Err(DefaultJsonError::Capacity);
        }
    }
    Ok(output)
}

/// Callers validate finite JSON floats before formatting. Keep Python's
/// notation thresholds while using the existing round-to-even shortest digits.
pub(crate) fn float_string(value: f64) -> String {
    if value == 0.0 {
        return if value.is_sign_negative() {
            "-0.0".into()
        } else {
            "0.0".into()
        };
    }
    let raw = Number::from_f64(value)
        .expect("JSON helpers validate finite floats")
        .to_string();
    if (1e-4..1e16).contains(&value.abs()) && raw.contains('.') && !raw.contains('e') {
        return raw;
    }
    let unsigned = raw.strip_prefix('-').unwrap_or(&raw);
    let (coefficient, scale) =
        unsigned
            .split_once('e')
            .map_or((unsigned, 0), |(digits, exponent)| {
                (
                    digits,
                    exponent.parse::<i32>().expect("serde_json float exponent"),
                )
            });
    let before_dot = coefficient.find('.').unwrap_or(coefficient.len());
    let mut digits: String = coefficient.chars().filter(|ch| *ch != '.').collect();
    let leading = digits
        .bytes()
        .position(|byte| byte != b'0')
        .expect("nonzero finite float");
    digits.drain(..leading);
    let exponent = scale + before_dot as i32 - leading as i32 - 1;
    while digits.ends_with('0') {
        digits.pop();
    }
    let mut result = if value.is_sign_negative() {
        "-".to_owned()
    } else {
        String::new()
    };
    if !(-4..16).contains(&exponent) {
        result.push_str(&digits[..1]);
        if digits.len() > 1 {
            result.push('.');
            result.push_str(&digits[1..]);
        }
        result.push_str(&format!("e{exponent:+03}"));
    } else {
        let point = exponent + 1;
        if point <= 0 {
            result.push_str("0.");
            result.extend(std::iter::repeat_n('0', (-point) as usize));
            result.push_str(&digits);
        } else if point as usize >= digits.len() {
            result.push_str(&digits);
            result.extend(std::iter::repeat_n('0', point as usize - digits.len()));
            result.push_str(".0");
        } else {
            let point = point as usize;
            result.push_str(&digits[..point]);
            result.push('.');
            result.push_str(&digits[point..]);
        }
    }
    result
}

/// Original output channel number display. Integral values use Python int
/// spelling while fractional values use Python's shortest round-to-even digits.
pub fn source_channel_number(value: f64) -> Option<String> {
    if !value.is_finite() {
        None
    } else if value == 0.0 {
        Some("0".into())
    } else if value == value.trunc() {
        Some(format!("{value:.0}"))
    } else {
        Some(float_string(value))
    }
}

/// Decode a Django path component once without form-urlencoded plus handling.
pub(crate) fn unquote_path(raw: &str) -> std::borrow::Cow<'_, str> {
    if !raw.contains('%') {
        return std::borrow::Cow::Borrowed(raw);
    }
    let input = format!("v={}", raw.replace('+', "%2B").replace('&', "%26"));
    url::form_urlencoded::parse(input.as_bytes())
        .next()
        .map_or_else(
            || std::borrow::Cow::Borrowed(raw),
            |(_, value)| std::borrow::Cow::Owned(value.into_owned()),
        )
}

fn is_float(number: &Number) -> bool {
    number
        .to_string()
        .bytes()
        .any(|value| matches!(value, b'.' | b'e' | b'E'))
}

pub(crate) fn python_number_length(number: &Number) -> usize {
    if is_float(number) {
        match number.as_f64() {
            Some(value) if value.is_finite() => float_string(value).len(),
            Some(value) if value.is_sign_negative() => 9,
            _ => 8,
        }
    } else {
        BigInt::from_str(&number.to_string()).map_or_else(
            |_| number.to_string().len(),
            |value| value.to_string().len(),
        )
    }
}

pub(crate) fn normalize_input_numbers(value: &mut Value) -> Result<(), ()> {
    let mut pending = vec![value];
    while let Some(value) = pending.pop() {
        match value {
            Value::Number(number) => {
                if is_float(number) {
                    let value = number
                        .as_f64()
                        .filter(|value| value.is_finite())
                        .ok_or(())?;
                    *number = Number::from_f64(value).ok_or(())?;
                } else {
                    let raw = number.to_string();
                    // Python's default integer-string parser rejects more than
                    // 4,300 decimal digits. Keep that bound before BigInt work.
                    if raw.trim_start_matches('-').len() > 4300 {
                        return Err(());
                    }
                    let integer = BigInt::from_str(&raw).map_err(|_| ())?;
                    *number = Number::from_str(&integer.to_string()).map_err(|_| ())?;
                }
            }
            Value::Object(values) => pending.extend(values.values_mut()),
            Value::Array(values) => pending.extend(values.iter_mut()),
            _ => {}
        }
    }
    Ok(())
}

pub(crate) fn compact_json_bytes(value: &Value) -> Result<Vec<u8>, ()> {
    fn write(value: &Value, out: &mut Vec<u8>) -> Result<(), ()> {
        match value {
            Value::Number(number) => {
                let raw = if is_float(number) {
                    float_string(
                        number
                            .as_f64()
                            .filter(|value| value.is_finite())
                            .ok_or(())?,
                    )
                } else {
                    BigInt::from_str(&number.to_string())
                        .map_err(|_| ())?
                        .to_string()
                };
                out.extend(raw.as_bytes());
            }
            Value::Object(values) => {
                out.push(b'{');
                for (index, (key, value)) in values.iter().enumerate() {
                    if index > 0 {
                        out.push(b',');
                    }
                    out.extend(serde_json::to_vec(key).map_err(|_| ())?);
                    out.push(b':');
                    write(value, out)?;
                }
                out.push(b'}');
            }
            Value::Array(values) => {
                out.push(b'[');
                for (index, value) in values.iter().enumerate() {
                    if index > 0 {
                        out.push(b',');
                    }
                    write(value, out)?;
                }
                out.push(b']');
            }
            value => out.extend(serde_json::to_vec(value).map_err(|_| ())?),
        }
        Ok(())
    }
    let mut output = Vec::new();
    write(value, &mut output)?;
    Ok(output)
}

enum Numeric {
    Integer(BigInt),
    Float(f64),
}
fn numeric(value: &Value) -> Option<Numeric> {
    match value {
        Value::Bool(value) => Some(Numeric::Integer(BigInt::from(u8::from(*value)))),
        Value::Number(number) if is_float(number) => Some(Numeric::Float(number.as_f64()?)),
        Value::Number(number) => Some(Numeric::Integer(
            BigInt::from_str(&number.to_string()).ok()?,
        )),
        _ => None,
    }
}
fn float_integer(value: f64) -> Option<BigInt> {
    if !value.is_finite() || value.fract() != 0.0 {
        return None;
    }
    if value == 0.0 {
        return Some(BigInt::from(0));
    }
    let bits = value.to_bits();
    let exponent = ((bits >> 52) & 0x7ff) as i32 - 1023 - 52;
    let mantissa = (bits & ((1u64 << 52) - 1)) | (1u64 << 52);
    let value = if exponent >= 0 {
        BigInt::from(mantissa) << exponent as usize
    } else {
        BigInt::from(mantissa) >> (-exponent) as usize
    };
    Some(if bits >> 63 == 1 { -value } else { value })
}

/// CPython int() uses %.200R: precision truncates the COMPLETE repr by
/// Unicode codepoints, even inside an escape or before its closing quote.
/// Quote selection must inspect the entire input before emitting a prefix.
fn integer_error_repr(raw: &str) -> String {
    let quote = if raw.contains('\'') && !raw.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut result = String::with_capacity(800);
    result.push(quote);
    let mut used = 1;
    for c in raw.chars() {
        let fragment = match c {
            '\\' => "\\\\".into(),
            '\t' => "\\t".into(),
            '\n' => "\\n".into(),
            '\r' => "\\r".into(),
            c if c == quote => format!("\\{c}"),
            c if !crate::python_action::printable(c) => {
                let n = c as u32;
                if n <= 0xff {
                    format!("\\x{n:02x}")
                } else if n <= 0xffff {
                    format!("\\u{n:04x}")
                } else {
                    format!("\\U{n:08x}")
                }
            }
            c => c.to_string(),
        };
        for part in fragment.chars() {
            if used == 200 {
                return result;
            }
            result.push(part);
            used += 1;
        }
    }
    if used < 200 {
        result.push(quote);
    }
    result
}
/// Existing Core parser intentionally exposes Option only. Distinguish its
/// captured valid-decimal >4300 failure without changing any Core callers.
/// Unicode digits reuse the pinned Source15.1 Core decoder, not Rust's newer
/// Unicode numeric predicate. ASCII fastpath avoids allocation perASCII digit.
fn oversized_decimal_digits(raw: &str) -> Option<usize> {
    let raw = raw
        .split('\0')
        .next()
        .unwrap_or(raw)
        .trim_matches(char::is_whitespace);
    let raw = raw.strip_prefix(['+', '-']).unwrap_or(raw);
    let mut digits = 0usize;
    let mut previous_digit = false;
    for c in raw.chars() {
        if c == '_' {
            if !previous_digit {
                return None;
            }
            previous_digit = false;
            continue;
        }
        let digit = c.is_ascii_digit()
            || (!c.is_ascii() && {
                let mut bytes = [0; 4];
                dispatcharr_ingest_core::source_integer_literal(c.encode_utf8(&mut bytes)).is_some()
            });
        if !digit {
            return None;
        }
        digits += 1;
        previous_digit = true;
    }
    (previous_digit && digits > 4300).then_some(digits)
}
pub(crate) fn python_int_string(raw: &str) -> Result<BigInt, String> {
    if let Some(value) =
        dispatcharr_ingest_core::source_integer_literal(raw).and_then(|v| v.parse().ok())
    {
        return Ok(value);
    }
    if let Some(digits) = oversized_decimal_digits(raw) {
        return Err(format!("Exceeds the limit (4300 digits) for integer string conversion: value has {digits} digits; use sys.set_int_max_str_digits() to increase the limit"));
    }
    Err(format!(
        "invalid literal for int() with base 10: {}",
        integer_error_repr(raw)
    ))
}
/// Source int() over JSON scalar values. Container/null type errors retain
/// their Python type names, and floats truncate using their binary value.
pub(crate) fn python_json_int(value: &Value) -> Result<Value, String> {
    let wrong_type = |kind| {
        format!(
            "int() argument must be a string, a bytes-like object or a real number, not '{kind}'"
        )
    };
    let integer = match value {
        Value::Bool(value) => return Ok(Value::Number(Number::from(u8::from(*value)))),
        Value::Number(value) if is_float(value) => {
            let finite = value
                .as_f64()
                .filter(|v| v.is_finite())
                .ok_or_else(|| "cannot convert float infinity to integer".to_owned())?;
            float_integer(finite.trunc()).expect("truncated finite float is integral")
        }
        Value::Number(_) => return Ok(value.clone()),
        Value::String(value) => python_int_string(value)?,
        Value::Null => return Err(wrong_type("NoneType")),
        Value::Array(_) => return Err(wrong_type("list")),
        Value::Object(_) => return Err(wrong_type("dict")),
    };
    Ok(Value::Number(
        Number::from_str(&integer.to_string()).expect("exact JSON integer"),
    ))
}

pub(crate) fn python_equal(left: &Value, right: &Value) -> bool {
    match (numeric(left), numeric(right)) {
        (Some(Numeric::Integer(left)), Some(Numeric::Integer(right))) => return left == right,
        (Some(Numeric::Float(left)), Some(Numeric::Float(right))) => return left == right,
        (Some(Numeric::Integer(integer)), Some(Numeric::Float(float)))
        | (Some(Numeric::Float(float)), Some(Numeric::Integer(integer))) => {
            return float_integer(float).is_some_and(|float| integer == float)
        }
        _ => {}
    }
    match (left, right) {
        (Value::Array(left), Value::Array(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| python_equal(left, right))
        }
        (Value::Object(left), Value::Object(right)) => {
            left.len() == right.len()
                && left.iter().all(|(key, left)| {
                    right
                        .get(key)
                        .is_some_and(|right| python_equal(left, right))
                })
        }
        _ => left == right,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn python_float_formats_and_arbitrary_integer_bytes_are_preserved() {
        assert_eq!(float_string(1e-6), "1e-06");
        assert_eq!(float_string(1e16), "1e+16");
        assert_eq!(float_string(1.0), "1.0");
        assert_eq!(float_string(-0.0), "-0.0");
        assert_eq!(float_string(0.0001), "0.0001");
        let mut value: Value =
            serde_json::from_str(r#"{"a":1e-6,"b":1e16,"c":-0.0,"d":10000000000000000000000001}"#)
                .unwrap();
        normalize_input_numbers(&mut value).unwrap();
        assert_eq!(
            String::from_utf8(compact_json_bytes(&value).unwrap()).unwrap(),
            r#"{"a":1e-06,"b":1e+16,"c":-0.0,"d":10000000000000000000000001}"#
        );
    }
    #[test]
    fn channel_number_preserves_original_fractional_ties_and_integer_spelling() {
        for (value, expected) in [
            (795.6304321289062, "795.6304321289062"),
            (-795.6304321289062, "-795.6304321289062"),
            (2_f64.powi(-25), "2.9802322387695312e-08"),
            (1e-5, "1e-05"),
            (1e-4, "0.0001"),
            (42.0, "42"),
            (-0.0, "0"),
            (1e30, "1000000000000000019884624838656"),
        ] {
            assert_eq!(source_channel_number(value).as_deref(), Some(expected));
        }
        assert_eq!(source_channel_number(f64::INFINITY), None);
        assert_eq!(source_channel_number(f64::NAN), None);
    }
    #[test]
    fn overflow_floats_reject_and_underflow_matches_python_zero() {
        let mut overflow: Value = serde_json::from_str("1e999").unwrap();
        assert!(normalize_input_numbers(&mut overflow).is_err());
        let mut underflow: Value = serde_json::from_str("1e-999").unwrap();
        normalize_input_numbers(&mut underflow).unwrap();
        assert_eq!(compact_json_bytes(&underflow).unwrap(), b"0.0");
    }
    #[test]
    fn numeric_equality_matches_python_without_losing_large_integer_precision() {
        assert!(python_equal(&json!({"x":true}), &json!({"x":1.0})));
        assert!(python_equal(&json!(false), &json!(0)));
        let integer: Value = serde_json::from_str("100000000000000000000000").unwrap();
        assert!(!python_equal(&integer, &json!(1e23)));
        let exact: Value = serde_json::from_str("99999999999999991611392").unwrap();
        assert!(python_equal(&exact, &json!(1e23)));
    }
}
