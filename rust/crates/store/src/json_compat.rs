//! JSON sizing/equality rules used by the original Python app-data helpers.
use num_bigint::BigInt;
use serde_json::{Number, Value};
use std::str::FromStr;

fn float_string(value: f64) -> String {
    if value == 0.0 {
        return if value.is_sign_negative() {
            "-0.0".into()
        } else {
            "0.0".into()
        };
    }
    if value.abs() < 1e-4 || value.abs() >= 1e16 {
        let raw = format!("{value:e}");
        let (mantissa, exponent) = raw.split_once('e').unwrap();
        let exponent: i32 = exponent.parse().unwrap();
        return format!("{mantissa}e{exponent:+03}");
    }
    let mut raw = value.to_string();
    if !raw.contains('.') {
        raw.push_str(".0");
    }
    raw
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
