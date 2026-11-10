//! Original channels.utils.resolve_epg_output_window, credited to Dispatcharr.
use dispatcharr_store::EpgHistoryPolicy;
use num_bigint::BigInt;
use serde_json::Value;
use std::str::FromStr;

#[derive(Debug, PartialEq, Eq)]
pub struct Window {
    pub forward_days: i64,
    pub prev_days: i64,
}
#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    Overflow,
    Capacity,
}
fn day(raw: Option<&Value>, upper: i64) -> Result<Option<i64>, Error> {
    let number = match raw {
        Some(Value::Bool(value)) => return Ok(Some(i64::from(*value).min(upper))),
        Some(Value::Number(value)) => {
            let text = value.to_string();
            if text.contains(['.', 'e', 'E']) {
                let value = value
                    .as_f64()
                    .filter(|v| v.is_finite())
                    .ok_or(Error::Overflow)?;
                return Ok(Some(if value <= 0.0 {
                    0
                } else if value >= upper as f64 {
                    upper
                } else {
                    value.trunc() as i64
                }));
            }
            Some(text)
        }
        Some(Value::String(value)) => dispatcharr_ingest_core::source_integer_literal(value),
        _ => None,
    };
    let Some(number) = number.and_then(|v| BigInt::from_str(&v).ok()) else {
        return Ok(None);
    };
    Ok(Some(if number < BigInt::from(0) {
        0
    } else if number > BigInt::from(upper) {
        upper
    } else {
        number
            .to_string()
            .parse()
            .expect("bounded Source day count")
    }))
}
/// QueryDict.get uses the final value; an explicitly invalid URL value means zero.
/// Policy defaults are already normalized by the shared Source history helper.
pub fn resolve(
    params: &[(String, String)],
    custom: Option<&Value>,
    policy: EpgHistoryPolicy,
) -> Result<Window, Error> {
    if !(0..=365).contains(&policy.max_days) || params.len() > 4096 {
        return Err(Error::Capacity);
    }
    let parsed = match custom {
        Some(Value::String(value)) => {
            if value.len() > 1024 * 1024 {
                return Err(Error::Capacity);
            }
            serde_json::from_str::<Value>(value).ok()
        }
        _ => None,
    };
    let props = parsed.as_ref().or(custom).filter(|v| v.is_object());
    let explicit = |url_key: &str, property: &str, upper| -> Result<Option<i64>, Error> {
        if let Some((_, value)) = params.iter().rev().find(|(key, _)| key == url_key) {
            return Ok(Some(
                day(Some(&Value::String(value.clone())), upper)?.unwrap_or(0),
            ));
        }
        day(props.and_then(|v| v.get(property)), upper)
    };
    Ok(Window {
        forward_days: explicit("days", "epg_days", 365)?.unwrap_or(policy.default_days),
        prev_days: explicit("prev_days", "epg_prev_days", policy.max_days)?
            .unwrap_or(policy.default_prev_days),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn original_export_window_vectors() {
        let corpus: Value =
            serde_json::from_str(include_str!("../datasets/source-window.json")).unwrap();
        let cases = corpus["cases"].as_array().unwrap();
        assert_eq!(cases.len(), 1664);
        for (index, case) in cases.iter().enumerate() {
            let params = case["params"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| {
                    (
                        v[0].as_str().unwrap().to_owned(),
                        v[1].as_str().unwrap().to_owned(),
                    )
                })
                .collect::<Vec<_>>();
            let policy: EpgHistoryPolicy = serde_json::from_value(case["policy"].clone()).unwrap();
            let got = resolve(&params, Some(&case["custom_properties"]), policy);
            if case.get("error").is_some() {
                assert!(got.is_err(), "case {index}");
                continue;
            }
            let expected = &case["expected"];
            assert_eq!(
                got,
                Ok(Window {
                    forward_days: expected["forward_days"].as_i64().unwrap(),
                    prev_days: expected["prev_days"].as_i64().unwrap()
                }),
                "case {index}"
            );
        }
    }
}
