//! Original CoreSettings.get_epg_history_policy coercion and bounds.
//! All application and algorithm credit belongs to Dispatcharr's authors.
use num_bigint::BigInt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::str::FromStr;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpgHistoryPolicy {
    pub min_days: i64,
    pub max_days: i64,
    pub default_prev_days: i64,
    pub default_days: i64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EpgPolicyError {
    Overflow,
}
fn clamp(value: Option<&Value>, default: i64, lo: i64, hi: i64) -> Result<i64, EpgPolicyError> {
    let raw = match value {
        Some(Value::Bool(value)) => return Ok(i64::from(*value).clamp(lo, hi)),
        Some(Value::Number(value)) => {
            let text = value.to_string();
            if text.bytes().any(|b| matches!(b, b'.' | b'e' | b'E')) {
                let value = value
                    .as_f64()
                    .filter(|v| v.is_finite())
                    .ok_or(EpgPolicyError::Overflow)?;
                return Ok(if value.trunc() <= lo as f64 {
                    lo
                } else if value.trunc() >= hi as f64 {
                    hi
                } else {
                    value.trunc() as i64
                });
            }
            Some(text)
        }
        Some(Value::String(value)) => dispatcharr_ingest_core::source_integer_literal(value),
        _ => None,
    };
    let Some(raw) = raw else { return Ok(default) };
    let Ok(number) = BigInt::from_str(&raw) else {
        return Ok(default);
    };
    Ok(if number < BigInt::from(lo) {
        lo
    } else if number > BigInt::from(hi) {
        hi
    } else {
        number
            .to_string()
            .parse()
            .expect("bounded original day value")
    })
}
/// Source int coercion catches TypeError/ValueError, but not float overflow.
pub fn source_epg_history_policy(settings: &Value) -> Result<EpgHistoryPolicy, EpgPolicyError> {
    let max_days = clamp(settings.get("epg_history_max_days"), 30, 0, 365)?;
    let min_days = clamp(settings.get("epg_history_min_days"), 1, 0, 365)?.min(max_days);
    Ok(EpgHistoryPolicy {
        min_days,
        max_days,
        default_prev_days: clamp(settings.get("epg_default_prev_days"), 0, 0, max_days)?,
        default_days: clamp(settings.get("epg_default_days"), 0, 0, 365)?,
    })
}
