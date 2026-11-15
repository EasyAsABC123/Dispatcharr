//! CoreSettings.get_dvr_output_profile_id + _dvr_capture_url, normal runtime only.
//! Original Dispatcharr authors retain full application/algorithm credit.
use serde_json::Value;

/// The actual cached DVR group supplies this value. Invalid ValueError/TypeError
/// operands become None exactly as Source's getter; unsupported/nonfinite numeric
/// domains stay explicit errors rather than silently switching a recording to raw.
pub(crate) fn configured_id(value: Option<&Value>) -> Result<Option<String>, &'static str> {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(None);
    };
    let integer = match value {
        Value::Bool(value) => Some(if *value { "1" } else { "0" }.to_owned()),
        Value::String(value) => dispatcharr_ingest_core::source_integer_literal(value),
        Value::Number(value) => {
            let raw = value.to_string();
            if !raw.contains(['.', 'e', 'E']) {
                Some(
                    dispatcharr_ingest_core::source_integer_literal(&raw)
                        .ok_or("DVR numeric profile integer capacity")?,
                )
            } else {
                let number = value
                    .as_f64()
                    .filter(|number| number.is_finite())
                    .ok_or("DVR nonfinite profile integer")?;
                // Python int(float) truncates the actual binary64 value. Fixed
                // zero precision prints that integral float exactly, including
                // values outside machine-int range; it is not a saturating cast.
                let text = format!("{:.0}", number.trunc());
                Some(if text == "-0" { "0".into() } else { text })
            }
        }
        _ => None,
    };
    Ok(integer)
}
/// Source's URL helper tests integer truthiness, so 0 is omitted while negative
/// or very large canonical IDs remain in the actual request query. The ordinary
/// authenticated live resolver decides whether such an active profile exists.
pub(crate) fn capture_id(value: Option<&Value>) -> Result<Option<String>, &'static str> {
    Ok(configured_id(value)?.filter(|value| value != "0"))
}
pub(crate) fn valid_canonical(value: &str) -> bool {
    value != "0"
        && value.len() <= 4301
        && dispatcharr_ingest_core::source_integer_literal(value).as_deref() == Some(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unchanged_source_getter_and_capture_truthiness_vectors() {
        let data: Value = serde_json::from_str(include_str!(
            "../datasets/source-dvr-output-profile-getter-host.json"
        ))
        .unwrap();
        for row in data["cases"].as_array().unwrap() {
            let value: Value = serde_json::from_str(row["json"].as_str().unwrap()).unwrap();
            let expected = row["getter"].as_str().map(str::to_owned);
            assert_eq!(
                configured_id(Some(&value)).unwrap(),
                expected,
                "{}",
                row["label"]
            );
            assert_eq!(
                capture_id(Some(&value)).unwrap(),
                row["capture"].as_str().map(str::to_owned),
                "{}",
                row["label"]
            );
        }
        assert_eq!(configured_id(None).unwrap(), None);
        let nonfinite: Value = serde_json::from_str("1e400").unwrap();
        assert!(configured_id(Some(&nonfinite)).is_err());
        assert!(valid_canonical("-7"));
        assert!(!valid_canonical("007"));
        assert!(!valid_canonical("7&credential=other"));
    }
}
