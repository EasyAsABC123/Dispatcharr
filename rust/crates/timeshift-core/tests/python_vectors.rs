//! Differential vectors captured from immutable original Python helpers.
use dispatcharr_timeshift_core::*;
use serde_json::{json, Value};
fn python_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(value) => Some(value.clone()),
        Value::Bool(true) => Some("True".into()),
        Value::Bool(false) => Some("False".into()),
        other => Some(other.to_string()),
    }
}
#[test]
fn original_python_helper_vectors() {
    let report: Value = serde_json::from_str(include_str!(
        "../../../../scripts/rust-timeshift-contracts/python-helper-goldens.json"
    ))
    .unwrap();
    let mut count = 0;
    for case in report["cases"].as_array().unwrap() {
        let input = &case["input"];
        let string = python_string(input);
        let value = match case["function"].as_str().unwrap() {
            "normalize_catchup_timestamp_input" => {
                json!(string.as_deref().and_then(normalize_timestamp))
            }
            "parse_catchup_timestamp" => json!(string
                .as_deref()
                .and_then(parse_timestamp)
                .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S").to_string())),
            "client_duration_to_window" => {
                json!(string.as_deref().and_then(client_duration_window))
            }
            "convert_timestamp_to_provider_tz" => json!(provider_timestamp(
                input[0].as_str().unwrap(),
                input[1].as_str()
            )),
            "programme_age_days" => json!(programme_age_days(
                input.as_str().unwrap(),
                parse_timestamp("2026-11-09T12:00:00Z").unwrap()
            )),
            "build_timeshift_candidate_urls" => json!(candidate_urls(
                &ProviderCredentials {
                    server_url: "http://provider.invalid/base/".into(),
                    username: "<fixture-user>".into(),
                    password: "<fixture-password>".into()
                },
                "123",
                input[0].as_str().unwrap(),
                65
            )),
            name => {
                let shape = match name {
                    "format_timestamp_as_colon_dash" => TimestampShape::ColonDash,
                    "format_timestamp_as_colon_seconds" => TimestampShape::ColonSeconds,
                    "format_timestamp_as_underscore" => TimestampShape::Underscore,
                    "format_timestamp_as_sql_datetime" => TimestampShape::Sql,
                    _ => panic!("unknown captured helper"),
                };
                match string.as_deref().and_then(parse_timestamp) {
                    Some(_) => json!(format_timestamp(string.as_deref().unwrap(), shape)),
                    None => input.clone(),
                }
            }
        };
        assert_eq!(
            json!({"value":value}),
            case["output"],
            "helper {} input {}",
            case["function"],
            input
        );
        count += 1;
    }
    assert_eq!(count, 207);
}
#[test]
fn credentials_are_encoded_as_utf8_path_segments_and_invalid_integer_hints_are_rejected() {
    let credentials = ProviderCredentials {
        server_url: "http://provider.invalid/".into(),
        username: "a b/c?".into(),
        password: "é+&".into(),
    };
    let url = path_url(&credentials, "1", "2026-01-01:00-00", 10);
    assert!(url.contains("/a%20b%2Fc%3F/%C3%A9%2B%26/"));
    for value in ["_1", "1_", "1__0", "+_1", "-1", "1.0"] {
        assert_eq!(client_duration_window(value), None);
    }
}

#[test]
fn invalid_calendar_and_leap_second_remain_invalid_after_shape_normalization() {
    assert_eq!(
        normalize_timestamp("2023-02-29:25-61").as_deref(),
        Some("2023-02-29T25:61:00")
    );
    for value in [
        "2023-02-29:25-61",
        "2026-01-01T23:59:60Z",
        "2026-01-01:23-59-60",
        "0000-01-01T00:00:00Z",
    ] {
        assert!(parse_timestamp(value).is_none());
    }
}
