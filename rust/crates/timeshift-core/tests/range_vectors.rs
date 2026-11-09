//! Immutable source differential archive-header vectors; source quirks retained.
//! All original application credit belongs to upstream Dispatcharr contributors.
use dispatcharr_timeshift_core::*;
use serde_json::{json, Value};
#[test]
fn original_python_range_length_vectors() {
    let report: Value = serde_json::from_str(include_str!(
        "../../../../scripts/rust-timeshift-contracts/python-range-goldens.json"
    ))
    .unwrap();
    for case in report["cases"].as_array().unwrap() {
        let input = &case["input"];
        let actual = match case["function"].as_str().unwrap() {
            "parse_client_range" => json!(parse_client_range(input.as_str())),
            "parse_content_range" => match parse_content_range(input.as_str()) {
                None => Value::Null,
                Some(value) => json!({"start":value.start,"end":value.end,"total":value.total}),
            },
            "representation_length" => {
                json!(representation_length(input[0].as_str(), input[1].as_str()))
            }
            "downstream_headers" => json!(downstream_length_headers(LengthRequest {
                range: input["range_header"].as_str(),
                status: input["status_code"].as_u64().unwrap() as u16,
                representation_length: input["representation_length"].as_i64().map(i128::from),
                upstream_range: input["upstream_content_range"].as_str(),
                upstream_length: input["upstream_content_length"].as_str(),
                streaming: input["streaming"].as_bool().unwrap()
            })
            .unwrap()),
            _ => panic!("unknown source vector"),
        };
        assert_eq!(actual, case["output"], "{case}");
    }
    assert_eq!(report["vector_count"], 246);
}

#[test]
fn provider_integer_arithmetic_overflow_is_explicit_and_never_panics() {
    let actual = downstream_length_headers(LengthRequest {
        range: Some("bytes=0-"),
        status: 206,
        representation_length: Some(i128::MIN),
        upstream_range: None,
        upstream_length: None,
        streaming: true,
    });
    assert_eq!(actual, Err(RangeOverflow));
    let content_range = format!("bytes 0-{}/500", i128::MAX);
    let actual = downstream_length_headers(LengthRequest {
        range: Some("bytes=0-"),
        status: 206,
        representation_length: None,
        upstream_range: Some(&content_range),
        upstream_length: None,
        streaming: true,
    });
    assert_eq!(actual, Err(RangeOverflow));
}
