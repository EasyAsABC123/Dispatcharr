//! Immutable original decision vectors; all credit to Dispatcharr contributors.
use dispatcharr_timeshift_core::*;
use serde_json::{json, Value};

#[test]
fn original_busy_range_control_vectors() {
    let report: Value = serde_json::from_str(include_str!(
        "../../../../scripts/rust-timeshift-contracts/python-busy-control-vectors.json"
    ))
    .unwrap();
    for case in report["cases"].as_array().unwrap() {
        let input = &case["input"];
        let actual = match case["function"].as_str().unwrap() {
            "displace" => json!(should_displace_busy_playback(
                input["range_header"].as_str(),
                input["content_length"].as_i64().map(i128::from),
                input["busy_serving_range"].as_str()
            )),
            "preempt_plain" => json!(should_preempt_plain_reconnect(
                input["range_header"].as_str(),
                input["pool_exists"].as_bool().unwrap(),
                input["pool_busy"].as_bool().unwrap(),
                input["pool_media_id"].as_str(),
                input["media_id"].as_str().unwrap()
            )),
            "cap_open_range" => json!(cap_open_ended_range(
                input[0].as_str(),
                input[1].as_i64().map(i128::from)
            )
            .unwrap()),
            _ => panic!("unknown source vector"),
        };
        assert_eq!(actual, case["output"], "{case}");
    }
    assert_eq!(report["vector_count"], 448);
}

#[test]
fn overflowing_open_range_is_explicit_and_eof_threshold_boundary_matches_source() {
    assert!(cap_open_ended_range(Some(&format!("bytes={}-", i128::MAX)), Some(2)).is_err());
    assert!(is_near_eof_probe(Some("bytes=100000000-"), None));
    assert!(!is_near_eof_probe(Some("bytes=99999999-"), None));
}
