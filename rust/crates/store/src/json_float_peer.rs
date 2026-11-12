//! Fixture-only finite float differential peer. Original authors retain credit.
use std::io::Read;
#[test]
#[ignore]
fn actual_finite_float_peer() {
    let mut raw = String::new();
    std::io::stdin().read_to_string(&mut raw).unwrap();
    assert!(raw.len() <= 16 * 1024 * 1024);
    let input: serde_json::Value = serde_json::from_str(&raw).unwrap();
    let input = input["rows"].as_array().unwrap();
    assert!(!input.is_empty() && input.len() <= 10000);
    let mut rows = Vec::new();
    for row in input {
        let bits = row["bits"].as_str().unwrap();
        let value = f64::from_bits(u64::from_str_radix(bits, 16).unwrap());
        assert!(value.is_finite());
        let number = serde_json::Number::from_f64(value).unwrap();
        let compact = crate::json_compat::compact_json_bytes(
            &serde_json::json!({"float":value,"label":"é𝄞"}),
        )
        .unwrap();
        let compact_size = compact.len();
        rows.push(serde_json::json!({"bits":bits,"repr":crate::json_compat::float_string(value),"repr_length":crate::json_compat::python_number_length(&number),"compact_size":compact_size,"compact":String::from_utf8(compact).unwrap()}));
    }
    println!(
        "FINITE_FLOAT_PEER_JSON={}",
        serde_json::to_string(&rows).unwrap()
    );
}
