//! Compare actual pinned Python msgpack fixtures without printing event payloads.
use dispatcharr_realtime::codec;
use serde_json::Value;
use std::io::{self, Read};
fn main() {
    let mut input = String::new();
    io::stdin()
        .take(4 * 1024 * 1024 + 1)
        .read_to_string(&mut input)
        .unwrap();
    assert!(input.len() <= 4 * 1024 * 1024);
    let rows: Vec<Value> = serde_json::from_str(&input).unwrap();
    let mut failed = Vec::new();
    for (index, row) in rows.iter().enumerate() {
        let encoded = codec::encode(&row["value"], 1024 * 1024).unwrap();
        let hex: String = encoded.iter().map(|byte| format!("{byte:02x}")).collect();
        if hex != row["msgpack_hex"].as_str().unwrap()
            || codec::decode(&encoded, 1024 * 1024).unwrap() != row["value"]
        {
            failed.push(index);
        }
    }
    println!(
        "{}",
        serde_json::json!({"vectors":rows.len(),"failed_indices":failed,"passed":failed.is_empty()})
    );
    assert!(failed.is_empty());
}
