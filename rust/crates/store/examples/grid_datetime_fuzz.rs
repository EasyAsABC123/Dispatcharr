//! Read-only parser differential harness; original Dispatcharr/Django credit.
use serde_json::{json, Value};
use std::{
    fs::File,
    io::{BufReader, BufWriter},
};
fn main() {
    let path = std::env::args_os().nth(1).expect("Source corpus path");
    let corpus: Value = serde_json::from_reader(BufReader::new(File::open(path).unwrap())).unwrap();
    let mut results = Vec::new();
    for case in corpus["cases"].as_array().expect("actual Source cases") {
        let raw = case["raw"].as_str().unwrap();
        let name = case["name"].as_str().unwrap();
        let actual = match dispatcharr_store::grid_parse_datetime(raw, name) {
            Ok(value) => json!({"value":value.source_isoformat()}),
            Err(error) => json!({"error":error.class,"message":error.message}),
        };
        results.push(json!({"raw":raw,"name":name,"actual":actual}));
    }
    serde_json::to_writer(
        BufWriter::new(std::io::stdout().lock()),
        &json!({"cases":results}),
    )
    .unwrap();
}
