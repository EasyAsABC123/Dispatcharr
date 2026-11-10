//! Fixture-only strict root comparison; original authors retain application credit.
use base64::{engine::general_purpose::STANDARD, Engine};
use dispatcharr_xmltv_parser::{parse_xmltv_element, StrictElementError};
use serde_json::{json, Value};
use std::{
    io::{self, BufRead},
    sync::atomic::AtomicBool,
};
fn main() {
    for line in io::stdin().lock().lines() {
        let line = line.unwrap();
        assert!(
            line.len() <= 8 * 1024 * 1024,
            "controlled fixture line limit"
        );
        let value: Value = serde_json::from_str(&line).unwrap();
        let input = STANDARD
            .decode(value["input_base64"].as_str().unwrap())
            .unwrap();
        let result = match parse_xmltv_element(&input, &AtomicBool::new(false)) {
            Ok(tree) => json!({"kind":"ok","tree":tree}),
            Err(StrictElementError::Syntax) => json!({"kind":"syntax"}),
            Err(error) => json!({"kind":"resource","error":format!("{error:?}")}),
        };
        println!("{result}");
    }
}
