//! Fixture-only driver for original XMLTV metadata projection.
use dispatcharr_ingest_core::{extract_xmltv_properties, XmltvElement, XmltvMetadataError};
use serde_json::json;
use std::io::{self, BufRead};
fn main() {
    for line in io::stdin().lock().lines() {
        let line = line.unwrap();
        let node: XmltvElement = serde_json::from_str(&line).unwrap();
        let result = match extract_xmltv_properties(&node) {
            Ok(value) => json!({"ok":true,"value":value}),
            Err(error) => json!({"ok":false,"error":match error {
                XmltvMetadataError::Integer=>"ValueError",XmltvMetadataError::Capacity=>"Capacity",
            }}),
        };
        println!("{result}");
    }
}
