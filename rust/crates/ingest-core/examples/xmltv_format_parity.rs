//! Draft original XMLTV format component replay; original authors retain credit.
use base64::Engine;
use dispatcharr_ingest_core::detect_xmltv_format;
use serde_json::{json, Value};
use std::io::{self, BufRead};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    for line in io::stdin().lock().lines() {
        let line = line?;
        let value: Value = serde_json::from_str(&line)?;
        let content = value["content_base64"]
            .as_str()
            .map(|encoded| base64::engine::general_purpose::STANDARD.decode(encoded))
            .transpose()?;
        let result = detect_xmltv_format(
            value["path"].as_str(),
            content.as_deref(),
            value["mime_hint"].as_str(),
        );
        println!(
            "{}",
            json!([result.kind, result.compressed, result.extension])
        );
    }
    Ok(())
}
