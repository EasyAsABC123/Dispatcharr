//! Fixture-only bridge for unchanged original XMLTV preparation comparisons.
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::SecondsFormat;
use dispatcharr_ingest_core::{parse_xmltv_time, xmltv_file_is_complete, XmltvReader};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    io::{self, BufRead, Read},
    path::PathBuf,
};

struct Fixture(PathBuf);
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::temp_dir().join(format!(
        "dispatcharr-xmltv-parity-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    std::fs::create_dir(&root)?;
    let fixture = Fixture(root);
    for line in io::stdin().lock().lines() {
        let input: Value = serde_json::from_str(&line?)?;
        let output = match input["kind"].as_str().unwrap() {
            "time" => match parse_xmltv_time(input["value"].as_str().unwrap()) {
                Ok(value) => {
                    json!({"ok":true,"iso":value.to_rfc3339_opts(SecondsFormat::Secs,false)})
                }
                Err(error) => json!({"ok":false,"error":error.source_error_name()}),
            },
            "missing" => json!(xmltv_file_is_complete(&fixture.0.join("missing.xml"))),
            "complete" | "prefix" => {
                let bytes = STANDARD.decode(input["input_base64"].as_str().unwrap())?;
                assert!(bytes.len() <= 65536);
                let path = fixture.0.join("guide.xml");
                std::fs::write(&path, bytes)?;
                if input["kind"] == "complete" {
                    json!(xmltv_file_is_complete(&path))
                } else {
                    let mut reader = XmltvReader::open(&path)?;
                    let mut output = Vec::new();
                    let chunk = input["chunk"].as_i64().unwrap();
                    if chunk < 0 {
                        reader.read_to_end(&mut output)?;
                    } else {
                        assert!(chunk <= 65536);
                        let mut buffer = vec![0; chunk as usize];
                        loop {
                            let n = reader.read(&mut buffer)?;
                            if n == 0 {
                                break;
                            }
                            output.extend_from_slice(&buffer[..n]);
                        }
                    }
                    json!(format!("{:x}", Sha256::digest(output)))
                }
            }
            _ => panic!("unknown fixture case"),
        };
        println!("{}", serde_json::to_string(&output)?);
    }
    Ok(())
}
