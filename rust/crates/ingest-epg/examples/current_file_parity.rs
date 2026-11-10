//! Controlled disposable file helper; original Dispatcharr authors retain credit.
use base64::Engine;
use dispatcharr_ingest_epg::current_file::*;
use std::{
    io::{self, BufRead},
    sync::atomic::AtomicBool,
};
fn main() {
    for line in io::stdin().lock().lines() {
        let value: serde_json::Value = serde_json::from_str(&line.unwrap()).unwrap();
        let data = base64::engine::general_purpose::STANDARD
            .decode(value["input_base64"].as_str().unwrap())
            .unwrap();
        let path =
            std::env::temp_dir().join(format!("dispatcharr-current-parity-{}", std::process::id()));
        let mut owned = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        std::io::Write::write_all(&mut owned, &data).unwrap();
        drop(owned);
        let offsets: Vec<u64> = value["offsets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_u64().unwrap())
            .collect();
        let now = value["now"]
            .as_str()
            .unwrap()
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap();
        let controlled = value.get("replace_forward_base64").is_some();
        let mut opens = 0usize;
        let result = if controlled {
            read_current_with_opener(
                || {
                    opens += 1;
                    if opens == 2 {
                        if value["delete_on_second_open"].as_bool().unwrap_or(false) {
                            std::fs::remove_file(&path)?;
                        } else {
                            let replacement = path.with_extension("replacement");
                            let replacement_bytes = base64::engine::general_purpose::STANDARD
                                .decode(value["replace_forward_base64"].as_str().unwrap())
                                .unwrap();
                            let mut owned = std::fs::OpenOptions::new()
                                .write(true)
                                .create_new(true)
                                .open(&replacement)?;
                            std::io::Write::write_all(&mut owned, &replacement_bytes)?;
                            drop(owned);
                            std::fs::rename(&replacement, &path)?;
                        }
                    }
                    std::fs::File::open(&path)
                },
                value["tvg_id"].as_str().unwrap(),
                &offsets,
                value["interleaved"].as_bool().unwrap(),
                now,
                CurrentFileLimits::default(),
                &AtomicBool::new(false),
            )
        } else {
            let file = std::fs::File::open(&path).unwrap();
            std::fs::remove_file(&path).unwrap();
            read_current_file(
                file,
                value["tvg_id"].as_str().unwrap(),
                &offsets,
                value["interleaved"].as_bool().unwrap(),
                now,
                CurrentFileLimits::default(),
                &AtomicBool::new(false),
            )
        };
        if controlled && path.exists() {
            std::fs::remove_file(&path).unwrap();
        }
        let mut result = match result {
            Ok(CurrentFileResult::Found(programme)) => {
                serde_json::json!({"kind":"found","programme":programme.source_value()})
            }
            Ok(CurrentFileResult::Missing) => serde_json::json!({"kind":"missing"}),
            Ok(CurrentFileResult::Timeout) => serde_json::json!({"kind":"timeout"}),
            Err(CurrentFileError::Time(error)) => {
                serde_json::json!({"kind":"error","error":error.source_error_name()})
            }
            Err(CurrentFileError::Runtime(dispatcharr_ingest_epg::ImportError::Io(error)))
                if error.kind() == io::ErrorKind::NotFound =>
            {
                serde_json::json!({"kind":"error","error":"FileNotFoundError"})
            }
            Err(_) => serde_json::json!({"kind":"resource"}),
        };
        if controlled {
            result["opens"] = serde_json::json!(opens);
        }
        println!("{result}");
    }
}
