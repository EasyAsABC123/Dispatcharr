//! Line-oriented differential oracle adapter; original author credit retained.
use dispatcharr_ingest_core::{
    parse_extinf, project_entry, stream_hash, validate_playlist_head, with_uploaded_playlist,
    CacheDownload, Entry, Limits, Parser, ProjectionContext, StreamIdentity, UploadLimits,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::io::{self, BufRead};
fn main() {
    let mut cache_case = 0_u64;
    for line in io::stdin().lock().lines() {
        let request: Value = serde_json::from_str(&line.unwrap()).unwrap();
        let result = if request["kind"] == "extinf" {
            serde_json::to_value(
                parse_extinf(request["line"].as_str().unwrap(), &Limits::default()).unwrap(),
            )
            .unwrap()
        } else if request["kind"] == "projection" {
            let entry: Entry = serde_json::from_value(request["entry"].clone()).unwrap();
            let raw = &request["context"];
            let groups = serde_json::from_value(raw["groups"].clone()).unwrap();
            let keys = raw["hash_keys"]
                .as_array()
                .unwrap()
                .iter()
                .map(|key| key.as_str().unwrap())
                .collect::<Vec<_>>();
            let context = ProjectionContext {
                account_id: raw["account_id"].as_i64().unwrap(),
                account_type: raw["account_type"].as_str().unwrap(),
                groups: &groups,
                hash_keys: &keys,
                max_name_characters: raw["max_name_characters"].as_u64().unwrap() as usize,
            };
            match project_entry(&entry, &context).unwrap() {
                None => Value::Null,
                Some(row) => {
                    let number = row.stream_chno;
                    let mut result = serde_json::to_value(row).unwrap();
                    if let Some(number) = number.filter(|number| !number.is_finite()) {
                        result["stream_chno"] = json!(format!(
                            "__float:{}",
                            if number.is_nan() {
                                "nan"
                            } else if number.is_sign_positive() {
                                "inf"
                            } else {
                                "-inf"
                            }
                        ));
                    }
                    result
                }
            }
        } else if request["kind"] == "prepared" {
            let directory = std::env::temp_dir().join(format!(
                "dispatcharr-prepared-oracle-{}-{cache_case}",
                std::process::id()
            ));
            cache_case += 1;
            std::fs::create_dir(&directory).unwrap();
            let path = directory.join("1.json");
            let bytes = request["bytes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_u64().unwrap() as u8)
                .collect::<Vec<_>>();
            let result = dispatcharr_ingest_core::prepare_playlist(
                std::io::Cursor::new(bytes),
                &path,
                dispatcharr_ingest_core::PreparationLimits::default(),
            )
            .unwrap();
            let cached: Value =
                serde_json::from_reader(std::fs::File::open(path).unwrap()).unwrap();
            std::fs::remove_dir_all(directory).unwrap();
            json!({"cache":cached,"groups":result.groups,"entries":result.entries})
        } else if request["kind"] == "upload" {
            let directory = std::env::temp_dir().join(format!(
                "dispatcharr-upload-oracle-{}-{cache_case}",
                std::process::id()
            ));
            cache_case += 1;
            std::fs::create_dir(&directory).unwrap();
            let suffix = request["suffix"].as_str().unwrap();
            assert!(suffix
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '.'));
            let path = directory.join(format!("fixture{suffix}"));
            let bytes = request["bytes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_u64().unwrap() as u8)
                .collect::<Vec<_>>();
            std::fs::write(&path, bytes).unwrap();
            let result = with_uploaded_playlist(&path, UploadLimits::default(), |reader| {
                let entries = dispatcharr_ingest_core::Entries::new(reader, Limits::default())
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| {
                        io::Error::new(io::ErrorKind::InvalidData, format!("{error:?}"))
                    })?;
                Ok(entries)
            });
            let result = match result {
                Ok(entries) => json!({"success":true,"entries":entries}),
                Err(_) => json!({"success":false}),
            };
            std::fs::remove_dir_all(directory).unwrap();
            result
        } else if request["kind"] == "cache" {
            let directory = std::env::temp_dir().join(format!(
                "dispatcharr-cache-oracle-{}-{cache_case}",
                std::process::id()
            ));
            cache_case += 1;
            std::fs::create_dir(&directory).unwrap();
            let path = directory.join("1.m3u");
            std::fs::write(&path, b"previous cache").unwrap();
            let mut writer = CacheDownload::begin(&path, 1024 * 1024).unwrap();
            for chunk in request["chunks"].as_array().unwrap() {
                let bytes = chunk
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_u64().unwrap() as u8)
                    .collect::<Vec<_>>();
                writer.write_chunk(&bytes).unwrap();
            }
            let result = writer.finish(
                request["status"].as_u64().unwrap() as u16,
                request["url"].as_str().unwrap(),
            );
            let (passed, error, lines) = match result {
                Ok(stats) => (true, None, Some(stats.lines)),
                Err(error) => (false, Some(error.to_string()), None),
            };
            let bytes = std::fs::read(&path).unwrap();
            let result = json!({"success":passed,"error":error,"lines":lines,"cache_sha256":format!("{:x}",Sha256::digest(bytes)),"temporary_exists":directory.join("1.m3u.tmp").exists()});
            std::fs::remove_dir_all(directory).unwrap();
            result
        } else if request["kind"] == "validation" {
            let bytes = request["bytes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_u64().unwrap() as u8)
                .collect::<Vec<_>>();
            let result = validate_playlist_head(&bytes, request["url"].as_str().unwrap());
            json!({"valid":result.valid,"error":result.error})
        } else if request["kind"] == "hash" {
            let identity = &request["identity"];
            let identity = StreamIdentity {
                name: identity["name"].as_str().unwrap(),
                url: identity["url"].as_str().unwrap(),
                tvg_id: identity["tvg_id"].as_str().unwrap(),
                m3u_id: identity["m3u_id"].as_i64(),
                group: identity["group"].as_str(),
                account_type: identity["account_type"].as_str().unwrap(),
                stream_id: identity["stream_id"].as_i64(),
            };
            let keys = request["keys"]
                .as_array()
                .unwrap()
                .iter()
                .map(|key| key.as_str().unwrap())
                .collect::<Vec<_>>();
            json!(stream_hash(&identity, &keys))
        } else {
            let mut parser = Parser::new(Limits::default()).unwrap();
            let mut entries = Vec::new();
            for line in request["lines"].as_array().unwrap() {
                if let Some(entry) = parser.feed(line.as_str().unwrap()).unwrap() {
                    entries.push(entry);
                }
            }
            parser.finish();
            json!(entries)
        };
        println!("{result}");
    }
}
