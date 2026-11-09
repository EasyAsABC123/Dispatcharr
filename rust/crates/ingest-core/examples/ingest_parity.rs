//! Line-oriented differential oracle adapter; original author credit retained.
use dispatcharr_ingest_core::{
    parse_extinf, project_entry, stream_hash, validate_playlist_head, Entry, Limits, Parser,
    ProjectionContext, StreamIdentity,
};
use serde_json::{json, Value};
use std::io::{self, BufRead};
fn main() {
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
