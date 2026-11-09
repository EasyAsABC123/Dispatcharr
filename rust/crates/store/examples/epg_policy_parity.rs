//! Fixture-only original policy comparator.
use dispatcharr_store::source_epg_history_policy;
use serde_json::{json, Value};
use std::io::{self, BufRead};
fn main() {
    for line in io::stdin().lock().lines() {
        let value: Value = serde_json::from_str(&line.unwrap()).unwrap();
        let result = match source_epg_history_policy(&value) {
            Ok(value) => json!({"ok":true,"value":value}),
            Err(_) => json!({"ok":false,"error":"OverflowError"}),
        };
        println!("{result}");
    }
}
