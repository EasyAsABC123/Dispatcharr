//! RPC adapter for unchanged upstream ChannelStatus differential checks.
use dispatcharr_stream_http::project_status_at;
use std::{
    collections::BTreeMap,
    io::{self, BufRead},
};
fn main() {
    for line in io::stdin().lock().lines() {
        let line = line.unwrap();
        let input: serde_json::Value = serde_json::from_str(&line).unwrap();
        let metadata: BTreeMap<String, String> =
            serde_json::from_value(input["metadata"].clone()).unwrap();
        let clients = serde_json::from_value(input["clients"].clone()).unwrap();
        let output = project_status_at(
            input["channel_id"].as_str().unwrap(),
            &metadata,
            input["index"].as_u64().unwrap(),
            clients,
            input["detailed"].as_bool().unwrap(),
            input["now"].as_f64().unwrap(),
        );
        println!("{}", output);
    }
}
