//! Fixture-only exact original loop projection comparator.
use dispatcharr_ingest_core::{project_xmltv_programme, XmltvElement};
use serde_json::json;
use std::io::{self, BufRead};
fn main() {
    for line in io::stdin().lock().lines() {
        let node: XmltvElement = serde_json::from_str(&line.unwrap()).unwrap();
        let id = match node.attrs.get("channel").map(String::as_str) {
            Some("one") => Some(11),
            Some("ch") => Some(12),
            Some("") => Some(13),
            _ => None,
        };
        let result = match id {
            None => json!({"rows":[],"skipped":1}),
            Some(id) => match project_xmltv_programme(&node, id, 255) {
                Err(_) => json!({"rows":[],"skipped":0}),
                Ok(row) => {
                    json!({"rows":[{"epg_id":row.epg_id,"start_time":row.start_time.to_rfc3339(),
                    "end_time":row.end_time.to_rfc3339(),"title":row.title,"sub_title":row.sub_title,
                    "description":row.description,"tvg_id":row.tvg_id,"custom_properties":row.custom_properties}],"skipped":0})
                }
            },
        };
        println!("{result}");
    }
}
