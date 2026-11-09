//! Resource fixture: stream into a sink which retains no programme records.
use dispatcharr_xmltv_parser::{parse_xmltv_file, XmltvError};
use serde_json::json;
use std::{path::Path, sync::atomic::AtomicBool};
fn main() {
    let path = std::env::args().nth(1).expect("XMLTV file path");
    let mut events = 0;
    let result = parse_xmltv_file(Path::new(&path), &AtomicBool::new(false), |_| {
        events += 1;
        Ok::<_, ()>(())
    });
    let peak = std::fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmHWM:")
                .and_then(|value| value.split_whitespace().next())
                .and_then(|value| value.parse::<u64>().ok())
        })
        .unwrap();
    let capacity = matches!(result, Err(XmltvError::Capacity));
    println!(
        "{}",
        json!({"events":events,"capacity":capacity,"peak_rss_kib":peak})
    );
    if result.is_err() && !capacity {
        std::process::exit(1);
    }
}
