//! JSON-line adapter for unchanged upstream fMP4 framing comparisons.
use dispatcharr_fmp4_core::{find_moof_offset, Event, FragmentParser};
use std::io::{self, BufRead};
fn main() {
    for line in io::stdin().lock().lines() {
        let input: serde_json::Value = serde_json::from_str(&line.unwrap()).unwrap();
        let data: Vec<u8> = serde_json::from_value(input["data"].clone()).unwrap();
        if input.get("scan").is_some() {
            println!(
                "{}",
                serde_json::json!(find_moof_offset(
                    &data,
                    input["scan"].as_u64().unwrap() as usize
                )
                .map_or(-1, |offset| offset as i64))
            );
            continue;
        }
        let mut parser = FragmentParser::new(10 * 1024 * 1024, 16 * 1024 * 1024).unwrap();
        let mut output = Vec::new();
        for chunk in data.chunks(input["width"].as_u64().unwrap() as usize) {
            output.extend(parser.push(chunk).unwrap());
        }
        output.extend(parser.finish().unwrap());
        println!(
            "{}",
            serde_json::json!(output
                .into_iter()
                .map(|event| match event {
                    Event::Init(data) => serde_json::json!({"init":data}),
                    Event::Fragment(data) => serde_json::json!({"fragment":data}),
                })
                .collect::<Vec<_>>())
        );
    }
}
