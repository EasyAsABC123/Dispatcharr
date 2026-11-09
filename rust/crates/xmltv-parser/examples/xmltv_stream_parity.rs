//! Fixture-only collector; production sinks must separately bound retention.
use base64::{engine::general_purpose::STANDARD, Engine};
use dispatcharr_ingest_core::XmltvReader;
use dispatcharr_xmltv_parser::parse_xmltv_reader;
use serde_json::{json, Value};
use std::{
    io::{self, BufRead, Read},
    sync::atomic::AtomicBool,
};
struct Fragmented<R> {
    source: R,
    chunk: usize,
}
impl<R: Read> Read for Fragmented<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let len = output.len().min(self.chunk);
        self.source.read(&mut output[..len])
    }
}
fn main() {
    let dir = std::env::temp_dir().join(format!(
        "dispatcharr-native-xmltv-fixture-{}",
        std::process::id()
    ));
    std::fs::create_dir(&dir).unwrap();
    let file = dir.join("guide.xml");
    for line in io::stdin().lock().lines() {
        let value: Value = serde_json::from_str(&line.unwrap()).unwrap();
        std::fs::write(
            &file,
            STANDARD
                .decode(value["input_base64"].as_str().unwrap())
                .unwrap(),
        )
        .unwrap();
        let input = Fragmented {
            source: XmltvReader::open(&file).unwrap(),
            chunk: value["chunk"].as_u64().unwrap() as usize,
        };
        let mut events = Vec::new();
        let result = parse_xmltv_reader(input, &AtomicBool::new(false), |node| {
            events.push(node);
            Ok::<_, ()>(())
        });
        println!(
            "{}",
            json!({"events":events,"error":result.err().map(|_|"Capacity")})
        );
    }
    std::fs::remove_file(file).unwrap();
    std::fs::remove_dir(dir).unwrap();
}
