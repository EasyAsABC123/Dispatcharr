//! Line protocol for differential verification against the existing Python helper.
//! Commands: packet HEX, batch HEX, reset. Output: HEX or ERR_ALIGNMENT.
use dispatcharr_stream_core::ts::{DiscontinuityMarker, PACKET_SIZE};
use std::io::{self, BufRead};

fn main() {
    let mut generation = DiscontinuityMarker::default();
    generation.arm();
    for line in io::stdin().lock().lines() {
        let line = line.expect("stdin read failed");
        if line == "reset" {
            generation.arm();
            println!("RESET");
            continue;
        }
        let (command, hex) = line.split_once(' ').expect("command HEX expected");
        assert!(hex.len() % 2 == 0, "even hex length required");
        let mut data: Vec<u8> = hex
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| {
                u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).expect("invalid hex")
            })
            .collect();
        match command {
            "packet" => {
                // Python's public single-packet helper is a no-op for malformed lengths.
                if data.len() == PACKET_SIZE {
                    let mut marker = DiscontinuityMarker::default();
                    marker.arm();
                    marker.stamp(data.as_mut_slice().try_into().unwrap());
                }
            }
            "batch" => {
                if !data.len().is_multiple_of(PACKET_SIZE) {
                    println!("ERR_ALIGNMENT");
                    continue;
                }
                for packet in data.as_chunks_mut::<PACKET_SIZE>().0 {
                    generation.stamp(packet);
                }
            }
            _ => panic!("unknown command"),
        }
        let encoded: String = data.iter().map(|byte| format!("{byte:02x}")).collect();
        println!("{encoded}");
    }
}
