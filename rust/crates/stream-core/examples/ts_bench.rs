//! Component-only comparison partner for scripts/rust-parity/benchmark.py.
use dispatcharr_stream_core::ts::{DiscontinuityMarker, PACKET_SIZE};
use std::hint::black_box;
use std::time::Instant;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let input = std::fs::read(&args[1]).expect("fixture");
    assert_eq!(input.len() % PACKET_SIZE, 0);
    let iterations: u64 = args[2].parse().expect("iterations");
    assert!(iterations > 0);
    let mut output = Vec::new();
    let mut stamped = 0u64;
    let began = Instant::now();
    for _ in 0..iterations {
        output = input.clone();
        let mut marker = DiscontinuityMarker::default();
        marker.arm();
        for packet in output.as_chunks_mut::<PACKET_SIZE>().0 {
            stamped += u64::from(marker.stamp(packet));
        }
        black_box(&output);
    }
    let wall_ns = began.elapsed().as_nanos();
    let checksum: u64 = output.iter().map(|byte| u64::from(*byte)).sum();
    let peak_kib = std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find(|line| line.starts_with("VmHWM:"))
                .and_then(|line| line.split_whitespace().nth(1))
                .and_then(|value| value.parse::<u64>().ok())
        });
    println!("{{\"implementation\":\"rust\",\"wall_ns\":{wall_ns},\"checksum\":{checksum},\"stamped\":{stamped},\"peak_rss_kib\":{}}}",
        peak_kib.map_or("null".to_string(), |value| value.to_string()));
}
