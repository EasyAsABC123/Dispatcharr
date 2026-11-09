//! A synchronous retention/fanout microbenchmark, not an HTTP/media pipeline benchmark.
//! cargo run --release -p dispatcharr-stream-core --example fanout_bench -- [iterations] [chunk_bytes]
use dispatcharr_stream_core::{MediaBuffer, Read};
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn run(viewers: usize, iterations: u64, chunk_bytes: usize) -> (u128, usize, u64) {
    let mut buffer = MediaBuffer::new(
        chunk_bytes.checked_mul(16).unwrap(),
        Duration::from_secs(30),
    )
    .unwrap();
    let started = Instant::now();
    let mut checksum = 0u64;
    for iteration in 0..iterations {
        // One new owned input allocation per write; all viewers share its bytes.
        let payload: Arc<[u8]> = vec![(iteration % 251) as u8; chunk_bytes].into();
        let sequence = buffer.push(payload, Instant::now()).unwrap();
        for _ in 0..viewers {
            let Read::Chunk(chunk) = buffer.read(sequence) else {
                panic!("missing current chunk")
            };
            checksum = checksum.wrapping_add(black_box(chunk.data[0]) as u64);
            black_box(chunk);
        }
    }
    (
        started.elapsed().as_nanos(),
        buffer.retained_bytes(),
        checksum,
    )
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let iterations = args.get(1).map_or(10_000, |s| {
        s.parse::<u64>().expect("iterations must be an integer")
    });
    let chunk_bytes = args.get(2).map_or(65_536, |s| {
        s.parse::<usize>().expect("chunk_bytes must be an integer")
    });
    assert!(
        iterations > 0 && chunk_bytes > 0,
        "workload sizes must be positive"
    );
    for viewers in [1, 10, 100] {
        black_box(run(viewers, 1_000.min(iterations), chunk_bytes));
        for sample in 1..=3 {
            let (wall_ns, retained_bytes, checksum) = run(viewers, iterations, chunk_bytes);
            println!("{{\"schema_version\":1,\"benchmark\":\"stream_core_fanout\",\"implementation\":\"rust\",\"scope\":\"microbenchmark\",\"viewers\":{viewers},\"iterations\":{iterations},\"chunk_bytes\":{chunk_bytes},\"sample\":{sample},\"wall_ns\":{wall_ns},\"retained_payload_bytes\":{retained_bytes},\"checksum\":{checksum}}}");
        }
    }
}
