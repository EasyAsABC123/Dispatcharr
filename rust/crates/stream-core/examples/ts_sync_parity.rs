//! Test-only line protocol for the pinned Python find_ts_sync helper.
use dispatcharr_stream_core::sync::find_ts_sync;
use std::io::{self, BufRead};

fn main() {
    for line in io::stdin().lock().lines() {
        let line = line.unwrap();
        let bytes = line
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect::<Vec<_>>();
        println!(
            "{}",
            find_ts_sync(&bytes).map_or(-1, |offset| offset as i64)
        );
    }
}
