//! Private existing-formatter/stat conversion differential peer. No exports.
use serde_json::{json, Value};
use std::io::Read;
#[test]
#[ignore]
fn native_scanner_float_peer() {
    let mut input = String::new();
    std::io::stdin().read_to_string(&mut input).unwrap();
    let input: Value = serde_json::from_str(&input).unwrap();
    let directory = std::env::temp_dir().join(format!(
        "rust-owned-scanner-float-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&directory).unwrap();
    let mut rows = Vec::new();
    for (index, source) in input.as_array().unwrap().iter().enumerate() {
        let bits = u64::from_str_radix(
            source["stat_before"]["ieee754_big_endian_hex"]
                .as_str()
                .unwrap(),
            16,
        )
        .unwrap();
        let value = f64::from_bits(bits);
        let native_format = crate::json_compat::float_string(value);
        let ns = source["requested_ns"].as_u64().unwrap();
        let file = if let Some(path) = source["same_file_path"].as_str() {
            assert!(path.starts_with("/data/logos/rust-owned-"));
            std::path::PathBuf::from(path)
        } else {
            let file = directory.join(index.to_string());
            let handle = std::fs::File::create(&file).unwrap();
            let timestamp = std::time::UNIX_EPOCH + std::time::Duration::from_nanos(ns);
            handle
                .set_times(std::fs::FileTimes::new().set_modified(timestamp))
                .unwrap();
            file
        };
        let observed = std::fs::metadata(&file)
            .unwrap()
            .modified()
            .unwrap()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap();
        let joined = observed.as_secs_f64();
        let split = observed.as_secs() as f64 + observed.subsec_nanos() as f64 * 1e-9;
        rows.push(json!({"source_bits":format!("{bits:016x}"),"formatted":native_format,"round_even_format":serde_json::Number::from_f64(value).unwrap().to_string(),"bounded_epoch_format":crate::source_scanner_epoch_string(value),"native_stat_ns":observed.as_nanos().to_string(),"native_joined_bits":format!("{:016x}",joined.to_bits()),"native_split_bits":format!("{:016x}",split.to_bits())}));
    }
    std::fs::remove_dir_all(&directory).unwrap();
    println!(
        "NATIVE_SCANNER_FLOAT_JSON={}",
        serde_json::to_string(&rows).unwrap()
    );
}
