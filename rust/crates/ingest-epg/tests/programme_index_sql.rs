//! Original raw byte-index maps versus bounded scanner; task/SQL/file leases
//! remain separate integration gates. Full credit belongs to original authors.
use base64::{engine::general_purpose::STANDARD, Engine};
use dispatcharr_ingest_epg::{
    programme_index::{scan_file, IndexLimits},
    ImportError,
};
use serde_json::Value;
use std::{
    fs::File,
    io::{Seek, SeekFrom, Write},
    path::PathBuf,
    sync::atomic::AtomicBool,
};
struct Files(Vec<PathBuf>);
impl Drop for Files {
    fn drop(&mut self) {
        for path in &self.0 {
            let _ = std::fs::remove_file(path);
        }
    }
}
#[tokio::test]
#[ignore = "requires captured original byte-index corpus"]
async fn original_byte_index_maps_and_bounded_file_cases() {
    assert_eq!(
        std::env::var("EPG_INDEX_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("EPG_INDEX_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    let mut compared = 0;
    for (index, case) in fixture["cases"].as_array().unwrap().iter().enumerate() {
        let name = case["name"].as_str().unwrap();
        if matches!(name, "missing-file" | "file-held" | "defer-exhausted") {
            continue;
        }
        let path = std::env::temp_dir().join(format!(
            "rust-programme-index-native-{}-{index}.xml",
            std::process::id()
        ));
        let _files = Files(vec![path.clone()]);
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        for part in case["spec"].as_array().unwrap() {
            if let Some(encoded) = part["bytes_base64"].as_str() {
                file.write_all(&STANDARD.decode(encoded).unwrap()).unwrap();
            } else {
                let mut left = part["length"].as_u64().unwrap();
                let buffer = vec![part["repeat"].as_u64().unwrap() as u8; 65536];
                while left > 0 {
                    let count = left.min(buffer.len() as u64) as usize;
                    file.write_all(&buffer[..count]).unwrap();
                    left -= count as u64;
                }
            }
        }
        file.flush().unwrap();
        assert_eq!(
            file.metadata().unwrap().len(),
            case["input_bytes"].as_u64().unwrap()
        );
        // Scanner resets even an already-used descriptor, matching a fresh
        // original open without reopening a possibly replaced filesystem path.
        file.seek(SeekFrom::End(0)).unwrap();
        let actual = scan_file(file, IndexLimits::default(), &AtomicBool::new(false)).unwrap();
        assert_eq!(
            actual.source_value(),
            case["after"]["data"],
            "original index {name}"
        );
        compared += 1;
    }
    assert_eq!(compared, 21);
    let path = std::env::temp_dir().join(format!(
        "rust-programme-index-physical-{}.xml",
        std::process::id()
    ));
    let replacement = path.with_extension("replacement");
    let _files = Files(vec![path.clone(), replacement.clone()]);
    std::fs::write(&path, b"<programme channel='a'/><programme channel='b'/>").unwrap();
    let result = scan_file(
        File::open(&path).unwrap(),
        IndexLimits {
            max_keys: 1,
            ..IndexLimits::default()
        },
        &AtomicBool::new(false),
    );
    assert!(matches!(
        result,
        Err(ImportError::Capacity("programme-index keys"))
    ));
    assert!(matches!(
        scan_file(
            File::open(&path).unwrap(),
            IndexLimits::default(),
            &AtomicBool::new(true)
        ),
        Err(ImportError::Cancelled)
    ));
    let original = File::open(&path).unwrap();
    std::fs::write(&replacement, b"<programme channel='replacement'/>").unwrap();
    std::fs::rename(&replacement, &path).unwrap();
    let old = scan_file(original, IndexLimits::default(), &AtomicBool::new(false)).unwrap();
    assert!(old.channels.contains_key("a"));
    assert!(!old.channels.contains_key("replacement"));
    let result = scan_file(
        File::open(&path).unwrap(),
        IndexLimits {
            file_bytes: 1,
            ..IndexLimits::default()
        },
        &AtomicBool::new(false),
    );
    assert!(matches!(
        result,
        Err(ImportError::Capacity("programme-index file"))
    ));
    println!("EPG_INDEX_PURE_SOURCE_CASES={compared};PURE_GUARDS=4;TASK_SQL_LEASE_VERIFIED=false");
}
