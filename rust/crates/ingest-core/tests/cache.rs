use dispatcharr_ingest_core::{CacheDownload, CacheError};
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "dispatcharr-cache-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn cache(&self) -> PathBuf {
        self.0.join("1.m3u")
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn validates_before_atomic_replacement_and_keeps_binary_line_count() {
    let fixture = Fixture::new();
    let cache = fixture.cache();
    fs::write(&cache, b"old").unwrap();
    let mut writer = CacheDownload::begin(&cache, 100).unwrap();
    for chunk in [
        b"\xff#EXT".as_slice(),
        b"M3U\r\nhttp://fixture\r\n",
        b"tail",
    ] {
        writer.write_chunk(chunk).unwrap();
    }
    assert_eq!(fs::read(&cache).unwrap(), b"old");
    let stats = writer.finish(200, "http://fixture/list").unwrap();
    assert_eq!(stats.lines, 3);
    assert_eq!(stats.bytes, fs::metadata(&cache).unwrap().len());
    assert!(!fixture.0.join("1.m3u.tmp").exists());
}

#[test]
fn invalid_empty_cancelled_and_oversize_downloads_preserve_previous_cache() {
    let fixture = Fixture::new();
    let cache = fixture.cache();
    fs::write(&cache, b"old").unwrap();
    let mut writer = CacheDownload::begin(&cache, 100).unwrap();
    writer.write_chunk(b"<html>error").unwrap();
    assert!(writer
        .finish(200, "fixture")
        .unwrap_err()
        .to_string()
        .contains("HTML page"));
    assert!(CacheDownload::begin(&cache, 100)
        .unwrap()
        .finish(206, "fixture")
        .unwrap_err()
        .to_string()
        .contains("HTTP 206"));
    let mut writer = CacheDownload::begin(&cache, 10).unwrap();
    writer.write_chunk(b"#EXTM3U").unwrap();
    assert!(matches!(
        writer.write_chunk(b"large"),
        Err(CacheError::Capacity)
    ));
    assert!(writer.finish(200, "fixture").is_err());
    let mut writer = CacheDownload::begin(&cache, 100).unwrap();
    writer.write_chunk(b"#EXTM3U").unwrap();
    drop(writer);
    assert_eq!(fs::read(&cache).unwrap(), b"old");
    assert!(!fixture.0.join("1.m3u.tmp").exists());
}

#[test]
fn preserves_unknown_staging_owner_and_rejects_late_valid_header() {
    let fixture = Fixture::new();
    let cache = fixture.cache();
    let temporary = fixture.0.join("1.m3u.tmp");
    fs::write(&temporary, b"another writer").unwrap();
    assert!(CacheDownload::begin(&cache, 100).is_err());
    assert_eq!(fs::read(&temporary).unwrap(), b"another writer");
    fs::remove_file(&temporary).unwrap();
    let mut writer = CacheDownload::begin(&cache, 40000).unwrap();
    for _ in 0..4 {
        writer.write_chunk(&[b' '; 8192]).unwrap();
    }
    writer.write_chunk(b"#EXTM3U").unwrap();
    assert!(writer.finish(200, "fixture").is_err());
    assert!(!cache.exists());
}
