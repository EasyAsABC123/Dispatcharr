use dispatcharr_ingest_core::{prepare_playlist, PreparationLimits};
use std::{
    fs,
    io::{self, BufRead, Cursor, Read},
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "dispatcharr-prepared-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn cache(&self) -> PathBuf {
        self.0.join("1.json")
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
struct BrokenTrailer(Cursor<Vec<u8>>);
impl Read for BrokenTrailer {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if self.0.position() == self.0.get_ref().len() as u64 {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "late CRC failure",
            ))
        } else {
            self.0.read(output)
        }
    }
}
impl BufRead for BrokenTrailer {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        if self.0.position() == self.0.get_ref().len() as u64 {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "late CRC failure",
            ))
        } else {
            self.0.fill_buf()
        }
    }
    fn consume(&mut self, count: usize) {
        self.0.consume(count)
    }
}
const PLAYLIST:&[u8]=b"#EXTM3U\n#EXTINF:-1 group-title=\"Group B\",One\nhttp://fixture/one\n#EXTINF:-1 group-title=\"Group A\",Two\nhttp://fixture/two\n";
#[test]
fn prepares_source_json_and_preserves_first_seen_group_order() {
    let fixture = Fixture::new();
    let path = fixture.cache();
    let result =
        prepare_playlist(Cursor::new(PLAYLIST), &path, PreparationLimits::default()).unwrap();
    assert_eq!(result.entries, 2);
    assert_eq!(result.groups, ["Default Group", "Group B", "Group A"]);
    assert_eq!(result.bytes, fs::metadata(&path).unwrap().len());
    let json: serde_json::Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    assert_eq!(json["extinf_data"].as_array().unwrap().len(), 2);
    assert_eq!(json["groups"].as_object().unwrap().len(), 3);
}
#[test]
fn late_trailer_failure_never_replaces_previous_parsed_cache() {
    let fixture = Fixture::new();
    let path = fixture.cache();
    fs::write(&path, b"previous cache").unwrap();
    assert!(prepare_playlist(
        BrokenTrailer(Cursor::new(PLAYLIST.to_vec())),
        &path,
        PreparationLimits::default()
    )
    .is_err());
    assert_eq!(fs::read(&path).unwrap(), b"previous cache");
    assert!(!fixture.0.join("1.json.tmp").exists());
}
#[test]
fn bounds_entry_group_and_serialized_disk_capacity_without_partial_promotion() {
    let fixture = Fixture::new();
    let path = fixture.cache();
    fs::write(&path, b"previous cache").unwrap();
    for limits in [
        PreparationLimits {
            entries: 1,
            ..PreparationLimits::default()
        },
        PreparationLimits {
            groups: 1,
            ..PreparationLimits::default()
        },
        PreparationLimits {
            cache_bytes: 100,
            ..PreparationLimits::default()
        },
        PreparationLimits {
            group_bytes: 13,
            ..PreparationLimits::default()
        },
    ] {
        assert!(prepare_playlist(Cursor::new(PLAYLIST), &path, limits).is_err());
        assert_eq!(fs::read(&path).unwrap(), b"previous cache");
        assert!(!fixture.0.join("1.json.tmp").exists());
    }
}
