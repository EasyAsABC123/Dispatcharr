use dispatcharr_ingest_core::{
    prepare_playlist, CacheReadLimits, PreparationLimits, ValidatedCache,
};
use std::{
    fs,
    io::Cursor,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "dispatcharr-cache-reader-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn path(&self) -> PathBuf {
        self.0.join("1.json")
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
const PLAYLIST:&[u8]=b"#EXTM3U\n#EXTINF:-1 group-title=\"B\",One\nhttp://fixture/one\n#EXTINF:-1 group-title=\"A\",Two\nhttp://fixture/two\n";
#[test]
fn reads_prepared_cache_in_original_entry_and_first_seen_group_order() {
    let fixture = Fixture::new();
    let path = fixture.path();
    prepare_playlist(Cursor::new(PLAYLIST), &path, PreparationLimits::default()).unwrap();
    let mut cache = ValidatedCache::open(&path, CacheReadLimits::default()).unwrap();
    assert_eq!(cache.entries, 2);
    assert_eq!(
        cache
            .groups
            .iter()
            .map(|group| group.name.as_str())
            .collect::<Vec<_>>(),
        ["Default Group", "B", "A"]
    );
    let mut names = Vec::new();
    cache
        .for_each(|entry| {
            names.push(entry.name);
            Ok(())
        })
        .unwrap();
    assert_eq!(names, ["One", "Two"]);
}
#[test]
fn rejects_late_corruption_and_invalid_shapes_before_any_consumer_side_effect() {
    let fixture = Fixture::new();
    let path = fixture.path();
    for bytes in [
        b"{\"extinf_data\":[],\"groups\":{}} trailing".as_slice(),
        b"{\"extinf_data\":[]}",
        b"{\"groups\":{}}",
        b"{\"extinf_data\":{},\"groups\":{}}",
        b"{\"extinf_data\":[],\"groups\":{\"bad\":null}}",
    ] {
        fs::write(&path, bytes).unwrap();
        assert!(ValidatedCache::open(&path, CacheReadLimits::default()).is_err());
    }
}
#[test]
fn limits_fields_entry_counts_group_counts_and_total_file_size() {
    let fixture = Fixture::new();
    let path = fixture.path();
    prepare_playlist(Cursor::new(PLAYLIST), &path, PreparationLimits::default()).unwrap();
    for limits in [
        CacheReadLimits {
            bytes: 10,
            ..CacheReadLimits::default()
        },
        CacheReadLimits {
            entry_bytes: 8,
            ..CacheReadLimits::default()
        },
        CacheReadLimits {
            entries: 1,
            ..CacheReadLimits::default()
        },
        CacheReadLimits {
            groups: 2,
            ..CacheReadLimits::default()
        },
        CacheReadLimits {
            group_bytes: 4,
            ..CacheReadLimits::default()
        },
    ] {
        assert!(ValidatedCache::open(&path, limits).is_err());
    }
    let row = serde_json::json!({"attributes":{},"name":"x".repeat(200000),"display_name":"","url":"http://fixture"});
    fs::write(
        &path,
        serde_json::to_vec(&serde_json::json!({"extinf_data":[row],"groups":{}})).unwrap(),
    )
    .unwrap();
    let error = ValidatedCache::open(
        &path,
        CacheReadLimits {
            entry_bytes: 1024,
            ..CacheReadLimits::default()
        },
    );
    assert!(error.is_err());
}
#[test]
fn atomic_replacement_does_not_change_validated_file_descriptor_or_retries() {
    let fixture = Fixture::new();
    let path = fixture.path();
    prepare_playlist(Cursor::new(PLAYLIST), &path, PreparationLimits::default()).unwrap();
    let mut cache = ValidatedCache::open(&path, CacheReadLimits::default()).unwrap();
    prepare_playlist(
        Cursor::new(b"#EXTM3U\n"),
        &path,
        PreparationLimits::default(),
    )
    .unwrap();
    let mut attempted = 0;
    assert!(cache
        .for_each(|_| {
            attempted += 1;
            Err("stop consumer".into())
        })
        .is_err());
    assert_eq!(attempted, 1);
    let mut names = Vec::new();
    cache
        .for_each(|entry| {
            names.push(entry.name);
            Ok(())
        })
        .unwrap();
    assert_eq!(names, ["One", "Two"]);
    assert_eq!(
        ValidatedCache::open(&path, CacheReadLimits::default())
            .unwrap()
            .entries,
        0
    );
}
#[test]
fn duplicate_group_names_keep_first_position_and_last_value_like_python_json() {
    let fixture = Fixture::new();
    let path = fixture.path();
    fs::write(
        &path,
        br#"{"extinf_data":[],"groups":{"B":{"xc_id":1},"A":{},"B":{"xc_id":2}}}"#,
    )
    .unwrap();
    let cache = ValidatedCache::open(&path, CacheReadLimits::default()).unwrap();
    assert_eq!(
        cache
            .groups
            .iter()
            .map(|group| group.name.as_str())
            .collect::<Vec<_>>(),
        ["B", "A"]
    );
    assert_eq!(cache.groups[0].properties["xc_id"], 2);
}
