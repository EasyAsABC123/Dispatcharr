//! Disk-backed full Groups result over the existing parsed-cache reader.
//! Original Dispatcharr/serde authors retain all algorithm/application credit.
use crate::transport::Error;
use dispatcharr_ingest_core::{CacheReadLimits, ValidatedCache};
use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};
use uuid::Uuid;
/// Only a real validated cache builder constructs this payload. Its private FD
/// and unlink guard outlive observer cancellation in the enclosing task owner.
pub(crate) struct GroupsResultFile {
    file: File,
    path: PathBuf,
    bytes: u64,
    _owner: Arc<dyn Send + Sync>,
}
impl Drop for GroupsResultFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}
struct Writer {
    file: BufWriter<File>,
    bytes: u64,
    limit: u64,
}
impl Write for Writer {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        let total = self
            .bytes
            .checked_add(data.len() as u64)
            .filter(|n| *n <= self.limit)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::FileTooLarge,
                    "M3U full result file capacity",
                )
            })?;
        self.file.write_all(data)?;
        self.bytes = total;
        Ok(data.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}
impl GroupsResultFile {
    pub(crate) fn from_actual_cache(
        cache: &Path,
        directory: &Path,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Self, Error> {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
        let source_file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(cache)
            .map_err(|_| Error::Backend)?;
        let metadata = source_file.metadata().map_err(|_| Error::Backend)?;
        if !metadata.is_file() || metadata.nlink() != 1 {
            return Err(Error::UnsupportedTask);
        }
        let limits = CacheReadLimits::default();
        let limit = limits.bytes;
        let mut source =
            ValidatedCache::from_file(source_file, limits).map_err(|_| Error::Protocol)?;
        let path = directory.join(format!(".m3u-result-{}.json", Uuid::new_v4()));
        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&path)
            .map_err(|_| Error::Backend)?;
        let mut cleanup = Cleanup(Some(path.clone()));
        let mut writer = Writer {
            file: BufWriter::with_capacity(65536, file),
            bytes: 0,
            limit,
        };
        writer.write_all(b"[[").map_err(|_| Error::Backend)?;
        let mut count = 0u64;
        source
            .for_each(|entry| {
                if count > 0 {
                    writer.write_all(b",").map_err(|_| "result file write")?;
                }
                serde_json::to_writer(&mut writer, &entry).map_err(|_| "result entry encode")?;
                count += 1;
                Ok(())
            })
            .map_err(|_| Error::Protocol)?;
        if count != source.entries {
            return Err(Error::Corrupt);
        }
        writer.write_all(b"],{").map_err(|_| Error::Backend)?;
        for (index, group) in source.groups.iter().enumerate() {
            if index > 0 {
                writer.write_all(b",").map_err(|_| Error::Backend)?;
            }
            serde_json::to_writer(&mut writer, &group.name).map_err(|_| Error::Protocol)?;
            writer.write_all(b":").map_err(|_| Error::Backend)?;
            serde_json::to_writer(&mut writer, &group.properties).map_err(|_| Error::Protocol)?;
        }
        writer.write_all(b"}]").map_err(|_| Error::Backend)?;
        writer.flush().map_err(|_| Error::Backend)?;
        let bytes = writer.bytes;
        let mut file = writer.file.into_inner().map_err(|_| Error::Backend)?;
        file.seek(SeekFrom::Start(0)).map_err(|_| Error::Backend)?;
        cleanup.0 = None;
        Ok(Self {
            file,
            path,
            bytes,
            _owner: owner,
        })
    }
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }
    pub(crate) fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, Error> {
        let mut bytes = vec![0; 65536];
        let n = self.file.read(&mut bytes).map_err(|_| Error::Backend)?;
        if n == 0 {
            return Ok(None);
        }
        bytes.truncate(n);
        Ok(Some(bytes))
    }
}
struct Cleanup(Option<PathBuf>);
impl Drop for Cleanup {
    fn drop(&mut self) {
        if let Some(path) = &self.0 {
            let _ = std::fs::remove_file(path);
        }
    }
}

#[cfg(test)]
mod controls {
    use super::*;
    use dispatcharr_ingest_core::Entry;
    use std::collections::BTreeMap;
    struct Directory(PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    #[test]
    fn descriptor_cache_keeps_opened_identity_when_path_is_replaced() {
        let directory = Directory(std::env::temp_dir().join(format!("m3u-fd-{}", Uuid::new_v4())));
        std::fs::create_dir(&directory.0).unwrap();
        let input = directory.0.join("cache.json");
        std::fs::write(
            &input,
            br#"{"extinf_data":[],"groups":{"Default Group":{}}}"#,
        )
        .unwrap();
        let opened = File::open(&input).unwrap();
        let replacement = directory.0.join("replacement.json");
        std::fs::write(&replacement, b"not JSON").unwrap();
        std::fs::rename(replacement, &input).unwrap();
        let cache = ValidatedCache::from_file(opened, CacheReadLimits::default()).unwrap();
        assert_eq!(cache.entries, 0);
        assert!(ValidatedCache::open(&input, CacheReadLimits::default()).is_err());
    }
    #[test]
    fn result_cache_rejects_symlink_and_hardlink_without_staging_output() {
        use std::os::unix::fs::symlink;
        let directory =
            Directory(std::env::temp_dir().join(format!("m3u-links-{}", Uuid::new_v4())));
        std::fs::create_dir(&directory.0).unwrap();
        let input = directory.0.join("cache.json");
        let link = directory.0.join("link.json");
        std::fs::write(
            &input,
            br#"{"extinf_data":[],"groups":{"Default Group":{}}}"#,
        )
        .unwrap();
        symlink(&input, &link).unwrap();
        assert!(GroupsResultFile::from_actual_cache(&link, &directory.0, Arc::new(())).is_err());
        std::fs::remove_file(&link).unwrap();
        std::fs::hard_link(&input, &link).unwrap();
        assert!(GroupsResultFile::from_actual_cache(&input, &directory.0, Arc::new(())).is_err());
        assert_eq!(std::fs::read_dir(&directory.0).unwrap().count(), 2);
    }
    #[test]
    fn full_groups_result_exceeds_old_metadata_cap_without_truncating_catalog_and_retires_file() {
        let directory =
            Directory(std::env::temp_dir().join(format!("m3u-result-{}", Uuid::new_v4())));
        std::fs::create_dir(&directory.0).unwrap();
        let input = directory.0.join("actual-parsed.json");
        let mut writer = std::io::BufWriter::new(File::create(&input).unwrap());
        writer.write_all(b"{\"extinf_data\":[").unwrap();
        for index in 0..5000 {
            if index > 0 {
                writer.write_all(b",").unwrap()
            }
            let entry = Entry {
                name: format!("Stream {index}"),
                display_name: format!("Stream {index}"),
                attributes: BTreeMap::from([
                    ("group-title".into(), "News".into()),
                    ("tvg-id".into(), format!("owned-{index}")),
                ]),
                url: Some(format!("http://owned.invalid/live/{index}")),
                vlc_opts: None,
            };
            serde_json::to_writer(&mut writer, &entry).unwrap();
        }
        writer
            .write_all(b"],\"groups\":{\"Default Group\":{},\"News\":{}}}")
            .unwrap();
        writer.flush().unwrap();
        drop(writer);
        let mut result =
            GroupsResultFile::from_actual_cache(&input, &directory.0, Arc::new(())).unwrap();
        assert!(result.bytes() > 262144);
        let path = result.path.clone();
        let mut data = Vec::new();
        while let Some(chunk) = result.next_chunk().unwrap() {
            assert!(chunk.len() <= 65536);
            data.extend_from_slice(&chunk)
        }
        assert_eq!(data.len() as u64, result.bytes());
        let value: serde_json::Value = serde_json::from_slice(&data).unwrap();
        assert_eq!(value[0].as_array().unwrap().len(), 5000);
        assert_eq!(value[0][4999]["url"], "http://owned.invalid/live/4999");
        assert_eq!(value[1], serde_json::json!({"Default Group":{},"News":{}}));
        drop(result);
        assert!(!path.exists());
        assert!(input.exists());
    }
    #[test]
    fn malformed_cache_never_creates_an_acknowledgeable_result_or_changes_input() {
        let directory =
            Directory(std::env::temp_dir().join(format!("m3u-result-invalid-{}", Uuid::new_v4())));
        std::fs::create_dir(&directory.0).unwrap();
        let input = directory.0.join("actual-parsed.json");
        let raw = b"{\"extinf_data\":[";
        std::fs::write(&input, raw).unwrap();
        assert!(GroupsResultFile::from_actual_cache(&input, &directory.0, Arc::new(())).is_err());
        assert_eq!(std::fs::read(&input).unwrap(), raw);
        assert_eq!(std::fs::read_dir(&directory.0).unwrap().count(), 1);
    }
}
