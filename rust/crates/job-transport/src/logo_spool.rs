//! Bounded private metadata/path spool; no Main/default intake authority.
//! Original Dispatcharr authors retain traversal/algorithm/code/license credit.
//! Source collects all paths before effects; this preserves that phase barrier.
use super::{inspect_into, Error, File as Record, Limits, Owner, Roots, SpoolConfig as Config};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::PathBuf,
};
#[path = "logo_scratch.rs"]
mod scratch;
const MAGIC: &[u8; 8] = b"DARRLG01";
const MAX_DISK: usize = 256 * 1024;
const BATCH: usize = 32;
/// Private file owns the same caller/admission guard until physical close/unlink.
/// Normal/error paths call close in spawn_blocking and await it. Drop is a
/// last-resort cleanup during a blocking-task unwind, never a success witness.
pub(super) struct Disk {
    reader: Option<BufReader<File>>,
    path: Option<PathBuf>,
    count: usize,
    read: usize,
    max_bytes: usize,
    consumed: usize,
    lease: Option<scratch::Lease>,
    _owner: Owner,
}
impl Drop for Disk {
    fn drop(&mut self) {
        drop(self.reader.take());
        if let Some(path) = self.path.take() {
            if let Some(lease) = self.lease.as_mut() {
                let _ = lease.finish(&path, self.max_bytes as u64);
            }
        }
    }
}
impl Disk {
    pub(super) fn count(&self) -> usize {
        self.count
    }
    pub(super) fn enumerate(
        roots: Roots,
        limits: Limits,
        config: Config,
        owner: Owner,
    ) -> Result<Self, Error> {
        if config.max_bytes < 128
            || config.max_bytes > MAX_DISK
            || config.directory.as_os_str().len() > 4096
        {
            return Err(Error::Unsupported);
        }
        let normal: PathBuf = config.directory.components().collect();
        if !config.directory.is_absolute()
            || normal.as_os_str() != config.directory.as_os_str()
            || config.directory.to_str().is_none()
            || config.directory.components().any(|c| {
                matches!(
                    c,
                    std::path::Component::CurDir | std::path::Component::ParentDir
                )
            })
        {
            return Err(Error::Unsupported);
        }
        for root in [&roots.m3u, &roots.epg, &roots.logos] {
            if root.starts_with(&config.directory) || config.directory.starts_with(root) {
                return Err(Error::Unsupported);
            }
        }
        for ancestor in config.directory.ancestors() {
            let meta = std::fs::symlink_metadata(ancestor).map_err(|_| Error::Filesystem)?;
            if !meta.is_dir() || meta.file_type().is_symlink() {
                return Err(Error::Unsupported);
            }
        }
        let mut lease = scratch::Lease::acquire(&config.directory, config.max_bytes as u64)?;
        let (path, file) = lease.create()?;
        let mut spool = Self {
            reader: Some(BufReader::with_capacity(8192, file)),
            path: Some(path),
            count: 0,
            read: 0,
            max_bytes: config.max_bytes,
            consumed: MAGIC.len(),
            lease: Some(lease),
            _owner: owner,
        };
        let own_meta = spool
            .reader
            .as_ref()
            .ok_or(Error::Operational)?
            .get_ref()
            .metadata()
            .map_err(|_| Error::Filesystem)?;
        let file = spool.reader.take().ok_or(Error::Operational)?.into_inner();
        let mut writer = BufWriter::with_capacity(8192, file);
        writer.write_all(MAGIC).map_err(|_| Error::Filesystem)?;
        let mut written = MAGIC.len();
        let count = inspect_into(roots, limits, |record| {
            // A bind-mount alias can expose the newly created spool inside a watch tree
            // despite lexically separate paths. Refuse before any SQL/cache effects.
            #[cfg(unix)]
            {
                let meta =
                    std::fs::symlink_metadata(&record.path).map_err(|_| Error::Filesystem)?;
                if (meta.dev() == own_meta.dev() && meta.ino() == own_meta.ino())
                    || spool
                        .lease
                        .as_ref()
                        .ok_or(Error::Operational)?
                        .aliases(&meta)?
                {
                    return Err(Error::Unsupported);
                }
            }
            let p = record.path.as_bytes();
            let stem = record.stem.as_bytes();
            let extra = 2usize
                .checked_add(p.len())
                .and_then(|v| v.checked_add(2 + stem.len() + 1 + 8))
                .ok_or(Error::Unsupported)?;
            written = written.checked_add(extra).ok_or(Error::Unsupported)?;
            if written > config.max_bytes {
                return Err(Error::Unsupported);
            }
            let path_len = u16::try_from(p.len()).map_err(|_| Error::Unsupported)?;
            let stem_len = u16::try_from(stem.len()).map_err(|_| Error::Unsupported)?;
            writer
                .write_all(&path_len.to_le_bytes())
                .and_then(|_| writer.write_all(p))
                .and_then(|_| writer.write_all(&stem_len.to_le_bytes()))
                .and_then(|_| writer.write_all(stem))
                .and_then(|_| writer.write_all(&[u8::from(record.supported)]))
                .and_then(|_| writer.write_all(&record.mtime.to_bits().to_le_bytes()))
                .map_err(|_| Error::Filesystem)
        })?;
        writer.flush().map_err(|_| Error::Filesystem)?;
        let mut file = writer.into_inner().map_err(|_| Error::Filesystem)?;
        file.seek(SeekFrom::Start(0))
            .map_err(|_| Error::Filesystem)?;
        let mut reader = BufReader::with_capacity(8192, file);
        let mut magic = [0; 8];
        reader
            .read_exact(&mut magic)
            .map_err(|_| Error::Filesystem)?;
        if &magic != MAGIC {
            return Err(Error::Operational);
        }
        spool.reader = Some(reader);
        spool.count = count;
        Ok(spool)
    }
    fn bytes(&mut self, n: usize) -> Result<Vec<u8>, Error> {
        self.consumed = self.consumed.checked_add(n).ok_or(Error::Operational)?;
        if self.consumed > self.max_bytes {
            return Err(Error::Operational);
        }
        let mut result = vec![0; n];
        self.reader
            .as_mut()
            .ok_or(Error::Operational)?
            .read_exact(&mut result)
            .map_err(|_| Error::Operational)?;
        Ok(result)
    }
    fn short(&mut self) -> Result<usize, Error> {
        let bytes = self.bytes(2)?;
        Ok(u16::from_le_bytes(bytes.try_into().map_err(|_| Error::Operational)?) as usize)
    }
    pub(super) fn batch(&mut self) -> Result<Vec<Record>, Error> {
        let size = self
            .count
            .checked_sub(self.read)
            .ok_or(Error::Operational)?
            .min(BATCH);
        let mut result = Vec::with_capacity(size);
        for _ in 0..size {
            let n = self.short()?;
            if n == 0 || n > 4096 {
                return Err(Error::Operational);
            }
            let path = String::from_utf8(self.bytes(n)?).map_err(|_| Error::Operational)?;
            let n = self.short()?;
            if n > 1020 {
                return Err(Error::Operational);
            }
            let stem = String::from_utf8(self.bytes(n)?).map_err(|_| Error::Operational)?;
            let supported = match self.bytes(1)?[0] {
                0 => false,
                1 => true,
                _ => return Err(Error::Operational),
            };
            let bits =
                u64::from_le_bytes(self.bytes(8)?.try_into().map_err(|_| Error::Operational)?);
            let mtime = f64::from_bits(bits);
            if supported {
                if !(100_000_000.0..10_000_000_000.0).contains(&mtime) || stem.chars().count() > 255
                {
                    return Err(Error::Operational);
                }
            } else if bits != 0 || !stem.is_empty() {
                return Err(Error::Operational);
            }
            result.push(Record {
                path,
                stem,
                supported,
                mtime,
            });
            self.read += 1;
        }
        if self.read == self.count {
            let mut trailing = [0; 1];
            if self
                .reader
                .as_mut()
                .ok_or(Error::Operational)?
                .read(&mut trailing)
                .map_err(|_| Error::Operational)?
                != 0
            {
                return Err(Error::Operational);
            }
        }
        Ok(result)
    }
    pub(super) fn close(mut self) -> Result<(), Error> {
        drop(self.reader.take());
        let path = self.path.take().ok_or(Error::Operational)?;
        self.lease
            .as_mut()
            .ok_or(Error::Operational)?
            .finish(&path, self.max_bytes as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::super::{Effects, FileEffect, FutureResult, Receipt, Scanner, SpoolConfig};
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use std::time::{Duration, UNIX_EPOCH};
    struct Fixture {
        base: PathBuf,
        roots: Roots,
        scratch: PathBuf,
    }
    impl Fixture {
        fn new() -> Self {
            let base = std::env::temp_dir()
                .join(format!("rust-owned-logo-spool-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir(&base).unwrap();
            let roots = Roots {
                m3u: base.join("m3us"),
                epg: base.join("epgs"),
                logos: base.join("logos"),
            };
            let scratch = base.join("scratch");
            for p in [&roots.m3u, &roots.epg, &roots.logos, &scratch] {
                std::fs::create_dir(p).unwrap()
            }
            Self {
                base,
                roots,
                scratch,
            }
        }
        fn files(&self, n: usize) {
            for i in 0..n {
                let file =
                    File::create(self.roots.logos.join(format!("owned-{i:04}.png"))).unwrap();
                file.set_times(
                    std::fs::FileTimes::new()
                        .set_modified(UNIX_EPOCH + Duration::from_secs(1700000000)),
                )
                .unwrap();
            }
        }
        fn config(&self, bytes: usize) -> Config {
            Config {
                directory: self.scratch.clone(),
                max_bytes: bytes,
            }
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.base).unwrap()
        }
    }
    struct Spy {
        calls: AtomicUsize,
        scratch: PathBuf,
        full_bytes: usize,
    }
    impl Effects for Spy {
        fn no_index_sources<'a>(&'a self, _: Arc<Receipt>) -> FutureResult<'a, bool> {
            Box::pin(async { Ok(true) })
        }
        fn marker<'a>(&'a self, _: &'a str, _: Arc<Receipt>) -> FutureResult<'a, Option<String>> {
            Box::pin(async move {
                let files = std::fs::read_dir(&self.scratch)
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                let files = files
                    .into_iter()
                    .filter(|v| v.file_name() != ".scanner-owner-v2")
                    .collect::<Vec<_>>();
                assert_eq!(files.len(), 1);
                assert_eq!(files[0].metadata().unwrap().len() as usize, self.full_bytes);
                Ok(None)
            })
        }
        fn exists<'a>(&'a self, _: &'a str, _: Arc<Receipt>) -> FutureResult<'a, bool> {
            Box::pin(async { Ok(false) })
        }
        fn get_or_create<'a>(
            &'a self,
            _: &'a str,
            _: &'a str,
            _: Arc<Receipt>,
        ) -> FutureResult<'a, FileEffect<()>> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::AcqRel);
                Ok(FileEffect::Settled(()))
            })
        }
        fn set_marker<'a>(
            &'a self,
            _: &'a str,
            _: &'a str,
            _: Arc<Receipt>,
        ) -> FutureResult<'a, FileEffect<()>> {
            Box::pin(async { Ok(FileEffect::Settled(())) })
        }
        fn summary<'a>(
            &'a self,
            _: serde_json::Value,
            _: Arc<Receipt>,
        ) -> FutureResult<'a, FileEffect<()>> {
            Box::pin(async { Ok(FileEffect::Settled(())) })
        }
    }
    fn limits() -> Limits {
        Limits {
            files: 512,
            ..Limits::default()
        }
    }
    #[tokio::test]
    async fn all_paths_spooled_and_flushed_before_first_effect_then_cleaned() {
        let f = Fixture::new();
        f.files(259);
        let expected = super::super::inspect(f.roots.clone(), limits()).unwrap();
        let bytes = MAGIC.len()
            + expected
                .iter()
                .map(|v| 13 + v.path.len() + v.stem.len())
                .sum::<usize>();
        let effects = Arc::new(Spy {
            calls: AtomicUsize::new(0),
            scratch: f.scratch.clone(),
            full_bytes: bytes,
        });
        assert!(matches!(
            Scanner::new(effects.clone(), f.roots.clone(), limits()),
            Err(Error::Unsupported)
        ));
        let scanner = Scanner::new_spooled(
            effects.clone(),
            f.roots.clone(),
            limits(),
            SpoolConfig {
                directory: f.scratch.clone(),
                max_bytes: MAX_DISK,
            },
        )
        .unwrap();
        let counts = scanner.scan(1700000010.0, Arc::new(())).await.unwrap();
        assert_eq!(counts.processed, 259);
        assert_eq!(counts.total_files, 259);
        assert_eq!(effects.calls.load(Ordering::Acquire), 259);
        assert!(scanner.first_scan_completed());
        assert_eq!(
            std::fs::read_dir(&f.scratch)
                .unwrap()
                .filter_map(Result::ok)
                .filter(|v| v.file_name() != ".scanner-owner-v2")
                .count(),
            0
        );
        assert!(scanner.shutdown(Duration::from_secs(1)).await);
    }
    #[tokio::test]
    async fn disk_cap_refuses_all_effects_and_does_not_mark_first() {
        let f = Fixture::new();
        f.files(10);
        let effects = Arc::new(Spy {
            calls: AtomicUsize::new(0),
            scratch: f.scratch.clone(),
            full_bytes: 0,
        });
        let scanner =
            Scanner::new_spooled(effects.clone(), f.roots.clone(), limits(), f.config(128))
                .unwrap();
        assert!(matches!(
            scanner.scan(1700000010.0, Arc::new(())).await,
            Err(Error::Unsupported)
        ));
        assert_eq!(effects.calls.load(Ordering::Acquire), 0);
        assert!(!scanner.first_scan_completed());
        assert_eq!(
            std::fs::read_dir(&f.scratch)
                .unwrap()
                .filter_map(Result::ok)
                .filter(|v| v.file_name() != ".scanner-owner-v2")
                .count(),
            0
        );
    }
    #[tokio::test]
    async fn foreign_scratch_entry_is_retained_without_any_effect() {
        let f = Fixture::new();
        f.files(1);
        let foreign = f.scratch.join("foreign");
        std::fs::write(&foreign, b"preserve").unwrap();
        let effects = Arc::new(Spy {
            calls: AtomicUsize::new(0),
            scratch: f.scratch.clone(),
            full_bytes: 0,
        });
        let scanner = Scanner::new_spooled(
            effects.clone(),
            f.roots.clone(),
            limits(),
            f.config(MAX_DISK),
        )
        .unwrap();
        assert!(matches!(
            scanner.scan(1700000010.0, Arc::new(())).await,
            Err(Error::Unsupported)
        ));
        assert_eq!(effects.calls.load(Ordering::Acquire), 0);
        assert_eq!(std::fs::read(&foreign).unwrap(), b"preserve");
    }
    #[test]
    fn batches_preserve_unsorted_inspected_order_and_are_bounded() {
        let f = Fixture::new();
        f.files(259);
        let expected = super::super::inspect(f.roots.clone(), limits())
            .unwrap()
            .into_iter()
            .map(|v| v.path)
            .collect::<Vec<_>>();
        let mut disk =
            Disk::enumerate(f.roots.clone(), limits(), f.config(MAX_DISK), Arc::new(())).unwrap();
        let mut actual = Vec::new();
        loop {
            let batch = disk.batch().unwrap();
            assert!(batch.len() <= 32);
            if batch.is_empty() {
                break;
            }
            actual.extend(batch.into_iter().map(|v| v.path));
        }
        assert_eq!(actual, expected);
        disk.close().unwrap();
        assert_eq!(
            std::fs::read_dir(&f.scratch)
                .unwrap()
                .filter_map(Result::ok)
                .filter(|v| v.file_name() != ".scanner-owner-v2")
                .count(),
            0
        );
    }
    #[test]
    fn malformed_length_truncation_and_trailing_bytes_are_bounded_errors() {
        for kind in 0..3 {
            let f = Fixture::new();
            f.files(1);
            let mut disk =
                Disk::enumerate(f.roots.clone(), limits(), f.config(MAX_DISK), Arc::new(()))
                    .unwrap();
            let mut file = disk.reader.take().unwrap().into_inner();
            match kind {
                0 => {
                    file.seek(SeekFrom::Start(8)).unwrap();
                    file.write_all(&u16::MAX.to_le_bytes()).unwrap();
                }
                1 => file.set_len(9).unwrap(),
                _ => {
                    file.seek(SeekFrom::End(0)).unwrap();
                    file.write_all(b"x").unwrap();
                }
            }
            file.seek(SeekFrom::Start(8)).unwrap();
            disk.reader = Some(BufReader::new(file));
            assert!(matches!(disk.batch(), Err(Error::Operational)));
            disk.close().unwrap();
            assert_eq!(
                std::fs::read_dir(&f.scratch)
                    .unwrap()
                    .filter_map(Result::ok)
                    .filter(|v| v.file_name() != ".scanner-owner-v2")
                    .count(),
                0
            );
        }
    }
    #[cfg(unix)]
    #[test]
    fn scratch_is_private_and_alias_is_refused() {
        use std::os::unix::fs::PermissionsExt;
        let f = Fixture::new();
        f.files(1);
        let disk =
            Disk::enumerate(f.roots.clone(), limits(), f.config(MAX_DISK), Arc::new(())).unwrap();
        assert_eq!(
            std::fs::metadata(disk.path.as_ref().unwrap())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        disk.close().unwrap();
        let spy = Arc::new(Spy {
            calls: AtomicUsize::new(0),
            scratch: f.scratch.clone(),
            full_bytes: 0,
        });
        assert!(matches!(
            Scanner::new_spooled(
                spy,
                f.roots.clone(),
                limits(),
                Config {
                    directory: f.roots.logos.clone(),
                    max_bytes: MAX_DISK
                }
            ),
            Err(Error::Unsupported)
        ));
    }
}
