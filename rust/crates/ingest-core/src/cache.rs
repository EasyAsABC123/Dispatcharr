//! Bounded disk staging for original Dispatcharr's download/validate/promote path.
//! Callers must serialize refreshes for an account before opening its cache.
use crate::{validate_playlist_head, VALIDATION_READ_BYTES};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};

#[derive(Debug)]
pub enum CacheError {
    Io(io::Error),
    Capacity,
    Invalid(String),
}
impl From<io::Error> for CacheError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}
impl std::fmt::Display for CacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => error.fmt(f),
            Self::Capacity => f.write_str("M3U download exceeds configured disk-byte limit"),
            Self::Invalid(message) => f.write_str(message),
        }
    }
}
impl std::error::Error for CacheError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DownloadStats {
    pub bytes: u64,
    /// Python's binary-file iteration counts LF delimiters and a final tail.
    pub lines: u64,
}

/// Holds at most the source's 32-KiB validation prefix in memory.
/// Failed validation, I/O or cancellation by dropping this value removes staging
/// and preserves the previous cache. This synchronous writer belongs on a
/// bounded blocking worker; it must not run on an asynchronous executor thread.
pub struct CacheDownload {
    destination: PathBuf,
    temporary: PathBuf,
    file: Option<File>,
    head: Vec<u8>,
    maximum: u64,
    stats: DownloadStats,
    final_byte: Option<u8>,
    failed: bool,
    promoted: bool,
}
impl CacheDownload {
    pub fn begin(destination: &Path, maximum: u64) -> Result<Self, CacheError> {
        if maximum == 0 {
            return Err(CacheError::Capacity);
        }
        let mut temporary = destination.as_os_str().to_os_string();
        temporary.push(".tmp");
        let temporary = PathBuf::from(temporary);
        // A leftover/concurrent staging file requires account-lock recovery;
        // never truncate another writer's staging data without ownership.
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        Ok(Self {
            destination: destination.to_owned(),
            temporary,
            file: Some(file),
            head: Vec::with_capacity(VALIDATION_READ_BYTES),
            maximum,
            stats: DownloadStats { bytes: 0, lines: 0 },
            final_byte: None,
            failed: false,
            promoted: false,
        })
    }
    pub fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), CacheError> {
        if self.failed {
            return Err(CacheError::Invalid("M3U download already failed".into()));
        }
        let Some(total) = self
            .stats
            .bytes
            .checked_add(chunk.len() as u64)
            .filter(|total| *total <= self.maximum)
        else {
            self.failed = true;
            return Err(CacheError::Capacity);
        };
        if let Err(error) = self
            .file
            .as_mut()
            .expect("open staging file")
            .write_all(chunk)
        {
            self.failed = true;
            return Err(error.into());
        }
        self.head
            .extend_from_slice(&chunk[..chunk.len().min(VALIDATION_READ_BYTES - self.head.len())]);
        self.stats.bytes = total;
        self.stats.lines += chunk.iter().filter(|byte| **byte == b'\n').count() as u64;
        self.final_byte = chunk.last().copied().or(self.final_byte);
        Ok(())
    }
    pub fn finish(mut self, status: u16, url: &str) -> Result<DownloadStats, CacheError> {
        if self.failed {
            return Err(CacheError::Invalid("M3U download already failed".into()));
        }
        if self.stats.bytes == 0 {
            return Err(CacheError::Invalid(format!("Server responded successfully (HTTP {status}) but provided empty M3U file from URL: {url}")));
        }
        if let Some(error) = validate_playlist_head(&self.head, url).error {
            return Err(CacheError::Invalid(error));
        }
        let mut file = self.file.take().expect("open staging file");
        file.flush()?;
        drop(file);
        fs::rename(&self.temporary, &self.destination)?;
        self.promoted = true;
        self.stats.lines += u64::from(self.final_byte != Some(b'\n'));
        Ok(self.stats)
    }
}
impl Drop for CacheDownload {
    fn drop(&mut self) {
        self.file.take();
        if !self.promoted {
            let _ = fs::remove_file(&self.temporary);
        }
    }
}
