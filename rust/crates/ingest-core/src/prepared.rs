//! Disk-backed parsed-cache preparation for the original M3U refresh format.
use crate::{Entries, Limits, ReadError};
use std::{
    collections::HashSet,
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufWriter, Write},
    path::{Path, PathBuf},
};

#[derive(Clone, Debug)]
pub struct PreparationLimits {
    pub parser: Limits,
    pub entries: u64,
    pub groups: usize,
    pub group_bytes: usize,
    pub cache_bytes: u64,
}
impl Default for PreparationLimits {
    fn default() -> Self {
        Self {
            parser: Limits::default(),
            entries: 5_000_000,
            groups: 32768,
            group_bytes: 16 * 1024 * 1024,
            cache_bytes: 4 * 1024 * 1024 * 1024,
        }
    }
}
#[derive(Debug)]
pub enum PreparationError {
    Io(io::Error),
    Json(serde_json::Error),
    Parse(ReadError),
    ParserConfiguration(crate::Error),
    Capacity(&'static str),
}
impl From<io::Error> for PreparationError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}
impl From<serde_json::Error> for PreparationError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}
impl std::fmt::Display for PreparationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => e.fmt(f),
            Self::Json(e) => e.fmt(f),
            Self::Parse(e) => write!(f, "M3U parse failed: {e:?}"),
            Self::ParserConfiguration(e) => write!(f, "Invalid M3U parser configuration: {e:?}"),
            Self::Capacity(name) => write!(f, "M3U preparation exceeds configured {name} limit"),
        }
    }
}
impl std::error::Error for PreparationError {}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedPlaylist {
    pub entries: u64,
    pub groups: Vec<String>,
    pub bytes: u64,
}
struct Staging {
    path: PathBuf,
    promoted: bool,
}
impl Drop for Staging {
    fn drop(&mut self) {
        if !self.promoted {
            let _ = fs::remove_file(&self.path);
        }
    }
}
struct BoundedWriter {
    inner: BufWriter<File>,
    bytes: u64,
    maximum: u64,
}
impl Write for BoundedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let total = self
            .bytes
            .checked_add(bytes.len() as u64)
            .filter(|total| *total <= self.maximum)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::FileTooLarge,
                    "Parsed M3U cache exceeds configured byte limit",
                )
            })?;
        self.inner.write_all(bytes)?;
        self.bytes = total;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// Reproduces `extinf_data` and first-seen `groups` in the source JSON cache,
/// keeping one parsed entry and a bounded group index in memory. Promotion only
/// occurs after EOF, including compressed-file CRC/trailer validation.
/// The caller must hold the account refresh lock and use a blocking worker.
pub fn prepare_playlist<R: BufRead>(
    reader: R,
    destination: &Path,
    limits: PreparationLimits,
) -> Result<PreparedPlaylist, PreparationError> {
    if limits.groups == 0 || limits.group_bytes < "Default Group".len() || limits.cache_bytes == 0 {
        return Err(PreparationError::Capacity("nonzero capacity"));
    }
    let entries =
        Entries::new(reader, limits.parser).map_err(PreparationError::ParserConfiguration)?;
    let mut path = destination.as_os_str().to_os_string();
    path.push(".tmp");
    let path = PathBuf::from(path);
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)?;
    let mut staging = Staging {
        path,
        promoted: false,
    };
    let mut writer = BoundedWriter {
        inner: BufWriter::with_capacity(8192, file),
        bytes: 0,
        maximum: limits.cache_bytes,
    };
    writer.write_all(b"{\"extinf_data\":[")?;
    let mut groups = vec!["Default Group".to_owned()];
    let mut seen = HashSet::from(["Default Group".to_owned()]);
    let mut group_bytes = "Default Group".len();
    let mut count = 0_u64;
    for entry in entries {
        let entry = entry.map_err(PreparationError::Parse)?;
        if count >= limits.entries {
            return Err(PreparationError::Capacity("entry-count"));
        }
        if let Some(group) = entry
            .attributes
            .get("group-title")
            .filter(|group| !group.is_empty())
        {
            if !seen.contains(group) {
                if groups.len() >= limits.groups {
                    return Err(PreparationError::Capacity("group-count"));
                }
                group_bytes = group_bytes
                    .checked_add(group.len())
                    .filter(|total| *total <= limits.group_bytes)
                    .ok_or(PreparationError::Capacity("group-byte"))?;
                seen.insert(group.clone());
                groups.push(group.clone());
            }
        }
        if count > 0 {
            writer.write_all(b",")?;
        }
        serde_json::to_writer(&mut writer, &entry)?;
        count += 1;
    }
    writer.write_all(b"],\"groups\":{")?;
    for (index, group) in groups.iter().enumerate() {
        if index > 0 {
            writer.write_all(b",")?;
        }
        serde_json::to_writer(&mut writer, group)?;
        writer.write_all(b":{}")?;
    }
    writer.write_all(b"}}")?;
    writer.flush()?;
    let bytes = writer.bytes;
    drop(writer);
    fs::rename(&staging.path, destination)?;
    staging.promoted = true;
    Ok(PreparedPlaylist {
        entries: count,
        groups,
        bytes,
    })
}
