//! Original Dispatcharr byte-offset current-programme lookup, bounded separately
//! from bulk guide ingestion. Original authors retain all algorithm credit.
//! Pure file reader only; fresh index/clock/queue and HTTP wiring are not implied.
use crate::{channel_core, programme_index, ImportError};
use chrono::{DateTime, SecondsFormat, Utc};
use dispatcharr_ingest_core::{parse_xmltv_time, XmltvTimeError};
use dispatcharr_xmltv_parser::{parse_xmltv_element, StrictElementError};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};
#[derive(Debug)]
pub enum CurrentFileError {
    Runtime(ImportError),
    Time(XmltvTimeError),
}
impl From<ImportError> for CurrentFileError {
    fn from(value: ImportError) -> Self {
        Self::Runtime(value)
    }
}
impl From<std::io::Error> for CurrentFileError {
    fn from(value: std::io::Error) -> Self {
        Self::Runtime(value.into())
    }
}
impl From<XmltvTimeError> for CurrentFileError {
    fn from(value: XmltvTimeError) -> Self {
        Self::Time(value)
    }
}
const READ: usize = 2 * 1024 * 1024;
const CLOSE: &[u8] = b"</programme>";
#[derive(Clone, Copy, Debug)]
pub struct CurrentFileLimits {
    pub buffer_bytes: usize,
    pub read_bytes: u64,
    pub max_offsets: usize,
    pub forward_timeout: Duration,
}
impl Default for CurrentFileLimits {
    fn default() -> Self {
        Self {
            buffer_bytes: 8 * 1024 * 1024,
            read_bytes: 256 * 1024 * 1024,
            max_offsets: 10,
            forward_timeout: Duration::from_secs(10),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CurrentProgramme {
    pub title: String,
    pub description: String,
    pub sub_title: String,
    pub start_time: String,
    pub end_time: String,
}
impl CurrentProgramme {
    pub fn source_value(&self) -> serde_json::Value {
        serde_json::json!({"title":self.title,"description":self.description,"sub_title":self.sub_title,"start_time":self.start_time,"end_time":self.end_time})
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CurrentFileResult {
    Found(CurrentProgramme),
    Missing,
    Timeout,
}
/// Retain worker/memory/file ownership until this synchronous operation returns.
/// Validation and reads use the same opened regular file. Offsets refer to this
/// descriptor; path replacement cannot switch an in-progress reader's inode.
pub fn read_current_file(
    mut file: File,
    tvg_id: &str,
    offsets: &[u64],
    interleaved: bool,
    now: DateTime<Utc>,
    limits: CurrentFileLimits,
    cancel: &AtomicBool,
) -> Result<CurrentFileResult, CurrentFileError> {
    if !(READ..=16 * 1024 * 1024).contains(&limits.buffer_bytes)
        || limits.read_bytes == 0
        || limits.read_bytes > 4 * 1024 * 1024 * 1024
        || limits.max_offsets == 0
        || limits.max_offsets > 100
        || offsets.len() > limits.max_offsets
        || limits.forward_timeout.is_zero()
        || limits.forward_timeout > Duration::from_secs(10)
    {
        return Err(ImportError::Capacity("current-file configuration").into());
    }
    active(cancel)?;
    if !file.metadata()?.is_file() {
        return Err(ImportError::Capacity("current-file regular file").into());
    }
    let mut read_bytes = 0;
    for &offset in offsets {
        file.seek(SeekFrom::Start(offset))?;
        if let CurrentFileResult::Found(programme) = scan(
            &mut file,
            tvg_id,
            now,
            limits,
            cancel,
            false,
            &mut read_bytes,
        )? {
            return Ok(CurrentFileResult::Found(programme));
        }
    }
    if interleaved {
        // Source offsets[-1] raises for a malformed empty interleaved index.
        let offset = offsets.last().ok_or(ImportError::Parser)?;
        file.seek(SeekFrom::Start(*offset))?;
        scan(
            &mut file,
            tvg_id,
            now,
            limits,
            cancel,
            true,
            &mut read_bytes,
        )
    } else {
        Ok(CurrentFileResult::Missing)
    }
}
/// Post96 additive adapter draft: invoke the file factory independently for
/// offsets and (after a miss) the interleaved forward scan, as Source does.
/// No new Source/concurrent-replacement acceptance is implied before its gate.
/// Caller retains the path/factory context through both physical helper runs.
pub fn read_current_with_opener(
    mut open: impl FnMut() -> std::io::Result<File>,
    tvg_id: &str,
    offsets: &[u64],
    interleaved: bool,
    now: DateTime<Utc>,
    limits: CurrentFileLimits,
    cancel: &AtomicBool,
) -> Result<CurrentFileResult, CurrentFileError> {
    if !(READ..=16 * 1024 * 1024).contains(&limits.buffer_bytes)
        || limits.read_bytes == 0
        || limits.read_bytes > 4 * 1024 * 1024 * 1024
        || limits.max_offsets == 0
        || limits.max_offsets > 100
        || offsets.len() > limits.max_offsets
        || limits.forward_timeout.is_zero()
        || limits.forward_timeout > Duration::from_secs(10)
    {
        return Err(ImportError::Capacity("current-file configuration").into());
    }
    active(cancel)?;
    let mut file = open()?;
    active(cancel)?;
    if !file.metadata()?.is_file() {
        return Err(ImportError::Capacity("current-file regular file").into());
    }
    let mut read_bytes = 0;
    for &offset in offsets {
        active(cancel)?;
        file.seek(SeekFrom::Start(offset))?;
        if let CurrentFileResult::Found(programme) = scan(
            &mut file,
            tvg_id,
            now,
            limits,
            cancel,
            false,
            &mut read_bytes,
        )? {
            return Ok(CurrentFileResult::Found(programme));
        }
    }
    // Source closes the offset helper before invoking its forward helper.
    drop(file);
    if !interleaved {
        return Ok(CurrentFileResult::Missing);
    }
    active(cancel)?;
    let offset = offsets.last().ok_or(ImportError::Parser)?;
    let mut file = open()?;
    active(cancel)?;
    if !file.metadata()?.is_file() {
        return Err(ImportError::Capacity("current-file regular file").into());
    }
    file.seek(SeekFrom::Start(*offset))?;
    scan(
        &mut file,
        tvg_id,
        now,
        limits,
        cancel,
        true,
        &mut read_bytes,
    )
}
fn active(cancel: &AtomicBool) -> Result<(), ImportError> {
    if cancel.load(Ordering::Acquire) {
        Err(ImportError::Cancelled)
    } else {
        Ok(())
    }
}
#[allow(clippy::too_many_arguments)]
fn scan(
    file: &mut File,
    tvg_id: &str,
    now: DateTime<Utc>,
    limits: CurrentFileLimits,
    cancel: &AtomicBool,
    forward: bool,
    read_bytes: &mut u64,
) -> Result<CurrentFileResult, CurrentFileError> {
    let deadline = Instant::now() + limits.forward_timeout;
    let mut buffer = Vec::with_capacity(limits.buffer_bytes);
    loop {
        active(cancel)?;
        // Source timeout is checked at each outer read, not each programme.
        if forward && Instant::now() > deadline {
            return Ok(CurrentFileResult::Timeout);
        }
        let old = buffer.len();
        if old > limits.buffer_bytes - READ {
            return Err(ImportError::Capacity("current-file retained buffer").into());
        }
        buffer.resize(old + READ, 0);
        let mut count = 0;
        while count < READ {
            active(cancel)?;
            let got = match file.read(&mut buffer[old + count..old + READ]) {
                Ok(value) => value,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error.into()),
            };
            if got == 0 {
                break;
            }
            count += got;
            *read_bytes += got as u64;
            if *read_bytes > limits.read_bytes {
                return Err(ImportError::Capacity("current-file read bytes").into());
            }
        }
        buffer.truncate(old + count);
        if count == 0 && buffer.is_empty() {
            break;
        }
        let mut search = 0;
        let mut trim = 0;
        loop {
            active(cancel)?;
            let Some((start, end)) = programme_index::find_tag(&buffer, search) else {
                if forward {
                    trim = search;
                }
                break;
            };
            if end.is_none() && count > 0 {
                if forward {
                    trim = start;
                }
                break;
            }
            let attr_limit = end.map_or((start + 4096).min(buffer.len()), |end| end + 1);
            let Some(raw) = programme_index::channel_attribute(&buffer[start..attr_limit]) else {
                search = end.map_or(start + 10, |end| end + 1);
                continue;
            };
            let decoded = dispatcharr_store::source_html_unescape(&String::from_utf8_lossy(raw));
            if channel_core::strip(&decoded) != tvg_id {
                if !forward {
                    return Ok(CurrentFileResult::Missing);
                }
                search = end.map_or(start + 10, |end| end + 1);
                continue;
            }
            // Python regex m.end() includes the closing attribute quote.
            let close_search = end.map_or_else(|| attribute_end(&buffer, raw), |end| end + 1);
            let Some(close) = memchr::memmem::find(&buffer[close_search..], CLOSE)
                .map(|n| close_search + n + CLOSE.len())
            else {
                if forward {
                    trim = start;
                }
                break;
            };
            search = close;
            match parse_xmltv_element(&buffer[start..close], cancel) {
                Ok(element) => {
                    let start = element.attrs.get("start").filter(|v| !v.is_empty());
                    let end = element.attrs.get("stop").filter(|v| !v.is_empty());
                    if let (Some(start), Some(end)) = (start, end) {
                        let start = parse_xmltv_time(start)?;
                        let end = parse_xmltv_time(end)?;
                        if start <= now && now < end {
                            let text = |tag: &str| {
                                element
                                    .children
                                    .iter()
                                    .find(|child| child.tag == tag)
                                    .and_then(|child| child.text.clone())
                                    .unwrap_or_default()
                            };
                            return Ok(CurrentFileResult::Found(CurrentProgramme {
                                title: text("title"),
                                description: text("desc"),
                                sub_title: text("sub-title"),
                                start_time: start.to_rfc3339_opts(SecondsFormat::AutoSi, false),
                                end_time: end.to_rfc3339_opts(SecondsFormat::AutoSi, false),
                            }));
                        }
                    }
                }
                Err(StrictElementError::Syntax) => {}
                Err(StrictElementError::Cancelled) => return Err(ImportError::Cancelled.into()),
                Err(_) => return Err(ImportError::Capacity("current-file strict element").into()),
            }
        }
        let discard = if forward { trim } else { search };
        if discard > 0 {
            buffer.copy_within(discard.., 0);
            buffer.truncate(buffer.len() - discard);
        }
        if count == 0 {
            break;
        }
    }
    Ok(CurrentFileResult::Missing)
}
fn attribute_end(buffer: &[u8], raw: &[u8]) -> usize {
    // channel_attribute returns a borrowed subslice of this same buffer.
    raw.as_ptr() as usize - buffer.as_ptr() as usize + raw.len() + 1
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{io::Write, sync::atomic::AtomicU64};
    static IDS: AtomicU64 = AtomicU64::new(0);
    fn owned(data: &[u8]) -> (std::path::PathBuf, File) {
        let path = std::env::temp_dir().join(format!(
            "dispatcharr-current-test-{}-{}",
            std::process::id(),
            IDS.fetch_add(1, Ordering::Relaxed)
        ));
        let mut writer = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        writer.write_all(data).unwrap();
        drop(writer);
        let file = File::open(&path).unwrap();
        (path, file)
    }
    fn now() -> DateTime<Utc> {
        "2026-11-10T12:00:00+00:00".parse().unwrap()
    }
    const VALID:&[u8]=b"<programme channel='one' start='20261110110000 +0000' stop='20261110130000 +0000'><title> before <b>nested</b>after</title></programme>";
    #[test]
    fn draft_factory_reopens_after_offset_miss() {
        let old=b"<programme channel='one' start='20261110100000 +0000' stop='20261110110000 +0000'><title>past</title></programme>";
        let (path, _) = owned(old);
        let (replacement, _) = owned(VALID);
        let mut opens = 0;
        let result = read_current_with_opener(
            || {
                opens += 1;
                if opens == 2 {
                    std::fs::rename(&replacement, &path)?;
                }
                File::open(&path)
            },
            "one",
            &[0],
            true,
            now(),
            CurrentFileLimits::default(),
            &AtomicBool::new(false),
        )
        .unwrap();
        std::fs::remove_file(path).unwrap();
        assert_eq!(opens, 2);
        assert!(
            matches!(result,CurrentFileResult::Found(CurrentProgramme{title,..}) if title==" before ")
        );
    }
    #[test]
    fn draft_factory_first_hit_does_not_reopen() {
        let (path, _) = owned(VALID);
        let mut opens = 0;
        let result = read_current_with_opener(
            || {
                opens += 1;
                File::open(&path)
            },
            "one",
            &[0],
            true,
            now(),
            CurrentFileLimits::default(),
            &AtomicBool::new(false),
        )
        .unwrap();
        std::fs::remove_file(path).unwrap();
        assert_eq!(opens, 1);
        assert!(matches!(result, CurrentFileResult::Found(_)));
    }
    #[test]
    fn opened_descriptor_survives_atomic_replacement() {
        let (path, file) = owned(VALID);
        let (replacement, _) = owned(b"different file");
        std::fs::rename(&replacement, &path).unwrap();
        let result = read_current_file(
            file,
            "one",
            &[0],
            false,
            now(),
            CurrentFileLimits::default(),
            &AtomicBool::new(false),
        )
        .unwrap();
        std::fs::remove_file(path).unwrap();
        assert!(
            matches!(result,CurrentFileResult::Found(CurrentProgramme{title,..}) if title==" before ")
        );
    }
    #[test]
    fn cancellation_precedes_physical_seek_and_read() {
        let (path, mut file) = owned(VALID);
        file.seek(SeekFrom::Start(17)).unwrap();
        // Cloned descriptors share the kernel cursor; a canceled operation must
        // not reposition even this already-open descriptor.
        let check = file.try_clone().unwrap();
        let result = read_current_file(
            file,
            "one",
            &[0],
            false,
            now(),
            CurrentFileLimits::default(),
            &AtomicBool::new(true),
        );
        let mut check = check;
        assert_eq!(check.stream_position().unwrap(), 17);
        std::fs::remove_file(path).unwrap();
        assert!(matches!(
            result,
            Err(CurrentFileError::Runtime(ImportError::Cancelled))
        ));
    }
    #[test]
    fn retained_incomplete_element_and_aggregate_reads_are_bounded() {
        let mut data = b"<programme channel='one'>".to_vec();
        data.resize(READ * 3, b'x');
        let (path, file) = owned(&data);
        std::fs::remove_file(path).unwrap();
        let limits = CurrentFileLimits {
            buffer_bytes: READ,
            ..CurrentFileLimits::default()
        };
        assert!(matches!(
            read_current_file(
                file,
                "one",
                &[0],
                false,
                now(),
                limits,
                &AtomicBool::new(false)
            ),
            Err(CurrentFileError::Runtime(ImportError::Capacity(
                "current-file retained buffer"
            )))
        ));
        let (path, file) = owned(VALID);
        std::fs::remove_file(path).unwrap();
        let limits = CurrentFileLimits {
            read_bytes: 1,
            ..CurrentFileLimits::default()
        };
        assert!(matches!(
            read_current_file(
                file,
                "one",
                &[0],
                false,
                now(),
                limits,
                &AtomicBool::new(false)
            ),
            Err(CurrentFileError::Runtime(ImportError::Capacity(
                "current-file read bytes"
            )))
        ));
    }
}
