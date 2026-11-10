//! Literal original programme byte-index scanner. Original Dispatcharr authors
//! retain all algorithm/application credit. Task/lease/SQL integration is gated
//! separately; this scanner never reads the whole guide into memory.
use crate::{channel_core, ImportError};
use memchr::{memchr, memmem};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{Read, Seek},
    sync::atomic::{AtomicBool, Ordering},
};
const CHUNK: usize = 8 * 1024 * 1024;
const MAX_START_TAG: usize = 4096;
const OFFSET_CAP: usize = 10;
const TAG: &[u8] = b"<programme";
#[derive(Clone, Copy, Debug)]
pub struct IndexLimits {
    pub max_keys: usize,
    pub state_bytes: usize,
    pub file_bytes: u64,
    pub max_tags: u64,
}
impl Default for IndexLimits {
    fn default() -> Self {
        Self {
            max_keys: 100_000,
            state_bytes: 64 * 1024 * 1024,
            file_bytes: 4 * 1024 * 1024 * 1024 - 65536,
            max_tags: 5_000_000,
        }
    }
}
impl IndexLimits {
    pub(crate) fn validate(&self) -> Result<(), ImportError> {
        if self.max_keys == 0
            || self.max_keys > 1_000_000
            || self.state_bytes < 4096
            || self.state_bytes > 256 * 1024 * 1024
            || self.file_bytes == 0
            || self.file_bytes > 4 * 1024 * 1024 * 1024 - 65536
            || self.max_tags == 0
            || self.max_tags > 50_000_000
        {
            return Err(ImportError::Capacity("programme-index configuration"));
        }
        Ok(())
    }
}
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ProgrammeIndex {
    pub channels: BTreeMap<String, Vec<u64>>,
    pub interleaved_channels: BTreeSet<String>,
}
impl ProgrammeIndex {
    /// Exact UTF-8 JSON string/decimal lengths plus conservative fixed framing.
    /// Check before constructing the SQL JSON tree, which owns cloned strings
    /// and number nodes. This is a resource guard, not a Source output change.
    pub(crate) fn fits_json_bytes(&self, limit: usize) -> bool {
        fn string_bytes(value: &str) -> usize {
            2 + value
                .chars()
                .map(|ch| match ch {
                    '"' | '\\' | '\u{0008}' | '\u{000c}' | '\n' | '\r' | '\t' => 2,
                    ch if ch < ' ' => 6,
                    ch => ch.len_utf8(),
                })
                .sum::<usize>()
        }
        let mut size = 64usize;
        for (key, offsets) in &self.channels {
            size = size.saturating_add(string_bytes(key) + 4);
            for offset in offsets {
                size = size.saturating_add(if *offset == 0 {
                    2
                } else {
                    offset.ilog10() as usize + 2
                });
            }
            if size > limit {
                return false;
            }
        }
        for key in &self.interleaved_channels {
            size = size.saturating_add(string_bytes(key) + 1);
            if size > limit {
                return false;
            }
        }
        size <= limit
    }
    pub fn source_value(&self) -> serde_json::Value {
        serde_json::json!({"channels":self.channels,"interleaved_channels":self.interleaved_channels})
    }
}
/// Caller supplies an opened file and must retain physical worker/job/file-lease
/// ownership until this synchronous operation actually returns. Validation and
/// subsequent reads use the same descriptor, including after path replacement.
pub fn scan_file(
    mut file: File,
    limits: IndexLimits,
    cancel: &AtomicBool,
) -> Result<ProgrammeIndex, ImportError> {
    limits.validate()?;
    active(cancel)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > limits.file_bytes {
        return Err(ImportError::Capacity("programme-index file"));
    }
    file.rewind()?;
    let mut output = ProgrammeIndex::default();
    let mut previous = None::<String>;
    let mut state_bytes = 0usize;
    let mut tags = 0u64;
    let mut total_read = 0u64;
    let mut buffer = Vec::with_capacity(CHUNK + MAX_START_TAG);
    let mut absolute = 0u64;
    loop {
        active(cancel)?;
        let old = buffer.len();
        buffer.resize(old + CHUNK, 0);
        let mut count = 0;
        while count < CHUNK {
            active(cancel)?;
            let read = match file.read(&mut buffer[old + count..old + CHUNK]) {
                Ok(value) => value,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error.into()),
            };
            if read == 0 {
                break;
            }
            count += read;
            total_read += read as u64;
            if total_read > limits.file_bytes {
                return Err(ImportError::Capacity("programme-index file growth"));
            }
        }
        buffer.truncate(old + count);
        if count == 0 && buffer.is_empty() {
            break;
        }
        let mut search = 0usize;
        loop {
            active(cancel)?;
            let Some((start, end)) = find_tag(&buffer, search) else {
                break;
            };
            if end.is_none() && count > 0 {
                break;
            }
            tags += 1;
            if tags > limits.max_tags {
                return Err(ImportError::Capacity("programme-index tags"));
            }
            let limit =
                end.map_or_else(|| (start + MAX_START_TAG).min(buffer.len()), |end| end + 1);
            if let Some(raw) = channel_attribute(&buffer[start..limit]) {
                let decoded =
                    dispatcharr_store::source_html_unescape(&String::from_utf8_lossy(raw));
                let channel = channel_core::strip(&decoded).to_string();
                let position = absolute + start as u64;
                if let Some(offsets) = output.channels.get_mut(&channel) {
                    if previous.as_deref() != Some(channel.as_str()) {
                        if offsets.len() < OFFSET_CAP {
                            offsets.push(position);
                        } else if !output.interleaved_channels.contains(&channel) {
                            state_bytes = state_bytes.saturating_add(channel.capacity() + 128);
                            if state_bytes > limits.state_bytes {
                                return Err(ImportError::Capacity("programme-index state"));
                            }
                            output.interleaved_channels.insert(channel.clone());
                        }
                    }
                } else {
                    if output.channels.len() >= limits.max_keys {
                        return Err(ImportError::Capacity("programme-index keys"));
                    }
                    // Offset storage is reserved at its fixed Source cap. Charge
                    // retained capacities and conservative B-tree node overhead.
                    state_bytes = state_bytes.saturating_add(
                        channel.capacity() + OFFSET_CAP * std::mem::size_of::<u64>() + 192,
                    );
                    if state_bytes > limits.state_bytes {
                        return Err(ImportError::Capacity("programme-index state"));
                    }
                    let mut offsets = Vec::with_capacity(OFFSET_CAP);
                    offsets.push(position);
                    output.channels.insert(channel.clone(), offsets);
                }
                previous = Some(channel);
            }
            search = end.map_or(start + TAG.len(), |end| end + 1);
        }
        if count == 0 {
            break;
        }
        let keep = search.max(buffer.len().saturating_sub(MAX_START_TAG));
        buffer.copy_within(keep.., 0);
        buffer.truncate(buffer.len() - keep);
        absolute += keep as u64;
    }
    active(cancel)?;
    Ok(output)
}
fn active(cancel: &AtomicBool) -> Result<(), ImportError> {
    if cancel.load(Ordering::Acquire) {
        Err(ImportError::Cancelled)
    } else {
        Ok(())
    }
}
/// None matches Source's (-1,-1); an absent end matches (idx,-1), including EOF.
pub(crate) fn find_tag(buffer: &[u8], mut position: usize) -> Option<(usize, Option<usize>)> {
    while position <= buffer.len() {
        let index = position + memmem::find(&buffer[position..], TAG)?;
        let follow = index + TAG.len();
        if follow >= buffer.len() {
            return Some((index, None));
        }
        if !b" \t\n\r>/".contains(&buffer[follow]) {
            position = follow;
            continue;
        }
        let limit = (index + MAX_START_TAG).min(buffer.len());
        let end = memchr(b'>', &buffer[follow..limit]).map(|offset| follow + offset);
        if end.is_none() && buffer.len() >= index + MAX_START_TAG {
            return None;
        }
        return Some((index, end));
    }
    None
}
/// Python bytes regex channel\s*=\s*(?:"([^"]+)"|'([^']+)'), with no word
/// boundary. Empty values and attribute-prefix quirks are intentionally literal.
pub(crate) fn channel_attribute(buffer: &[u8]) -> Option<&[u8]> {
    let mut search = 0;
    while search <= buffer.len() {
        let start = search + memmem::find(&buffer[search..], b"channel")?;
        let mut position = start + 7;
        while position < buffer.len() && matches!(buffer[position], b' ' | b'\t'..=b'\r') {
            position += 1;
        }
        if buffer.get(position) != Some(&b'=') {
            search = start + 1;
            continue;
        }
        position += 1;
        while position < buffer.len() && matches!(buffer[position], b' ' | b'\t'..=b'\r') {
            position += 1;
        }
        let &quote = buffer.get(position)?;
        if !matches!(quote, b'\'' | b'"') {
            search = start + 1;
            continue;
        }
        position += 1;
        if let Some(end) = memchr(quote, &buffer[position..]) {
            if end > 0 {
                return Some(&buffer[position..position + end]);
            }
        }
        search = start + 1;
    }
    None
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn json_guard_accounts_for_controls_utf8_and_large_offsets() {
        let mut index = ProgrammeIndex::default();
        index
            .channels
            .insert("\"\\\n\u{001f}🦀".repeat(100), vec![0, u64::MAX]);
        let actual = serde_json::to_vec(&index.source_value()).unwrap().len();
        assert!(index.fits_json_bytes(actual + 64));
        assert!(!index.fits_json_bytes(actual / 2));
    }
    #[test]
    fn literal_attribute_and_tag_boundaries() {
        assert_eq!(
            channel_attribute(b"notchannel = 'first' channel=\"second\""),
            Some(&b"first"[..])
        );
        assert_eq!(
            channel_attribute(b"channel='' channel=\"next\""),
            Some(&b"next"[..])
        );
        assert_eq!(
            find_tag(b"<programmeXYZ><programme/>", 0),
            Some((14, Some(25)))
        );
        assert_eq!(find_tag(b"<programme", 0), Some((0, None)));
        assert!(channel_attribute(b"channel\x0b=\x0c'a'").is_some());
    }
}
