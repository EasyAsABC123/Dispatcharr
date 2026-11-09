//! Streaming playlist parsing ported from Dispatcharr's original M3U tasks.
//! Full application/algorithm credit belongs to the original authors; see ATTRIBUTION.md.
mod reader;
pub use reader::{Entries, ReadError};
mod identity;
pub use identity::{stream_hash, StreamIdentity};
mod validation;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::OnceLock};
pub use validation::{validate_playlist_head, HeadValidation, VALIDATION_READ_BYTES};
mod projection;
pub use projection::{project_entry, ProjectedStream, ProjectionContext, ProjectionError};
mod cache;
pub use cache::{CacheDownload, CacheError, DownloadStats};
mod uploads;
pub use uploads::{with_uploaded_playlist, UploadError, UploadLimits};
mod prepared;
pub use prepared::{prepare_playlist, PreparationError, PreparationLimits, PreparedPlaylist};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub attributes: BTreeMap<String, String>,
    pub display_name: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vlc_opts: Option<Vec<String>>,
}
#[derive(Clone, Debug)]
pub struct Limits {
    pub max_line_bytes: usize,
    pub max_entry_bytes: usize,
    pub max_attributes: usize,
    pub max_vlc_options: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_line_bytes: 256 * 1024,
            max_entry_bytes: 1024 * 1024,
            max_attributes: 4096,
            max_vlc_options: 256,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Configuration,
    LineCapacity,
    EntryCapacity,
    AttributeCapacity,
    OptionCapacity,
}
fn trim(value: &str) -> &str {
    value.trim_matches(|c: char| c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c))
}
fn attributes() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r#"([^\s\x1c-\x1f=]+)[\s\x1c-\x1f]*=[\s\x1c-\x1f]*(?:"([^"\n]*)"|'([^'\n]*)')"#)
            .unwrap()
    })
}
/// Preserve literal source parsing, including case-sensitive directives, the
/// final-attribute display-name rule and duplicate-key last-value behavior.
pub fn parse_extinf(line: &str, limits: &Limits) -> Result<Option<Entry>, Error> {
    if line.len() > limits.max_line_bytes {
        return Err(Error::LineCapacity);
    }
    let Some(content) = line.strip_prefix("#EXTINF:") else {
        return Ok(None);
    };
    let content = trim(content);
    let mut attrs = BTreeMap::new();
    let mut last = 0;
    for capture in attributes().captures_iter(content) {
        attrs.insert(
            capture[1].to_lowercase(),
            capture
                .get(2)
                .or_else(|| capture.get(3))
                .unwrap()
                .as_str()
                .to_owned(),
        );
        if attrs.len() > limits.max_attributes {
            return Err(Error::AttributeCapacity);
        }
        last = capture.get(0).unwrap().end();
    }
    let display = if last > 0 {
        let rest = trim(&content[last..]);
        trim(rest.strip_prefix(',').unwrap_or(rest))
    } else {
        trim(content.split_once(',').map_or(content, |(_, name)| name))
    };
    let name = std::iter::once(display)
        .chain([
            attrs
                .get("tvc-guide-title")
                .map(String::as_str)
                .unwrap_or(""),
            attrs.get("tvg-name").map(String::as_str).unwrap_or(""),
            content,
        ])
        .find(|value| !value.is_empty())
        .unwrap_or("")
        .to_owned();
    let entry = Entry {
        attributes: attrs,
        display_name: display.to_owned(),
        name,
        url: None,
        vlc_opts: None,
    };
    check_size(&entry, limits)?;
    Ok(Some(entry))
}
fn size(entry: &Entry) -> usize {
    entry
        .attributes
        .iter()
        .map(|(k, v)| k.len() + v.len())
        .sum::<usize>()
        + entry.display_name.len()
        + entry.name.len()
        + entry.url.as_ref().map_or(0, String::len)
        + entry
            .vlc_opts
            .as_ref()
            .map_or(0, |items| items.iter().map(String::len).sum())
}
fn check_size(entry: &Entry, limits: &Limits) -> Result<(), Error> {
    if size(entry) > limits.max_entry_bytes {
        Err(Error::EntryCapacity)
    } else {
        Ok(())
    }
}
/// Holds one pending entry. Callers consume each yielded entry before feeding
/// another line; the parser never materializes an entire provider playlist.
pub struct Parser {
    limits: Limits,
    pending: Option<Entry>,
    failed: bool,
}
impl Parser {
    pub fn new(limits: Limits) -> Result<Self, Error> {
        if limits.max_line_bytes == 0
            || limits.max_entry_bytes == 0
            || limits.max_attributes == 0
            || limits.max_vlc_options == 0
        {
            return Err(Error::Configuration);
        }
        Ok(Self {
            limits,
            pending: None,
            failed: false,
        })
    }
    pub fn feed(&mut self, raw: &str) -> Result<Option<Entry>, Error> {
        if self.failed {
            return Err(Error::EntryCapacity);
        }
        let result = self.feed_inner(raw);
        if result.is_err() {
            self.failed = true;
            self.pending = None;
        }
        result
    }
    fn feed_inner(&mut self, raw: &str) -> Result<Option<Entry>, Error> {
        if raw.len() > self.limits.max_line_bytes {
            return Err(Error::LineCapacity);
        }
        let line = trim(raw);
        if line.starts_with("#EXTINF") {
            self.pending = parse_extinf(line, &self.limits)?;
        } else if let Some(group) = line.strip_prefix("#EXTGRP:") {
            if let Some(entry) = &mut self.pending {
                entry
                    .attributes
                    .entry("group-title".into())
                    .or_insert_with(|| trim(group).to_owned());
            }
        } else if let Some(option) = line.strip_prefix("#EXTVLCOPT:") {
            if let Some(entry) = &mut self.pending {
                let options = entry.vlc_opts.get_or_insert_with(Vec::new);
                if options.len() >= self.limits.max_vlc_options {
                    return Err(Error::OptionCapacity);
                }
                options.push(option.to_owned());
            }
        } else if ["http", "rtsp", "rtp", "udp"]
            .iter()
            .any(|prefix| line.starts_with(prefix))
        {
            if let Some(mut entry) = self.pending.take() {
                entry.url = Some(if let Some(rest) = line.strip_prefix("udp://@") {
                    format!("udp://{rest}")
                } else {
                    line.to_owned()
                });
                check_size(&entry, &self.limits)?;
                return Ok(Some(entry));
            }
        }
        if let Some(entry) = &self.pending {
            if entry.attributes.len() > self.limits.max_attributes {
                return Err(Error::AttributeCapacity);
            }
            check_size(entry, &self.limits)?;
        }
        Ok(None)
    }
    /// Discard an unfinished final EXTINF, as the source generator does.
    pub fn finish(&mut self) {
        self.pending = None;
    }
}
