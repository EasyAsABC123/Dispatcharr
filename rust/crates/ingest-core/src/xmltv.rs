//! Original XMLTV preparation rules; original Dispatcharr authors retain credit.
use crate::projection::{integer, DECIMAL_ZEROS};
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use regex::Regex;
use std::{
    fs::File,
    io::{self, Cursor, Read, Seek, SeekFrom},
    path::Path,
    sync::OnceLock,
};

pub const HTML_ENTITY_DOCTYPE: &[u8] = include_bytes!("../datasets/html4-doctype.xml");
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum XmltvTimeError {
    Invalid,
    Overflow,
}
impl XmltvTimeError {
    pub fn source_error_name(self) -> &'static str {
        match self {
            Self::Invalid => "ValueError",
            Self::Overflow => "OverflowError",
        }
    }
}
fn datetime_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(||{
        // Captured Python3.13 strptime pattern. Match first, then check consumed
        // length: adding an end anchor changes short-field backtracking.
        let format=r"(?P<Y>\d\d\d\d)(?P<m>1[0-2]|0[1-9]|[1-9])(?P<d>3[0-1]|[1-2]\d|0[1-9]|[1-9]| [1-9])(?P<H>2[0-3]|[0-1]\d|\d| \d)(?P<M>[0-5]\d|\d)(?P<S>6[0-1]|[0-5]\d|\d)";
        let mut digits=String::from("[");
        for zero in DECIMAL_ZEROS {digits.push_str(&format!("\\u{{{zero:x}}}-\\u{{{:x}}}",zero+9));}
        digits.push(']');
        Regex::new(&format!("^{}",format.replace(r"\d",&digits))).unwrap()
    })
}
fn number(value: &str) -> Result<i64, XmltvTimeError> {
    integer(value)
        .map_err(|_| XmltvTimeError::Invalid)?
        .ok_or(XmltvTimeError::Invalid)
}
/// Preserve original character slicing, short fields and timezone quirks.
/// XML attributes are strings; nonstring JSON validation belongs to the caller.
pub fn parse_xmltv_time(value: &str) -> Result<DateTime<Utc>, XmltvTimeError> {
    let prefix: Vec<char> = value.chars().take(20).collect();
    let base: String = prefix.iter().take(14).collect();
    let found = datetime_pattern()
        .captures(&base)
        .ok_or(XmltvTimeError::Invalid)?;
    if found.get(0).unwrap().end() != base.len() {
        return Err(XmltvTimeError::Invalid);
    }
    let part = |key: &str| number(found.name(key).unwrap().as_str());
    let year = part("Y")?;
    if !(1..=9999).contains(&year) {
        return Err(XmltvTimeError::Invalid);
    }
    let date = NaiveDate::from_ymd_opt(year as i32, part("m")? as u32, part("d")? as u32)
        .ok_or(XmltvTimeError::Invalid)?;
    let hour = part("H")? as u32;
    let minute = part("M")? as u32;
    let second = part("S")? as u32;
    // Python datetime rejects leap-second values accepted by its regex.
    if second > 59 {
        return Err(XmltvTimeError::Invalid);
    }
    let local = date
        .and_hms_opt(hour, minute, second)
        .ok_or(XmltvTimeError::Invalid)?
        .and_utc();
    let offset = if prefix.len() >= 20 {
        // Original parses both numbers even when an unknown sign means UTC.
        let hours = number(&prefix[16..18].iter().collect::<String>())?;
        let minutes = number(&prefix[18..20].iter().collect::<String>())?;
        let delta = hours * 3600 + minutes * 60;
        match prefix[15] {
            '+' | '-' => {
                if delta.abs() >= 86400 {
                    return Err(XmltvTimeError::Invalid);
                }
                if prefix[15] == '+' {
                    delta
                } else {
                    -delta
                }
            }
            _ => 0,
        }
    } else {
        0
    };
    let utc = local
        .checked_sub_signed(chrono::Duration::seconds(offset))
        .ok_or(XmltvTimeError::Overflow)?;
    if !(1..=9999).contains(&utc.year()) {
        return Err(XmltvTimeError::Overflow);
    }
    Ok(utc)
}
/// Original only inspects the last8192 bytes. This is not XML well-formedness.
pub fn xmltv_file_is_complete(path: &Path) -> bool {
    let check = || -> io::Result<bool> {
        let size = std::fs::metadata(path)?.len();
        if size == 0 {
            return Ok(false);
        }
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(size.saturating_sub(8192)))?;
        let mut tail = Vec::with_capacity(8192);
        file.take(8192).read_to_end(&mut tail)?;
        while tail.last().is_some_and(|b| matches!(b, 9..=13 | 32)) {
            tail.pop();
        }
        Ok(tail.ends_with(b"</tv>"))
    };
    check().unwrap_or(false)
}

pub struct XmltvReader {
    prefix: Cursor<Vec<u8>>,
    file: File,
}
impl XmltvReader {
    /// Read at most512 source bytes and prepend the exact HTML4 entity DTD.
    /// Existing declarations are passed through unchanged, including recovery
    /// cases which a subsequent parser may reject.
    pub fn open(path: &Path) -> io::Result<Self> {
        let mut file = File::open(path)?;
        let mut head = Vec::with_capacity(512);
        (&mut file).take(512).read_to_end(&mut head)?;
        let lower: Vec<u8> = head.iter().map(u8::to_ascii_lowercase).collect();
        let mut prefix = Vec::new();
        let position = if lower.windows(9).any(|v| v == b"<!doctype") {
            0
        } else if let Some(start) = head.windows(5).position(|v| v == b"<?xml") {
            if let Some(end) = head[start..].windows(2).position(|v| v == b"?>") {
                let end = start + end + 2;
                prefix.extend_from_slice(&head[..end]);
                prefix.push(b'\n');
                prefix.extend_from_slice(HTML_ENTITY_DOCTYPE);
                end as u64
            } else {
                prefix.extend_from_slice(HTML_ENTITY_DOCTYPE);
                0
            }
        } else {
            prefix.extend_from_slice(HTML_ENTITY_DOCTYPE);
            0
        };
        file.seek(SeekFrom::Start(position))?;
        Ok(Self {
            prefix: Cursor::new(prefix),
            file,
        })
    }
}
impl Read for XmltvReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let count = self.prefix.read(output)?;
        if count == output.len() {
            return Ok(count);
        }
        Ok(count + self.file.read(&mut output[count..])?)
    }
}
