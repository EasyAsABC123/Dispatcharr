//! Original CPython ISO parser behavior used by original Dispatcharr/Django.
//! Python Software Foundation credit/license: third-party/PYTHON-COPYRIGHT.
//! Mirrors the compiled parser, whose grammar differs from _pydatetime.py.
use crate::epg_grid_window::GridTimestamp;
use chrono::{Datelike, NaiveDate, Weekday};
fn digits(bytes: &[u8], start: usize, count: usize) -> Option<u32> {
    let mut value = 0;
    for byte in bytes.get(start..start.checked_add(count)?)? {
        if !byte.is_ascii_digit() {
            return None;
        }
        value = value * 10 + u32::from(byte - b'0');
    }
    Some(value)
}
fn iso_date(bytes: &[u8], len: usize) -> Option<NaiveDate> {
    let year = digits(bytes, 0, 4)? as i32;
    if !(1..=9999).contains(&year) {
        return None;
    }
    let separated = bytes.get(4) == Some(&b'-');
    let mut pos = 4 + usize::from(separated);
    let date = if bytes.get(pos) == Some(&b'W') {
        pos += 1;
        let week = digits(bytes, pos, 2)?;
        pos += 2;
        let weekday = if pos < len {
            if separated {
                if bytes.get(pos) != Some(&b'-') {
                    return None;
                }
                pos += 1;
            }
            digits(bytes, pos, 1)?
        } else {
            1
        };
        let weekday = *([
            Weekday::Mon,
            Weekday::Tue,
            Weekday::Wed,
            Weekday::Thu,
            Weekday::Fri,
            Weekday::Sat,
            Weekday::Sun,
        ]
        .get(weekday.checked_sub(1)? as usize)?);
        NaiveDate::from_isoywd_opt(year, week, weekday)?
    } else {
        let month = digits(bytes, pos, 2)?;
        pos += 2;
        if separated {
            if bytes.get(pos) != Some(&b'-') {
                return None;
            }
            pos += 1;
        }
        let day = digits(bytes, pos, 2)?;
        NaiveDate::from_ymd_opt(year, month, day)?
    };
    (1..=9999).contains(&date.year()).then_some(date)
}
fn separator(bytes: &[u8]) -> Option<usize> {
    let len = bytes.len();
    if len < 7 {
        return None;
    }
    if len == 7 {
        return Some(7);
    }
    if bytes.get(4) == Some(&b'-') {
        if bytes.get(5) == Some(&b'W') {
            if len > 8 && bytes[8] == b'-' {
                if len == 9 {
                    return None;
                }
                if len > 10 && bytes[10].is_ascii_digit() {
                    Some(8)
                } else {
                    Some(10)
                }
            } else {
                Some(8)
            }
        } else {
            Some(10)
        }
    } else if bytes.get(4) == Some(&b'W') {
        let mut index = 7;
        while index < len && bytes[index].is_ascii_digit() {
            index += 1;
        }
        Some(if index < 9 {
            index
        } else if index % 2 == 0 {
            7
        } else {
            8
        })
    } else {
        Some(8)
    }
}
// Return values and the original non-NUL-tail flag. The C parser permits
// digit-only fractions after the third compact component; do not add a dot.
fn clock(bytes: &[u8], start: usize, end: usize) -> Option<([u32; 4], bool)> {
    let mut values = [0; 4];
    let mut pos = start;
    let mut separated = false;
    for index in 0..3 {
        values[index] = digits(bytes, pos, 2)?;
        pos += 2;
        let c = bytes.get(pos).copied().unwrap_or(0);
        pos += 1;
        if index == 0 {
            separated = c == b':';
        }
        if pos >= end {
            return Some((values, c != 0));
        }
        if separated && c == b':' {
            continue;
        }
        if matches!(c, b'.' | b',') {
            break;
        }
        if !separated {
            pos -= 1;
        } else {
            return None;
        }
    }
    let count = (end.checked_sub(pos)?).min(6);
    if count == 0 {
        return None;
    }
    values[3] = digits(bytes, pos, count)? * 10u32.pow(6 - count as u32);
    pos += count;
    while bytes.get(pos).is_some_and(u8::is_ascii_digit) {
        pos += 1;
    }
    Some((values, bytes.get(pos).copied().unwrap_or(0) != 0))
}
fn time(bytes: &[u8]) -> Option<([u32; 4], i64)> {
    let zone = bytes
        .iter()
        .position(|v| matches!(v, b'Z' | b'+' | b'-'))
        .unwrap_or(bytes.len());
    let (values, trailing) = clock(bytes, 0, zone)?;
    let offset = if zone == bytes.len() {
        if trailing {
            return None;
        }
        0
    } else if bytes[zone] == b'Z' {
        if zone + 1 != bytes.len() {
            return None;
        }
        0
    } else {
        let (offset, trailing) = clock(bytes, zone + 1, bytes.len())?;
        if trailing {
            return None;
        }
        let sign = if bytes[zone] == b'-' { -1 } else { 1 };
        sign * ((i64::from(offset[0]) * 3600 + i64::from(offset[1]) * 60 + i64::from(offset[2]))
            * 1_000_000
            + i64::from(offset[3]))
    };
    if values[0] > 23 || values[1] > 59 || values[2] > 59 || offset.abs() >= 86_400_000_000 {
        return None;
    }
    Some((values, offset))
}
pub(super) fn datetime(raw: &str) -> Option<GridTimestamp> {
    let bytes = raw.as_bytes();
    let sep = separator(bytes)?;
    let date = iso_date(bytes, sep)?;
    if bytes.len() == sep {
        return Some(GridTimestamp {
            local: date.and_hms_opt(0, 0, 0)?,
            offset_micros: 0,
        });
    }
    let tail = raw.get(sep..)?;
    let separator = tail.chars().next()?;
    let time_start = sep + separator.len_utf8();
    let (values, offset_micros) = time(bytes.get(time_start..)?)?;
    Some(GridTimestamp {
        local: date.and_hms_micro_opt(values[0], values[1], values[2], values[3])?,
        offset_micros,
    })
}
pub(super) fn date(raw: &str) -> Option<GridTimestamp> {
    let len = raw.len();
    if !matches!(len, 7 | 8 | 10) {
        return None;
    }
    let date = iso_date(raw.as_bytes(), len)?;
    Some(GridTimestamp {
        local: date.and_hms_opt(0, 0, 0)?,
        offset_micros: 0,
    })
}
