//! Literal busy seek decisions ported from Dispatcharr Timeshift views.
//! All original application credit belongs to upstream authors/contributors.
use crate::{parse_client_range, RangeOverflow};

pub const EOF_PROBE_TAIL_BYTES: i128 = 1_880_000;
pub const EOF_PROBE_UNKNOWN_LENGTH_MIN: i128 = 100_000_000;

pub fn is_near_eof_probe(range: Option<&str>, length: Option<i128>) -> bool {
    let Some((start, _)) = parse_client_range(range) else {
        return false;
    };
    match length {
        Some(total) => start >= total.saturating_sub(EOF_PROBE_TAIL_BYTES).max(0),
        None => start >= EOF_PROBE_UNKNOWN_LENGTH_MIN,
    }
}

pub fn should_displace_busy_playback(
    range: Option<&str>,
    length: Option<i128>,
    serving: Option<&str>,
) -> bool {
    let Some((start, _)) = parse_client_range(range) else {
        return false;
    };
    if is_near_eof_probe(range, length) {
        return false;
    }
    start != 0 || serving == Some("none")
}

pub fn is_full_restart_range(range: Option<&str>) -> bool {
    match range.filter(|value| !value.is_empty()) {
        None => true,
        Some(value) => {
            parse_client_range(Some(value)).is_some_and(|(start, end)| start == 0 && end.is_none())
        }
    }
}

pub fn should_preempt_plain_reconnect(
    range: Option<&str>,
    exists: bool,
    busy: bool,
    pool_media: Option<&str>,
    media: &str,
) -> bool {
    is_full_restart_range(range) && exists && busy && pool_media.is_none_or(|value| value == media)
}

pub fn cap_open_ended_range(
    range: Option<&str>,
    span: Option<i128>,
) -> Result<Option<String>, RangeOverflow> {
    let Some((start, end)) = parse_client_range(range) else {
        return Ok(range.map(str::to_owned));
    };
    let Some(span) = span.filter(|value| *value > 0) else {
        return Ok(range.map(str::to_owned));
    };
    if end.is_some() {
        return Ok(range.map(str::to_owned));
    }
    let end = start.checked_add(span - 1).ok_or(RangeOverflow)?;
    Ok(Some(format!("bytes={start}-{end}")))
}
