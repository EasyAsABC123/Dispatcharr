//! Literal native archive Range/length behavior from apps/timeshift/views.py.
//! Original Dispatcharr application credit belongs to upstream contributors.
use std::collections::BTreeMap;

pub struct ContentRange {
    pub start: i128,
    pub end: Option<i128>,
    pub total: Option<i128>,
}
fn integer(value: &str) -> Option<i128> {
    let value = value.trim();
    let digits = value.strip_prefix(['+', '-']).unwrap_or(value);
    if digits.is_empty()
        || digits.starts_with('_')
        || digits.ends_with('_')
        || digits.contains("__")
        || !digits.bytes().all(|b| b.is_ascii_digit() || b == b'_')
    {
        return None;
    }
    value.replace('_', "").parse().ok()
}
pub fn parse_client_range(value: Option<&str>) -> Option<(i128, Option<i128>)> {
    let range = value?.strip_prefix("bytes=")?;
    let (start, end) = range.split_once('-')?;
    Some((
        if start.is_empty() { 0 } else { integer(start)? },
        if end.is_empty() {
            None
        } else {
            Some(integer(end)?)
        },
    ))
}
pub fn parse_content_range(value: Option<&str>) -> Option<ContentRange> {
    let (range, total) = value?.strip_prefix("bytes ")?.rsplit_once('/')?;
    let (start, end) = range.split_once('-')?;
    Some(ContentRange {
        start: if start.is_empty() { 0 } else { integer(start)? },
        end: if end.is_empty() {
            None
        } else {
            Some(integer(end)?)
        },
        total: if total == "*" {
            None
        } else {
            Some(integer(total)?)
        },
    })
}
pub fn representation_length(
    content_range: Option<&str>,
    content_length: Option<&str>,
) -> Option<i128> {
    if let Some(range) = parse_content_range(content_range) {
        return range.total;
    }
    content_length.and_then(integer)
}
pub struct LengthRequest<'a> {
    pub range: Option<&'a str>,
    pub status: u16,
    pub representation_length: Option<i128>,
    pub upstream_range: Option<&'a str>,
    pub upstream_length: Option<&'a str>,
    pub streaming: bool,
}
/// Preserve the source's suffix-as-zero parsing and omission rules. These
/// helpers characterize compatibility, rather than asserting RFC correctness.
#[derive(Debug, PartialEq, Eq)]
pub struct RangeOverflow;
pub fn downstream_length_headers(
    request: LengthRequest<'_>,
) -> Result<BTreeMap<String, String>, RangeOverflow> {
    let mut headers = BTreeMap::from([("Accept-Ranges".into(), "bytes".into())]);
    let upstream = parse_content_range(request.upstream_range);
    let representation = request
        .representation_length
        .or_else(|| upstream.as_ref().and_then(|range| range.total));
    let upstream_range = request.upstream_range.filter(|value| !value.is_empty());
    let upstream_length = request.upstream_length.filter(|value| !value.is_empty());
    let range = request.range.filter(|value| !value.is_empty());
    if request.status == 206 {
        if let Some(value) = upstream_range {
            headers.insert("Content-Range".into(), value.into());
        } else if let (Some(range), Some(total)) = (range, representation) {
            if let Some((start, end)) = parse_client_range(Some(range)) {
                let last = total.checked_sub(1).ok_or(RangeOverflow)?;
                let end = end.map_or(last, |end| end.min(last));
                headers.insert(
                    "Content-Range".into(),
                    format!("bytes {start}-{end}/{total}"),
                );
            }
        }
        if let Some(length) = upstream_length {
            headers.insert("Content-Length".into(), length.into());
        } else if let Some(ContentRange {
            start,
            end: Some(end),
            ..
        }) = upstream
        {
            headers.insert(
                "Content-Length".into(),
                end.checked_sub(start)
                    .and_then(|length| length.checked_add(1))
                    .ok_or(RangeOverflow)?
                    .to_string(),
            );
        }
        return Ok(headers);
    }
    if !request.streaming || request.status == 200 && range.is_none() {
        if let Some(total) = representation {
            headers.insert("Content-Length".into(), total.to_string());
        } else if let Some(length) = upstream_length {
            headers.insert("Content-Length".into(), length.into());
        }
    }
    Ok(headers)
}
