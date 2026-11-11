//! Exact pinned Original system TZif data, bounded known-zone decoder.
//! IANA/tzdata contributors retain data credit; Original authors retain credit.
//! No host tzdb fallback, POSIX-tail extrapolation or unbounded date authority.
use super::Error;
use chrono::{DateTime, Datelike, NaiveDateTime, Timelike, Utc};
use std::sync::OnceLock;
struct Zone {
    transitions: Vec<(i64, usize)>,
    offsets: Vec<i32>,
    utc: bool,
}
fn number(data: &[u8], at: usize) -> Result<usize, Error> {
    let bytes: [u8; 4] = data
        .get(at..at + 4)
        .ok_or(Error::Unsupported)?
        .try_into()
        .unwrap();
    Ok(u32::from_be_bytes(bytes) as usize)
}
fn counts(data: &[u8], at: usize) -> Result<[usize; 6], Error> {
    if data.get(at..at + 4) != Some(b"TZif") {
        return Err(Error::Unsupported);
    }
    let mut result = [0; 6];
    for (index, value) in result.iter_mut().enumerate() {
        *value = number(data, at + 20 + index * 4)?;
    }
    if result[0] > 256
        || result[1] > 256
        || result[2] > 64
        || result[3] > 4096
        || result[4] == 0
        || result[4] > 256
        || result[5] > 4096
    {
        return Err(Error::Capacity);
    }
    Ok(result)
}
fn parse(data: &[u8], utc: bool) -> Result<Zone, Error> {
    if data.len() > 65536 {
        return Err(Error::Capacity);
    }
    let first = counts(data, 0)?;
    let size = first[3] * 5 + first[4] * 6 + first[5] + first[2] * 8 + first[0] + first[1];
    let at = 44usize.checked_add(size).ok_or(Error::Capacity)?;
    if !matches!(data.get(4), Some(b'2' | b'3')) {
        return Err(Error::Unsupported);
    }
    let count = counts(data, at)?;
    let start = at + 44;
    let end = start
        .checked_add(count[3] * 9 + count[4] * 6 + count[5] + count[2] * 12 + count[0] + count[1])
        .ok_or(Error::Capacity)?;
    if end > data.len() {
        return Err(Error::Unsupported);
    }
    let mut offsets = Vec::with_capacity(count[4]);
    let types = start + count[3] * 9;
    for index in 0..count[4] {
        let pos = types + index * 6;
        let value = i32::from_be_bytes(data[pos..pos + 4].try_into().unwrap());
        if !(-86400..=86400).contains(&value)
            || data[pos + 4] > 1
            || usize::from(data[pos + 5]) >= count[5]
        {
            return Err(Error::Unsupported);
        }
        offsets.push(value);
    }
    let mut transitions = Vec::with_capacity(count[3]);
    for index in 0..count[3] {
        let time = i64::from_be_bytes(
            data[start + index * 8..start + index * 8 + 8]
                .try_into()
                .unwrap(),
        );
        let kind = usize::from(data[start + count[3] * 8 + index]);
        if kind >= offsets.len() || transitions.last().is_some_and(|(last, _)| *last >= time) {
            return Err(Error::Unsupported);
        }
        transitions.push((time, kind));
    }
    Ok(Zone {
        transitions,
        offsets,
        utc,
    })
}
static UTC: OnceLock<Result<Zone, Error>> = OnceLock::new();
static NY: OnceLock<Result<Zone, Error>> = OnceLock::new();
static LH: OnceLock<Result<Zone, Error>> = OnceLock::new();
static KTM: OnceLock<Result<Zone, Error>> = OnceLock::new();
static PARIS: OnceLock<Result<Zone, Error>> = OnceLock::new();
fn zone(name: &str) -> Result<&'static Zone, Error> {
    let (value, data, utc): (&OnceLock<Result<Zone, Error>>, &[u8], bool) = match name {
        "UTC" => (&UTC, include_bytes!("../datasets/tzdb-2026e/UTC"), true),
        "America/New_York" => (
            &NY,
            include_bytes!("../datasets/tzdb-2026e/America-New_York"),
            false,
        ),
        "Australia/Lord_Howe" => (
            &LH,
            include_bytes!("../datasets/tzdb-2026e/Australia-Lord_Howe"),
            false,
        ),
        "Asia/Kathmandu" => (
            &KTM,
            include_bytes!("../datasets/tzdb-2026e/Asia-Kathmandu"),
            false,
        ),
        "Europe/Paris" => (
            &PARIS,
            include_bytes!("../datasets/tzdb-2026e/Europe-Paris"),
            false,
        ),
        _ => return Err(Error::Unsupported),
    };
    value
        .get_or_init(|| parse(data, utc))
        .as_ref()
        .map_err(|_| Error::Unsupported)
}
impl Zone {
    fn offset(&self, time: DateTime<Utc>) -> Result<i32, Error> {
        if !self.utc && !(1970..=2037).contains(&time.year()) {
            return Err(Error::Unsupported);
        }
        let index = self
            .transitions
            .partition_point(|(point, _)| *point <= time.timestamp());
        Ok(self.offsets[if index == 0 {
            0
        } else {
            self.transitions[index - 1].1
        }])
    }
}
pub(super) fn offset(name: &str, time: DateTime<Utc>) -> Result<i32, Error> {
    zone(name)?.offset(time)
}
pub(super) fn local(name: &str, time: DateTime<Utc>) -> Result<NaiveDateTime, Error> {
    time.naive_utc()
        .checked_add_signed(chrono::Duration::seconds(i64::from(offset(name, time)?)))
        .ok_or(Error::Capacity)
}
pub(super) fn resolve(name: &str, wall: NaiveDateTime) -> Result<DateTime<Utc>, Error> {
    let zone = zone(name)?;
    let seconds = wall.and_utc().timestamp();
    let nanos = wall.nanosecond();
    let mut earliest = None;
    for value in &zone.offsets {
        let Some(time) = DateTime::from_timestamp(seconds - i64::from(*value), nanos) else {
            return Err(Error::Capacity);
        };
        if zone.offset(time).is_ok_and(|actual| actual == *value)
            && earliest.is_none_or(|old| time < old)
        {
            earliest = Some(time)
        }
    }
    if let Some(time) = earliest {
        return Ok(time);
    }
    // Source ZoneInfo fold=0 gives the pre-transition offset for nonexistent
    // local gap times. Celery then converts the resulting endpoint to UTC.
    let mut previous = zone.offsets[0];
    for (point, index) in &zone.transitions {
        let next = zone.offsets[*index];
        if next > previous
            && seconds >= point + i64::from(previous)
            && seconds < point + i64::from(next)
        {
            let time = DateTime::from_timestamp(seconds - i64::from(previous), nanos)
                .ok_or(Error::Capacity)?;
            zone.offset(time)?;
            return Ok(time);
        }
        previous = next;
    }
    Err(Error::Unsupported)
}
