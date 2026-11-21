//! Bounded decoder for the pinned Original runtime timezone catalogue.
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
include!("../datasets/tzdb-2026e/calendar-catalogue.rs");
static BUNDLE: &[u8] = include_bytes!("../datasets/tzdb-2026e/calendar-catalogue.tzif");
static CACHE: [OnceLock<Result<Zone, Error>>; RANGES.len()] =
    [const { OnceLock::new() }; RANGES.len()];

fn zone(name: &str) -> Result<&'static Zone, Error> {
    let key = NAMES
        .binary_search_by(|(known, _, _)| known.cmp(&name))
        .map_err(|_| Error::Unsupported)?;
    let index = NAMES[key].1;
    let (start, end, utc) = RANGES[index];
    CACHE[index]
        .get_or_init(|| parse(&BUNDLE[start..end], utc))
        .as_ref()
        .map_err(|_| Error::Unsupported)
}

pub(super) fn query_offsets() -> impl ExactSizeIterator<Item = (&'static str, i32)> {
    NAMES.iter().map(|(name, _, offset)| (*name, *offset))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn saved_timezone_catalogue_is_sorted_bounded_and_matches_the_decoder() {
        let january = DateTime::from_timestamp(1672574400, 0).unwrap();
        assert!((401..=768).contains(&NAMES.len()));
        assert!(BUNDLE.len() <= 8 * 1024 * 1024);
        assert!(NAMES.windows(2).all(|pair| pair[0].0 < pair[1].0));
        assert!(RANGES.windows(2).all(|pair| pair[0].1 == pair[1].0));
        let mut all_ranges_visited = vec![false; RANGES.len()];
        for &(name, range, planner_offset) in NAMES {
            let (start, end, utc) = RANGES[range];
            assert!(start < end && end <= BUNDLE.len());
            assert_eq!(name == "UTC", utc);
            assert_eq!(planner_offset, -offset(name, january).unwrap() / 3600);
            assert_eq!(
                resolve(name, local(name, january).unwrap()).unwrap(),
                january
            );
            all_ranges_visited[range] = true;
        }
        assert!(all_ranges_visited.into_iter().all(|visited| visited));
        assert!(offset("america/chicago", january).is_err());
        assert_eq!(query_offsets().len(), NAMES.len());
    }

    #[test]
    fn saved_timezone_catalogue_handles_dst_fractional_and_alias_cases() {
        let unix = |stamp: &str| {
            DateTime::parse_from_rfc3339(stamp)
                .unwrap()
                .with_timezone(&Utc)
        };
        let march_gap = unix("2026-03-08T02:30:00Z").naive_utc();
        let november_fold = unix("2026-11-01T01:30:00Z").naive_utc();
        assert_eq!(
            resolve("America/Chicago", march_gap).unwrap(),
            unix("2026-03-08T08:30:00Z")
        );
        assert_eq!(
            resolve("America/Chicago", november_fold).unwrap(),
            unix("2026-11-01T06:30:00Z")
        );
        let winter = unix("2026-01-05T12:00:00Z");
        assert_eq!(offset("Pacific/Chatham", winter).unwrap(), 49500);
        assert_eq!(offset("America/St_Johns", winter).unwrap(), -12600);
        assert_eq!(
            offset("US/Eastern", winter).unwrap(),
            offset("America/New_York", winter).unwrap()
        );
        assert!(offset("America/Chicago", unix("2038-01-01T12:00:00Z")).is_err());
    }
}
