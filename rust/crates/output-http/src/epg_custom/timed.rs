//! Original's timed and dated events with export-window filler.
//! Original Dispatcharr authors retain all credit.
use super::{
    capacity, group, merge_groups, string, template, Groups, NoTime, Programme, StoreError, Window,
};
use chrono::{
    DateTime, Datelike, Duration, LocalResult, NaiveDate, NaiveDateTime, NaiveTime, Offset,
    TimeZone, Timelike, Utc,
};
use chrono_tz::{OffsetComponents, Tz};
use dispatcharr_ingest_core::source_integer_literal;
use num_bigint::BigInt;

pub(super) fn time_info(groups: &Groups) -> Result<Option<NaiveTime>, StoreError> {
    let as_integer = |text| {
        let text = source_integer_literal(text)?;
        BigInt::parse_bytes(text.as_bytes(), 10)
    };
    let Some(mut hour) = group(groups, "hour").and_then(as_integer) else {
        return Ok(None);
    };
    let minute = match groups
        .iter()
        .find(|(key, _)| key == "minute")
        .and_then(|(_, value)| value.as_deref())
    {
        Some(value) => match as_integer(value) {
            Some(value) => value,
            None => return Ok(None),
        },
        None => BigInt::from(0),
    };
    match group(groups, "ampm") {
        Some(value) if value.eq_ignore_ascii_case("pm") => {
            if hour != BigInt::from(12) {
                hour += 12;
            }
        }
        Some(value) if value.eq_ignore_ascii_case("am") => {
            if hour == BigInt::from(12) {
                hour = BigInt::from(0);
            }
        }
        _ if hour > BigInt::from(23) => hour %= 24,
        _ => {}
    }
    let bad_time = || StoreError::UnhandledRestFault;
    let hour = u32::try_from(hour).map_err(|_| bad_time())?;
    let minute = u32::try_from(minute).map_err(|_| bad_time())?;
    NaiveTime::from_hms_opt(hour, minute, 0)
        .map(Some)
        .ok_or_else(bad_time)
}

fn source_is_digit(value: char) -> bool {
    // CPython 3.13 / Unicode 15.1 also accepts these non-decimal digits.
    // Its subsequent int() rejects them, which drops the date capture.
    value.is_ascii_digit()
        || (value.is_numeric()
            && (source_integer_literal(value.encode_utf8(&mut [0; 4])).is_some()
                || matches!(
                    u32::from(value),
                    0xB2..=0xB3 | 0xB9 | 0x1369..=0x1371 | 0x19DA | 0x2070
                        | 0x2074..=0x2079 | 0x2080..=0x2089 | 0x2460..=0x2468
                        | 0x2474..=0x247C | 0x2488..=0x2490 | 0x24EA | 0x24F5..=0x24FD
                        | 0x24FF | 0x2776..=0x277E | 0x2780..=0x2788 | 0x278A..=0x2792
                        | 0x10A40..=0x10A43 | 0x10E60..=0x10E68 | 0x11052..=0x1105A
                        | 0x1F100..=0x1F10A
                )))
}

pub(super) fn captured_date(
    groups: &Groups,
    base: NaiveDate,
) -> Result<Option<NaiveDate>, StoreError> {
    const MONTHS: [&str; 12] = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ];
    let integer = |value| {
        let value = source_integer_literal(value)?;
        BigInt::parse_bytes(value.as_bytes(), 10)
    };
    let number = |name, default| match group(groups, name).filter(|value| !value.is_empty()) {
        Some(value) => integer(value),
        None => Some(BigInt::from(default)),
    };
    let Some(day) = number("day", base.day() as i32) else {
        return Ok(None);
    };
    let Some(year) = number("year", base.year()) else {
        return Ok(None);
    };
    let month = match group(groups, "month").filter(|value| !value.is_empty()) {
        Some(value) if value.chars().all(source_is_digit) => match integer(value) {
            Some(month) => u32::try_from(month).unwrap_or(0),
            None => return Ok(None),
        },
        Some(value) => MONTHS
            .iter()
            .position(|month| {
                value.eq_ignore_ascii_case(month) || value.eq_ignore_ascii_case(&month[..3])
            })
            .map_or(base.month(), |index| index as u32 + 1),
        None => base.month(),
    };
    let day = u32::try_from(day).unwrap_or(0);
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return Ok(None);
    }
    let error = || StoreError::UnhandledRestFault;
    let year = i32::try_from(year).map_err(|_| error())?;
    if !(1..=9999).contains(&year) {
        return Err(error());
    }
    NaiveDate::from_ymd_opt(year, month, day)
        .map(Some)
        .ok_or_else(error)
}

pub(super) fn localize(zone: Tz, wall: NaiveDateTime) -> Result<DateTime<Utc>, StoreError> {
    let mut probe = wall;
    loop {
        let candidate = match zone.from_local_datetime(&probe) {
            LocalResult::Single(value) => value,
            LocalResult::Ambiguous(first, second) => {
                // pytz localize defaults to is_dst=False; when both sides
                // agree on DST status it chooses the latest UTC instant.
                let is_dst = |value: DateTime<Tz>| !value.offset().dst_offset().is_zero();
                match (is_dst(first), is_dst(second)) {
                    (false, true) => first,
                    (true, false) => second,
                    _ => first.max(second),
                }
            }
            LocalResult::None => {
                // pytz retries from six hours earlier for skipped wall times,
                // retaining the located offset when it restores the wall time.
                probe = probe
                    .checked_sub_signed(Duration::hours(6))
                    .ok_or_else(capacity)?;
                continue;
            }
        };
        let offset = i64::from(candidate.offset().fix().local_minus_utc());
        return wall
            .checked_sub_signed(Duration::seconds(offset))
            .map(|value| value.and_utc())
            .ok_or_else(capacity);
    }
}

pub(super) fn clock_fields(
    groups: &mut Groups,
    date: NaiveDate,
    starts: NaiveTime,
    duration: Duration,
) {
    let (ends, _) = starts.overflowing_add_signed(duration);
    let (start24, start12, start_long) = labels(starts);
    let (end24, end12, end_long) = labels(ends);
    merge_groups(
        groups,
        [
            ("date", date.format("%Y-%m-%d").to_string()),
            ("month", date.month().to_string()),
            ("day", date.day().to_string()),
            ("year", date.year().to_string()),
            ("starttime24", start24),
            ("starttime", start12),
            ("starttime_long", start_long),
            ("endtime24", end24),
            ("endtime", end12),
            ("endtime_long", end_long),
        ]
        .into_iter()
        .map(|(key, value)| (key.into(), Some(value)))
        .collect(),
    );
}

fn labels(time: NaiveTime) -> (String, String, String) {
    let (pm, hour) = time.hour12();
    let minute = time.minute();
    let daypart = if pm { "PM" } else { "AM" };
    let long = format!("{hour}:{minute:02} {daypart}");
    if minute == 0 {
        (
            format!("{:02}:00", time.hour()),
            format!("{hour} {daypart}"),
            long,
        )
    } else {
        (format!("{}:{minute:02}", time.hour()), long.clone(), long)
    }
}

pub(super) struct Labels {
    pub(super) title: String,
    pub(super) description: String,
}

impl Labels {
    pub(super) fn for_filler(
        properties: &serde_json::Value,
        phase: &str,
        default_prefix: &str,
        main_title: &str,
        main_description: &str,
        groups: &Groups,
        limit: usize,
    ) -> Result<Self, StoreError> {
        let title = string(properties, &format!("{phase}_title_template"))?;
        let description = string(properties, &format!("{phase}_description_template"))?;
        Ok(Self {
            title: if title.is_empty() {
                main_title.to_owned()
            } else {
                template(title, groups, false, limit)?
            },
            description: if description.is_empty() {
                if default_prefix.len().saturating_add(main_description.len()) > limit {
                    return Err(capacity());
                }
                format!("{default_prefix}{main_description}")
            } else {
                template(description, groups, false, limit)?
            },
        })
    }

    pub(super) fn bytes(&self) -> usize {
        self.title
            .capacity()
            .saturating_add(self.description.capacity())
    }
}

#[derive(Clone, Copy)]
enum Kind {
    Upcoming,
    Main,
    Ended,
}

#[derive(Clone, Copy)]
struct Segment {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    kind: Kind,
}

impl Segment {
    fn clipped(start: DateTime<Utc>, end: DateTime<Utc>, kind: Kind) -> Option<Self> {
        (start < end).then_some(Self { start, end, kind })
    }
}

fn ceil_half_hour(value: DateTime<Utc>) -> Result<DateTime<Utc>, StoreError> {
    let seconds_past = i64::from(value.minute() % 30 * 60 + value.second());
    let whole = value.with_nanosecond(0).ok_or_else(capacity)?;
    let remainder = (1800 - seconds_past) % 1800;
    whole
        .checked_add_signed(Duration::seconds(remainder))
        .ok_or_else(capacity)
}

fn dated_segments(window: Window, event: Segment) -> Result<[Option<Segment>; 3], StoreError> {
    let lookback = window.lookback.unwrap_or(window.start);
    let end = match window.cutoff {
        Some(cutoff) => cutoff,
        None => window
            .start
            .checked_add_signed(Duration::days(i64::from(if window.days == 0 {
                3
            } else {
                window.days
            })))
            .ok_or_else(capacity)?,
    };
    if lookback >= end {
        return Ok([None; 3]);
    }
    let before = if event.start > window.start {
        Segment::clipped(
            ceil_half_hour(lookback.max(window.start))?,
            end.min(event.start),
            Kind::Upcoming,
        )
    } else {
        None
    };
    let main = (event.end >= lookback && window.cutoff.is_none_or(|cutoff| event.start < cutoff))
        .then_some(event);
    let after_start = if event.end < lookback && main.is_none() {
        ceil_half_hour(lookback)?
    } else {
        event.end.max(lookback)
    };
    let after = Segment::clipped(after_start, end, Kind::Ended);
    Ok([before, main, after])
}

pub struct Timed {
    shared: NoTime,
    upcoming: Labels,
    ended: Labels,
    event: Segment,
    duration: Duration,
    day: u32,
    pending: [Option<Segment>; 3],
    emitted: bool,
}

impl Timed {
    pub(super) fn new(
        shared: NoTime,
        kickoff: NaiveTime,
        date: Option<NaiveDate>,
        duration: Duration,
        upcoming: Labels,
        ended: Labels,
    ) -> Result<Self, StoreError> {
        let event_date = date.unwrap_or_else(|| {
            shared
                .window
                .clock
                .with_timezone(&shared.source_timezone)
                .date_naive()
        });
        let start = localize(shared.source_timezone, event_date.and_time(kickoff))?;
        let event = Segment {
            start,
            end: start.checked_add_signed(duration).ok_or_else(capacity)?,
            kind: Kind::Main,
        };
        let (day, pending) = if date.is_some() {
            (shared.window.days, dated_segments(shared.window, event)?)
        } else {
            (0, [None; 3])
        };
        Ok(Self {
            shared,
            upcoming,
            ended,
            event,
            duration,
            day,
            pending,
            emitted: false,
        })
    }

    fn next_day(&mut self) -> Result<(), StoreError> {
        let Window {
            start,
            lookback,
            cutoff,
            ..
        } = self.shared.window;
        let day_start = start
            .checked_add_signed(Duration::days(i64::from(self.day)))
            .ok_or_else(capacity)?;
        let day_end = day_start
            .checked_add_signed(Duration::days(1))
            .ok_or_else(capacity)?;
        self.day += 1;
        let start = lookback.map_or(day_start, |bound| day_start.max(bound));
        let end = cutoff.map_or(day_end, |bound| day_end.min(bound));
        if start >= end {
            return Ok(());
        }

        // Source clamps the filler to each day, but emits the entire event
        // exactly once, even if it crosses a day or export edge.
        let main = (!self.emitted && self.event.start < end && self.event.end > start)
            .then_some(self.event);
        self.emitted |= main.is_some();
        self.pending = [
            Segment::clipped(start, end.min(self.event.start), Kind::Upcoming),
            main,
            Segment::clipped(start.max(self.event.end), end, Kind::Ended),
        ];
        Ok(())
    }

    fn next_programme(&mut self) -> Result<Option<Programme>, StoreError> {
        loop {
            if let Some(slot) = self.pending.iter_mut().find(|slot| slot.is_some()) {
                let mut segment = slot.take().unwrap();
                let start = segment.start;
                let (end, filler) = match segment.kind {
                    Kind::Main => (segment.end, None),
                    kind => {
                        let end = start
                            .checked_add_signed(self.duration)
                            .ok_or_else(capacity)?
                            .min(segment.end);
                        let label = if matches!(kind, Kind::Upcoming) {
                            &self.upcoming
                        } else {
                            &self.ended
                        };
                        (end, Some(label))
                    }
                };
                if end < segment.end {
                    segment.start = end;
                    *slot = Some(segment);
                }
                return self.shared.programme(start, end, filler).map(Some);
            }
            if self.day >= self.shared.window.days {
                return Ok(None);
            }
            self.next_day()?;
        }
    }
}

impl Iterator for Timed {
    type Item = Result<Programme, StoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_programme() {
            Ok(value) => value.map(Ok),
            Err(error) => {
                self.day = self.shared.window.days;
                self.pending = [None; 3];
                Some(Err(error))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_named_zone_keeps_localize_defaults_at_dst_transitions() {
        let ny = chrono_tz::America::New_York;
        for (wall, expected) in [
            ("2026-03-08 02:30:00", "2026-03-08T07:30:00Z"),
            ("2026-11-01 01:30:00", "2026-11-01T06:30:00Z"),
        ] {
            let wall = NaiveDateTime::parse_from_str(wall, "%Y-%m-%d %H:%M:%S").unwrap();
            assert_eq!(
                localize(ny, wall).unwrap(),
                expected.parse::<DateTime<Utc>>().unwrap()
            );
        }
    }

    #[test]
    fn source_dated_event_includes_full_stop_and_excludes_exact_cutoff_start() {
        let start = "2026-04-09T11:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let event = Segment {
            start,
            end: start + Duration::minutes(91),
            kind: Kind::Main,
        };
        let mut window = Window {
            start,
            clock: start,
            days: 1,
            lookback: Some(event.end),
            cutoff: Some(start + Duration::hours(6)),
        };
        let [before, main, after] = dated_segments(window, event).unwrap();
        assert!(before.is_none());
        assert_eq!(main.unwrap().end, event.end);
        assert_eq!(after.unwrap().start, event.end);

        window.lookback = Some(start - Duration::hours(1));
        window.start = start - Duration::hours(1);
        window.cutoff = Some(start);
        let [before, main, after] = dated_segments(window, event).unwrap();
        assert_eq!(before.unwrap().end, start);
        assert!(main.is_none() && after.is_none());
    }

    #[test]
    fn source_utc_time_parts_keep_unicode_int_and_midnight_spelling() {
        let groups = vec![
            ("hour".into(), Some("١٢".into())),
            ("minute".into(), Some("٥".into())),
            ("ampm".into(), Some("am".into())),
        ];
        let start = time_info(&groups).unwrap().unwrap();
        assert_eq!(start, NaiveTime::from_hms_opt(0, 5, 0).unwrap());
        assert_eq!(
            labels(start),
            ("0:05".into(), "12:05 AM".into(), "12:05 AM".into())
        );
        let midnight = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
        assert_eq!(
            labels(midnight),
            ("00:00".into(), "12 AM".into(), "12:00 AM".into())
        );
    }
}
