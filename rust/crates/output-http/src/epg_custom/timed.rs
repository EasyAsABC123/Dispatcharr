//! Original's UTC time-of-day event with export-window filler.
//! Original Dispatcharr authors retain all credit.
use super::{
    capacity, group, merge_groups, string, template, Groups, NoTime, Programme, StoreError, Window,
};
use chrono::{DateTime, Datelike, Duration, NaiveTime, Timelike, Utc};
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

pub(super) fn clock_fields(
    groups: &mut Groups,
    clock: DateTime<Utc>,
    starts: NaiveTime,
    duration: Duration,
) {
    let date = clock.date_naive();
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
        duration: Duration,
        upcoming: Labels,
        ended: Labels,
    ) -> Result<Self, StoreError> {
        let start = shared.window.clock.date_naive().and_time(kickoff).and_utc();
        let event = Segment {
            start,
            end: start.checked_add_signed(duration).ok_or_else(capacity)?,
            kind: Kind::Main,
        };
        Ok(Self {
            shared,
            upcoming,
            ended,
            event,
            duration,
            day: 0,
            pending: [None; 3],
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
