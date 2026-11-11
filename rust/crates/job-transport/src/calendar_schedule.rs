//! Bounded known numeric crontab calculation derived from pinned Celery5.6.3.
//! Original Celery/Dispatcharr/django-celery-beat authors retain credit/licenses.
//! Exact Source TZif2026e, no host tzdb or unknown expression fallback.
use super::{calendar_tz as tz, Error};
use chrono::{DateTime, Datelike, FixedOffset, NaiveDate, NaiveDateTime, Timelike, Utc};
#[derive(Clone)]
struct Field {
    values: Vec<u32>,
}
impl Field {
    fn parse(raw: &str, min: u32, count: u32) -> Result<Self, Error> {
        if raw.len() > 256 || raw.is_empty() {
            return Err(Error::Capacity);
        }
        let max = min + count - 1;
        let mut seen = vec![false; count as usize];
        for part in raw.split(',') {
            if part.is_empty() {
                return Err(Error::Unsupported);
            }
            let pieces: Vec<_> = part.split('/').collect();
            if pieces.len() > 2 {
                return Err(Error::Unsupported);
            }
            let step = if pieces.len() == 2 {
                pieces[1].parse::<usize>().map_err(|_| Error::Unsupported)?
            } else {
                1
            };
            if step == 0 || step > 4096 {
                return Err(Error::Unsupported);
            }
            let numbers = if pieces[0] == "*" {
                (min..=max).collect::<Vec<_>>()
            } else {
                let limits: Vec<_> = pieces[0].split('-').collect();
                if limits.len() > 2 || pieces.len() == 2 && limits.len() != 2 {
                    return Err(Error::Unsupported);
                }
                let a = limits[0].parse::<u32>().map_err(|_| Error::Unsupported)?;
                let b = if limits.len() == 2 {
                    limits[1].parse::<u32>().map_err(|_| Error::Unsupported)?
                } else {
                    a
                };
                if a < min || a > max || b < min || b > max {
                    return Err(Error::Unsupported);
                }
                if b < a {
                    (a..=max).chain(min..=b).collect()
                } else {
                    (a..=b).collect()
                }
            };
            for value in numbers.into_iter().step_by(step) {
                seen[(value - min) as usize] = true;
            }
        }
        let values = seen
            .into_iter()
            .enumerate()
            .filter_map(|(index, set)| set.then_some(index as u32 + min))
            .collect::<Vec<_>>();
        if values.is_empty() {
            return Err(Error::Unsupported);
        }
        Ok(Self { values })
    }
    fn has(&self, value: u32) -> bool {
        self.values.binary_search(&value).is_ok()
    }
    fn first(&self) -> u32 {
        self.values[0]
    }
    fn last(&self) -> u32 {
        *self.values.last().unwrap()
    }
    fn after(&self, value: u32) -> Option<u32> {
        self.values
            .iter()
            .copied()
            .find(|candidate| *candidate > value)
    }
}
#[derive(Clone)]
pub(super) struct Calendar {
    minute: Field,
    hour: Field,
    weekday: Field,
    day: Field,
    month: Field,
    all_day_month: bool,
    zone: String,
}
impl Calendar {
    pub fn parse(value: &(String, String, String, String, String, String)) -> Result<Self, Error> {
        let (minute, hour, weekday, day, month, zone) = value;
        tz::offset(zone, DateTime::from_timestamp(1672574400, 0).unwrap())?;
        Ok(Self {
            minute: Field::parse(minute, 0, 60)?,
            hour: Field::parse(hour, 0, 24)?,
            weekday: Field::parse(weekday, 0, 7)?,
            day: Field::parse(day, 1, 31)?,
            month: Field::parse(month, 1, 12)?,
            all_day_month: day == "*" && month == "*",
            zone: zone.clone(),
        })
    }
    fn at(&self, date: NaiveDate, hour: u32, minute: u32) -> Result<NaiveDateTime, Error> {
        date.and_hms_opt(hour, minute, 0).ok_or(Error::Capacity)
    }
    fn next_wall(&self, last: NaiveDateTime, now: NaiveDateTime) -> Result<NaiveDateTime, Error> {
        let matches = self.month.has(last.month())
            && self.day.has(last.day())
            && self.weekday.has(last.weekday().num_days_from_sunday());
        // Preserve Source remaining_delta's date-sensitive minute branch,
        // rather than substituting a generic next-matching-minute library.
        if matches
            && last.date() == now.date()
            && self.hour.has(last.hour())
            && last.minute() < self.minute.last()
        {
            return self.at(
                last.date(),
                last.hour(),
                self.minute.after(last.minute()).ok_or(Error::Unsupported)?,
            );
        }
        let minute = self.minute.first();
        if matches && last.hour() < self.hour.last() {
            return self.at(
                last.date(),
                self.hour.after(last.hour()).ok_or(Error::Unsupported)?,
                minute,
            );
        }
        let hour = self.hour.first();
        if self.all_day_month {
            let dow = last.weekday().num_days_from_sunday();
            let next = self.weekday.after(dow).unwrap_or(self.weekday.first());
            let mut days = (next + 7 - dow) % 7;
            if days == 0 {
                days = 7
            }
            let date = last
                .date()
                .checked_add_signed(chrono::Duration::days(i64::from(days)))
                .ok_or(Error::Capacity)?;
            return self.at(date, hour, minute);
        }
        // Bounded Gregorian calendar search. Forty years includes the full
        // leap-day/weekday recurrence used by this known domain. Unsupported
        // beyond that bound is explicit, never a fabricated future timestamp.
        let mut date = last.date();
        for _ in 0..366 * 40 {
            date = date.succ_opt().ok_or(Error::Capacity)?;
            if self.month.has(date.month())
                && self.day.has(date.day())
                && self.weekday.has(date.weekday().num_days_from_sunday())
            {
                return self.at(date, hour, minute);
            }
        }
        Err(Error::Unsupported)
    }
    fn remaining(
        &self,
        last: NaiveDateTime,
        now_wall: NaiveDateTime,
        now: DateTime<Utc>,
        fixed: Option<FixedOffset>,
    ) -> Result<f64, Error> {
        let next = self.next_wall(last, now_wall)?;
        let end = if let Some(offset) = fixed {
            next.and_utc()
                .checked_sub_signed(chrono::Duration::seconds(i64::from(
                    offset.local_minus_utc(),
                )))
                .ok_or(Error::Capacity)?
        } else {
            tz::resolve(&self.zone, next)?
        };
        let micros = i128::from(end.timestamp_micros()) - i128::from(now.timestamp_micros());
        Ok(micros as f64 / 1_000_000.0)
    }
    pub fn due_start(
        &self,
        start: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<DateTime<Utc>, Error> {
        // Source CrontabSchedule.due_start_time first converts start to the
        // ModelEntry current timezone (UTC server in this known lane), then
        // remaining_delta applies cron fields without TzAware.is_due's initial
        // schedule-zone conversion. Preserve that exact start-gate boundary.
        if !(1970..=2037).contains(&start.year()) || !(1970..=2037).contains(&now.year()) {
            return Err(Error::Unsupported);
        }
        Ok(self
            .next_wall(start.naive_utc(), tz::local(&self.zone, now)?)?
            .and_utc())
    }
    pub fn due(&self, last: DateTime<Utc>, now: DateTime<Utc>) -> Result<(bool, f64), Error> {
        self.calculate(last, now, None)
    }
    /// Legacy reference11 explicitly injected fixed-offset nowfun. This test
    /// mode is separate from real Source's ZoneInfo production clock.
    #[cfg(test)]
    pub fn legacy_due(
        &self,
        last: DateTime<Utc>,
        now: DateTime<FixedOffset>,
    ) -> Result<(bool, f64), Error> {
        self.calculate(last, now.with_timezone(&Utc), Some(*now.offset()))
    }
    fn calculate(
        &self,
        last: DateTime<Utc>,
        now: DateTime<Utc>,
        fixed: Option<FixedOffset>,
    ) -> Result<(bool, f64), Error> {
        if !(1970..=2037).contains(&last.year()) || !(1970..=2037).contains(&now.year()) {
            return Err(Error::Unsupported);
        }
        let now_wall = if let Some(offset) = fixed {
            now.with_timezone(&offset).naive_local()
        } else {
            tz::local(&self.zone, now)?
        };
        let remaining = self.remaining(tz::local(&self.zone, last)?, now_wall, now, None)?;
        let due = remaining <= 0.0;
        let next = if due {
            self.remaining(now_wall, now_wall, now, fixed)?.max(0.0)
        } else {
            remaining
        };
        if !next.is_finite() {
            return Err(Error::Capacity);
        }
        Ok((due, next))
    }
}
pub(super) fn query_hour_offset(zone: &str) -> Result<i32, Error> {
    // Actual django-beat Source uses Jan1 2023, UTC server, int toward zero.
    let seconds = tz::offset(zone, DateTime::from_timestamp(1672574400, 0).unwrap())?;
    Ok(-seconds / 3600)
}
