//! Controlled-clock interval due-state component derived from Original ModelEntry.
//! Original Dispatcharr, Celery and django-celery-beat authors retain credit.
//! This computes a decision and requested persistence effect. It performs no SQL,
//! publication, clock sampling or scheduler startup, and grants no app authority.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntervalUnit {
    Days,
    Hours,
    Minutes,
    Seconds,
    Microseconds,
}
impl IntervalUnit {
    fn micros(self) -> i64 {
        match self {
            Self::Days => 86_400_000_000,
            Self::Hours => 3_600_000_000,
            Self::Minutes => 60_000_000,
            Self::Seconds => 1_000_000,
            Self::Microseconds => 1,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntervalError {
    DatabaseIntegerRange,
    DurationCapacity,
    ClockDistanceCapacity,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Interval {
    micros: i64,
}
impl Interval {
    /// PostgreSQL PositiveIntegerField range; durations beyond native i64
    /// microsecond/Float64 exact-integer capacity are explicit errors, never
    /// shortened intervals. This component does not claim unbounded calendar
    /// or large-duration floating-point equivalence.
    pub fn new(every: u32, unit: IntervalUnit) -> Result<Self, IntervalError> {
        if every > i32::MAX as u32 {
            return Err(IntervalError::DatabaseIntegerRange);
        }
        let micros = i64::from(every)
            .checked_mul(unit.micros())
            .ok_or(IntervalError::DurationCapacity)?;
        if micros > 9_007_199_254_740_992 {
            return Err(IntervalError::DurationCapacity);
        }
        Ok(Self { micros })
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IntervalEntry {
    pub enabled: bool,
    pub last_run_at_micros: i64,
    pub start_at_micros: Option<i64>,
    pub expires_at_micros: Option<i64>,
    pub one_off: bool,
    pub total_run_count: u64,
    pub interval: Interval,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PersistenceEffect {
    Unchanged,
    /// Source _disable(): enabled=false, no_changes=true; retain run count.
    DisableExpired,
    /// Source completed one-off: enabled=false, count=0, no_changes=false.
    DisableCompletedOneOff,
}
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DueDecision {
    pub due: bool,
    pub next_seconds: f64,
    pub persistence: PersistenceEffect,
}
impl IntervalEntry {
    /// One explicit UTC clock value, matching the controlled Source corpus.
    /// Real scheduler clock sampling, timezone/calendar behavior and committing
    /// the requested effect require separate application integration gates.
    pub fn evaluate(self, now_micros: i64) -> Result<DueDecision, IntervalError> {
        let wait = |next_seconds, persistence| DueDecision {
            due: false,
            next_seconds,
            persistence,
        };
        if !self.enabled {
            return Ok(wait(5.0, PersistenceEffect::Unchanged));
        }
        if let Some(start) = self.start_at_micros {
            if now_micros < start {
                let remaining = i128::from(start) - i128::from(now_micros);
                if remaining > 9_007_199_254_740_992 {
                    return Err(IntervalError::ClockDistanceCapacity);
                }
                let ceil_seconds = (remaining as f64 / 1_000_000.0).ceil();
                return Ok(wait(ceil_seconds, PersistenceEffect::Unchanged));
            }
        }
        if self.expires_at_micros.is_some_and(|end| now_micros >= end) {
            return Ok(wait(100_000_000.0, PersistenceEffect::DisableExpired));
        }
        if self.one_off && self.total_run_count > 0 {
            return Ok(wait(
                100_000_000.0,
                PersistenceEffect::DisableCompletedOneOff,
            ));
        }
        let remaining = i128::from(self.last_run_at_micros) + i128::from(self.interval.micros)
            - i128::from(now_micros);
        if remaining <= 0 {
            Ok(DueDecision {
                due: true,
                next_seconds: self.interval.micros as f64 / 1_000_000.0,
                persistence: PersistenceEffect::Unchanged,
            })
        } else {
            if remaining > 9_007_199_254_740_992 {
                return Err(IntervalError::ClockDistanceCapacity);
            }
            Ok(wait(
                remaining as f64 / 1_000_000.0,
                PersistenceEffect::Unchanged,
            ))
        }
    }
}
