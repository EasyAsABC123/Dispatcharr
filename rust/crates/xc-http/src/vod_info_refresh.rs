//! Source XC info refresh eligibility only; no refresh or route activation.
//! Original Dispatcharr authors retain application/algorithm credit. The
//! pinned views.py checks Movie and Series in different evaluation orders.
//! A caller must preserve CheckFailed as the Source caught-exception path,
//! not reinterpret it as proof of fresh metadata or successful refresh.

const DAY_MICROS: i64 = 86_400_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Flags {
    Mapping {
        detailed: bool,
        episodes: bool,
    },
    /// Source `(custom_properties or {}).get(...)` fails for this shape.
    TruthyNonMapping,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Decision {
    Fresh,
    RefreshRequired,
    /// Source catches this error around refresh/check/projection, then
    /// continues its later output phases. Those phases still need validation.
    CheckFailed,
    /// Outside this finite timestamp grammar; never invent a cached 200.
    UnsupportedClock,
}

fn expired(last: i64, now: i64) -> Option<bool> {
    Some(last < now.checked_sub(DAY_MICROS)?)
}

pub(crate) fn movie(flags: Flags, last: Option<i64>, mut now: impl FnMut() -> i64) -> Decision {
    let Flags::Mapping { detailed, .. } = flags else {
        return Decision::CheckFailed;
    };
    // Python `or` stops before timezone.now() when either earlier operand
    // requests a refresh. Equality at exactly 24h is deliberately fresh.
    if !detailed || last.is_none() {
        return Decision::RefreshRequired;
    }
    match expired(last.expect("checked timestamp"), now()) {
        Some(true) => Decision::RefreshRequired,
        Some(false) => Decision::Fresh,
        None => Decision::UnsupportedClock,
    }
}

pub(crate) fn series(flags: Flags, last: Option<i64>, mut now: impl FnMut() -> i64) -> Decision {
    // Source computes should_refresh before inspecting either property flag.
    let stale = match last {
        None => true,
        Some(value) => match expired(value, now()) {
            Some(value) => value,
            None => return Decision::UnsupportedClock,
        },
    };
    let Flags::Mapping { detailed, episodes } = flags else {
        return Decision::CheckFailed;
    };
    if !episodes || !detailed || stale {
        Decision::RefreshRequired
    } else {
        Decision::Fresh
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const COMPLETE: Flags = Flags::Mapping {
        detailed: true,
        episodes: true,
    };

    #[test]
    fn strict_day_boundary_and_future_timestamp() {
        for last in [1, 2, DAY_MICROS + 1] {
            assert_eq!(
                movie(COMPLETE, Some(last), || DAY_MICROS + 1),
                Decision::Fresh
            );
            assert_eq!(
                series(COMPLETE, Some(last), || DAY_MICROS + 1),
                Decision::Fresh
            );
        }
        assert_eq!(
            movie(COMPLETE, Some(0), || DAY_MICROS + 1),
            Decision::RefreshRequired
        );
        assert_eq!(
            series(COMPLETE, Some(0), || DAY_MICROS + 1),
            Decision::RefreshRequired
        );
    }

    #[test]
    fn movie_clock_is_short_circuited_after_flags_and_missing_timestamp() {
        assert_eq!(
            movie(COMPLETE, None, || panic!("must not read clock")),
            Decision::RefreshRequired
        );
        assert_eq!(
            movie(
                Flags::Mapping {
                    detailed: false,
                    episodes: true
                },
                Some(0),
                || panic!("must not read clock")
            ),
            Decision::RefreshRequired
        );
        assert_eq!(
            movie(Flags::TruthyNonMapping, Some(0), || panic!(
                "must not read clock"
            )),
            Decision::CheckFailed
        );
    }

    #[test]
    fn series_reads_clock_before_flags_even_when_flags_fail() {
        let mut calls = 0;
        assert_eq!(
            series(Flags::TruthyNonMapping, Some(0), || {
                calls += 1;
                DAY_MICROS + 1
            }),
            Decision::CheckFailed
        );
        assert_eq!(calls, 1);
        assert_eq!(
            series(Flags::TruthyNonMapping, None, || panic!(
                "must not read clock"
            )),
            Decision::CheckFailed
        );
    }

    #[test]
    fn each_series_flag_is_required() {
        for flags in [
            Flags::Mapping {
                detailed: false,
                episodes: true,
            },
            Flags::Mapping {
                detailed: true,
                episodes: false,
            },
        ] {
            assert_eq!(
                series(flags, Some(1), || DAY_MICROS + 1),
                Decision::RefreshRequired
            );
        }
    }

    #[test]
    fn arithmetic_overflow_is_explicitly_unsupported() {
        assert_eq!(
            movie(COMPLETE, Some(0), || i64::MIN),
            Decision::UnsupportedClock
        );
        assert_eq!(
            series(COMPLETE, Some(0), || i64::MIN),
            Decision::UnsupportedClock
        );
    }
}
