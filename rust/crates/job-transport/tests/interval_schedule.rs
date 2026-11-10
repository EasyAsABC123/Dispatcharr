//! Compare native decisions with actual Original controlled-clock observations.
use dispatcharr_job_transport::interval_schedule::{
    Interval, IntervalEntry, IntervalUnit, PersistenceEffect,
};
use serde_json::Value;

#[test]
fn actual_original_interval_decisions_and_requested_effects() {
    let source: Value =
        serde_json::from_str(include_str!("../datasets/interval-source-1.json")).unwrap();
    assert_eq!(source["passed"], true);
    let now = chrono::DateTime::parse_from_rfc3339(source["fixed_now"].as_str().unwrap())
        .unwrap()
        .timestamp_micros();
    let cases = source["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 25);
    for case in cases {
        let input = &case["input"];
        let offset = |key: &str| input[key].as_i64().map(|offset| now + offset);
        let enabled = input["enabled"].as_bool().unwrap_or(true);
        let count = input["total_run_count"].as_u64().unwrap_or(0);
        let unit = match input["period"].as_str().unwrap_or("seconds") {
            "days" => IntervalUnit::Days,
            "hours" => IntervalUnit::Hours,
            "minutes" => IntervalUnit::Minutes,
            "seconds" => IntervalUnit::Seconds,
            "microseconds" => IntervalUnit::Microseconds,
            value => panic!("Unexpected Source interval unit {value}"),
        };
        let entry = IntervalEntry {
            enabled,
            last_run_at_micros: offset("last_offset_us").unwrap_or(now - 3_600_000_000),
            start_at_micros: offset("start_offset_us"),
            expires_at_micros: offset("expires_offset_us"),
            one_off: input["one_off"].as_bool().unwrap_or(false),
            total_run_count: count,
            interval: Interval::new(
                u32::try_from(input["every"].as_u64().unwrap_or(3600)).unwrap(),
                unit,
            )
            .unwrap(),
        };
        let decision = entry.evaluate(now).unwrap();
        assert_eq!(
            decision.due,
            case["due"].as_bool().unwrap(),
            "{}",
            input["name"]
        );
        assert_eq!(
            decision.next_seconds,
            case["next_seconds"].as_f64().unwrap(),
            "{}",
            input["name"]
        );
        // Project the requested mutation only. This is not a native SQL commit.
        let (projected_enabled, projected_count, no_changes) = match decision.persistence {
            PersistenceEffect::Unchanged => (enabled, count, false),
            PersistenceEffect::DisableExpired => (false, count, true),
            PersistenceEffect::DisableCompletedOneOff => (false, 0, false),
        };
        assert_eq!(
            projected_enabled,
            case["persisted_enabled"].as_bool().unwrap(),
            "{}",
            input["name"]
        );
        assert_eq!(
            projected_count,
            case["persisted_total_run_count"].as_u64().unwrap(),
            "{}",
            input["name"]
        );
        assert_eq!(
            no_changes,
            case["entry_no_changes"].as_bool().unwrap(),
            "{}",
            input["name"]
        );
    }
}

#[test]
fn native_capacity_is_explicit_without_truncating_decisions() {
    use dispatcharr_job_transport::interval_schedule::IntervalError;
    assert_eq!(
        Interval::new(u32::MAX, IntervalUnit::Seconds),
        Err(IntervalError::DatabaseIntegerRange)
    );
    assert_eq!(
        Interval::new(i32::MAX as u32, IntervalUnit::Days),
        Err(IntervalError::DurationCapacity)
    );
    let mut entry = IntervalEntry {
        enabled: true,
        last_run_at_micros: 0,
        start_at_micros: Some(i64::MAX),
        expires_at_micros: None,
        one_off: false,
        total_run_count: 0,
        interval: Interval::new(1, IntervalUnit::Seconds).unwrap(),
    };
    assert_eq!(entry.evaluate(0), Err(IntervalError::ClockDistanceCapacity));
    entry.start_at_micros = None;
    entry.last_run_at_micros = i64::MAX;
    assert_eq!(entry.evaluate(0), Err(IntervalError::ClockDistanceCapacity));
    entry.enabled = false;
    assert_eq!(entry.evaluate(0).unwrap().next_seconds, 5.0);
}
