# Interval scheduler decision component

Original Dispatcharr authors and contributors retain all application and algorithm
credit. Celery and django-celery-beat authors retain framework credit and licenses.
The existing AGPL licensing remains in force.

`source-original-interval-model-1.json` records 25 actual Original `ModelEntry`
decisions on owned copied-database rows. Each entry receives the same explicit UTC
instant through `_default_now` and its schedule's public `nowfun` dependency.
These are controlled-clock observations, not elapsed transitions. Owned rows are
removed, the default database is unchanged, and independent Compose resource
checks confirm project removal.

The Rust `interval_schedule` component compares its decision and requested
persistence effect against those observations. It preserves:

- Disabled entries' five-second delay, ahead of start/expiry/one-off checks.
- Future starts' ceiling to whole seconds, ahead of expiry and completed one-offs.
- Expiry at the exact boundary, disabling without clearing the run count and with
  `no_changes=true`.
- Completed one-offs' disable/count reset and `no_changes=false`.
- Interval boundary, overdue, future-last-run, zero-duration and microsecond
  behavior. The captured units are days, minutes, seconds and microseconds.

A requested effect is a value, not a committed SQL mutation. A native model test
projects that value for comparison with the Original persisted fields; it does
not claim native database, changed-sentinel or publication parity. The component
samples no clock and uses one explicit timestamp. Production clock sampling,
calendar/timezone behavior and moving-clock transitions remain separate work.

Native durations and positive clock distances have a maximum of 2^53
microseconds, so Float64 conversion never first rounds an inexact integer.
Larger values return explicit capacity errors; no interval is truncated or
clamped into a successful decision. `every` also obeys the database's signed
32-bit nonnegative integer range. These are native operational bounds, not
Original behavior or accepted parity outside the captured corpus.

Reproduce the Original reference with:

```sh
python3 scripts/rust-job-contracts/run_scheduler_interval.py \
  --output artifacts/rust/database-scheduler-interval-local
```

The targeted Rust comparison runs with:

```sh
cargo test --manifest-path rust/Cargo.toml -p dispatcharr-job-transport \
  --test interval_schedule
```

Full native scheduling still requires bounded database loading and retained
writes, row-disable persistence and change-marker semantics, publication/sync
ordering, defaults and reload integration, timezone/calendar scheduling,
process/crash recovery, application packaging and comparative performance.
