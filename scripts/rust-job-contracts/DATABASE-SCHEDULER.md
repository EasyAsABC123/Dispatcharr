# Original database scheduler reference

All application credit belongs to the original Dispatcharr authors and
contributors. Celery and django-celery-beat authors retain framework credit and
licenses. This capture changes no application or scheduler implementation.

[The Original-only gate](verified-original-database-scheduler-2.json) passes30
checks using a copied database and isolated broker8/result9 in Docker Compose.
The later [41-check capture](verified-original-database-scheduler-3.json) repeats
these checks and records11 explicit clock vectors for minute boundaries, leap
day, weekday/list/step expressions, New York DST gap/fold, Lord Howe's half-hour
gap and Kathmandu's quarter-hour offset. Each vector supplies `nowfun` and
last-run inputs to the unmodified Original calculation and records due/next.
These are reference outputs, not native calendar parity or elapsed transitions.
Nine real ModelEntry cases cover due/not-due intervals, disabled/future-start/
expired entries, first/completed one-off entries and invalid args/kwargs JSON.
Expired and invalid entries are disabled; completed one-off entries also reset
their run count. The delays are observations of the real fixture clock, not
elapsed-hour or deterministic calendar-vector acceptance.

Actual DatabaseScheduler.tick publishes the scheduled-backup task with retention
kwargs and periodic-task headers. Sync persists its last-run timestamp/count.
A second immediate tick and a reconstructed scheduler do not republish before
the next due time. No worker executes the publication or creates a backup.

Two schedulers that preload the same due row each publish a distinct task. Their
stale final database count is1 despite two publications. This controlled overlap
does not establish distributed ownership or an OS-process concurrency guarantee.
Separately, a real broker AUTH rejection raises SchedulingError, enqueues no task,
and still advances the saved last-run/count after sync. A native implementation
must not assume database reservation and publication are atomic.

Construction uses lazy=True to isolate tick/reserve/sync from default schedule
installation. Both runtime objects and their actual database/broker operations
are Original code. Owned task/interval rows are removed, prior enabled states
are restored in the copied database, and the untouched default database manifest
is checked after execution. Fixture projects and volumes are removed.

Native crontab/DST/timezone equivalence, elapsed transitions, timezone query
filtering, default initialization, full scheduler startup,
disabled-entry reloading, OS-process crashes, publication/sync crash windows,
backup execution/retention, native implementation, packaging and performance
remain separate gates. Reconstruction in this process is not a process restart
or a completed scheduling milestone.

```sh
python3 scripts/rust-job-contracts/run_scheduler.py \
  --output artifacts/rust/database-scheduler-local
```

[The eager startup/reload reference](verified-original-database-scheduler-startup-1.json)
passes 16 further actual Original checks. Full `DatabaseScheduler(lazy=False)`
installs five configured application schedules plus `celery.backend_cleanup`;
construction and reconstruction publish nothing and produce no task results.
After defaults are disabled with an explicit change marker, a model create/save
reloads the owned interval entry, while `QuerySet.update(enabled=False)` alone
leaves its cached enabled model present. Updating `PeriodicTasks` removes it; an
ordinary save re-enables it and deletion removes it. Reconstructing the scheduler
creates no duplicate default names. This is an in-process reference, with no
OS-process restart or elapsed-time guarantee.

The capture deliberately clears and changes periodic-task rows only in its
disposable copied database; that copy is removed with the Compose project. It
does not claim copied-row restoration. The original default database remains
unchanged, and independent container/network/volume checks confirm cleanup.

```sh
python3 scripts/rust-job-contracts/run_scheduler_startup.py \
  --output artifacts/rust/database-scheduler-startup-local
```

The earlier lazy-only reference's startup/reload omissions now have these scoped
Original observations. Native startup, timezone filtering, trigger execution,
crash windows, packaging and performance still require their own gates.

The [bounded interval decision component](INTERVAL-SCHEDULER.md) now matches25
actual controlled-clock Original cases and projected mutation effects. This
adds no native SQL persistence, publication, startup or scheduler authority.
