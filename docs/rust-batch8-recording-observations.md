# Candidate8 recording cohort observations

Original Dispatcharr and dependency authors retain all application, algorithm,
protocol and license credit. React remains unchanged.

The packaged runtime is `cb1ba69af8e0623222391a707a2b9fc03b5995f6`, image
`sha256:b6ecb538995846c960066f1563e1ffc8ce407c7fd10b88df52eae7295a947c4d`.
The [selected raw evidence](../scripts/rust-milestone-batches/m478/batch8-recording/manifest.json)
retains failed controller results, exact exits and cleanup. It is a partial
publication, not a portable closure of every fixture input or full acceptance.

The Source/Native recording batch exited1 after1639.762 seconds. Its single
exclusive lease used a shared1800-second functional deadline and separate bounded
cleanup. All12 exact container identities, both Compose project inventories and
recorded host process generations were absent afterward. Every recorded CLI and
controller process group settled with pipe EOF; cleanup reported no errors.
No compilation or other application fixture ran alongside this batch.

Earlier setup attempts remain failures: attempt3 lacked the installed Docker
Compose plugin configuration; attempt4's internal-only rootless bridge suppressed
host port bindings despite correct configured localhost mappings; attempt5 used
`psycopg2` instead of the pinned image's `psycopg`. The latter two attempts cleaned
up their owned containers and networks. No application parity is inferred from
those setup attempts.

## Observed application behavior

- Native passed its recording authorization context, including the nine captured
  anonymous, inactive, viewer, manager, admin and hidden-channel checks.
- Native passed selective stop, delete and a60-second extension. Both backends'
  final captures show one cancellation event, zero late duplicates, the ordinary
  viewer preserved and all recording children retired. Source's raw context
  result remains failed because teardown log collection raised an error.
- Both backends passed12 active HLS playback checks. Native's final playback
  capture has19 passed cells, two unqualified exceptional-range transports and
  one failed denial cell caused by the incorrectly configured fixture policy.
  This does not qualify physical slow-reader shutdown or the full playback matrix.
- Source's grouped scanner, recurring, expiration, cleanup and empty-statistics
  leaf reports passed. Native cleanup passed, but its scanner descendants remained
  queued; later families timed out behind the oldest Groups5 message. This is a
  functional failure requiring diagnosis, separately from observer defects.
- A held actual SQL write produced Native exit1 with `oom_killed:false`, as
  intended. Its final observer report was interrupted by log collection, so the
  complete held-drain case is unqualified.

## Required corrections and next batch

The observer queried bare task-result keys instead of the configured
`celery-tasks:` prefix. Its denial setup wrote `system_settings.allowed_networks`
instead of Source's `network_access` group with comma-separated strings. Docker
`cp` could not read the role logs from the rootless tmpfs even though guest reads
could; these collection failures also masked primary exceptions. Decoder report
names collided when isolated databases reused recording IDs.

The prepared replacement driver fixes these issues, preserves primary and
teardown errors separately, and uses bounded guest log metadata rather than
publishing complete logs. Qualified Source authorization, grouped-family and
control observations can be reused only with explicit evidence references; their
failed raw controller statuses are not rewritten. Failed and dependent Source
cases, the complete Native cohort and the scanner queue correction still require
execution against the next built candidate. Core scaling and normal Beat changes
also require their grouped acceptance cases.

No new CPU or memory reduction is accepted. This batch does not close M0–M9,
full scheduling/task/role coverage, SQLite compatibility, soak, restore or rollback.
