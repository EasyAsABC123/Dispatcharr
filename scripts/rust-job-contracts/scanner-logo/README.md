# Original logo scanner: bounded Source contract

This is an actual pinned Dispatcharr Celery-worker reference, not a native implementation. Original Dispatcharr, Django, Celery, Kombu, Channels and Redis authors retain their authorship and licenses. Source revision `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` and archive digest are checked by the runner. The public committed `scripts/rust-core-perf/support` supplies the disposable fixture.

```sh
# Requires Python3 and Docker Compose; use your configured Docker environment.
python3 scripts/rust-job-contracts/scanner-logo/run_pair.py \
  --repo . --support-root scripts/rust-core-perf/support \
  --output artifacts/rust/scanner-logo-original-fresh
```

The executed runner retains an inherited two-family docstring and unused resource flags. Use only the invocation above: the actual `integration.json` and source script define the scanner-only scope. No native binary is accepted or executed. Synthetic file contents are metadata fixtures, not decoded images. All files and SQL live in a disposable volume/copied database; default database manifests agree before/after. An internal Compose network prevents external provider access.

`source-logo-scanner-2.json` contains the exact scalar/typed reference, real SQL trace, rows, events, file hashes/mtimes and broker lineage. It preserves actual task UUIDs, row IDs and observed traversal/publication order; these are not sortable comparison keys. Events include the Channels routing wrapper. No handler/event replacement is used. The scanner's module-local clock is explicitly fixed to `1700000000.125` for age thresholds; Celery/backend clocks remain real. Redis marker TTL is three days; backend TTL is one hour.

Twelve cases passed 102 assertions:

- Nested Unicode names and uppercase supported extensions create stem-named Logo rows; unsupported files count as skipped. Exact Python float marker bytes match file mtime; file bytes/mtime remain unchanged.
- Same-process unchanged scans emit no summary and do not repeat first-scan index lookup. Age below six seconds skips; exactly six processes. Existing rows older than 30 seconds retain their name and receive a marker without a summary.
- Real P0001 logo INSERT failure is caught: normal null result, one error summary, no row/marker. Real marker SET denial is caught after the Logo commit: normal null result, error summary, committed row, no marker. Real Channels wrong-type failure is swallowed after the row/marker commit. Invalid stored float is outside the per-file catch: actual ValueError FAILURE, no row/event.
- First scan selects active non-dummy/non-SchedulesDirect sources with missing index or SQL NULL data; JSON `null` and `{}` are not selected. Two real Index children have actual root/parent lineage and default routing. Same-process second scan leaves those messages unchanged. Restart publishes two additional tasks for the same source IDs; all four messages are preserved until owned-project cleanup.

Only parent tasks consume an explicit fixture queue. Child publications retain real default routing and are deliberately **not consumed**. This qualifies publication and duplicate lineage, not Index execution or a whole scanner workflow. No queue is flushed or child deleted between cases. The final owned Compose project and its volumes are removed normally. Attempt1 remains retained: a missing initial observer group membership caused its event assertion failure; Source rows and markers had succeeded. Fresh2 fixes setup only.

## Minimal native contract plan, not yet implemented

First implement an inert bounded logo-only service with injected SQL, Redis, event sink and immutable clock/path context. Require empty M3U/EPG watch directories for this authority. Preserve lexical absolute URL, stem names, age/mtime comparisons, real traversal order, marker-before-counter behavior and exact event fields. Bound file/path count, total path bytes, row/query work and held memory before admitting work; out-of-domain directories/configuration remain explicit unsupported before mutations.

Per-file SQL transaction and marker/event commands must retain the caller and worker admission until physical replies, settled rollback/close and sink receipt. Preserve the observed committed Logo on failed marker SET and swallowed summary failure; do not fabricate a task failure or rollback already committed rows. Match invalid-marker failure only through an explicitly typed bounded result authority. Keep old SUCCESS/result TTL semantics in the existing supervised writer.

Keep first-scan state per execution-process lifetime, set only at the actual Source boundary, and publish Index children through existing narrow authority. Exact missing/SQLNULL selection, duplicate publications after restart, children result metadata and strict FIFO must be tested separately with real Index consumers before application attachment. Do not delete duplicates or quarantine children for an end-to-end completion claim.

Remaining Source gates before broader authority: mixed good/bad multi-file continuation and actual directory traversal ordering; changed/greater markers and float special values; extension/name truncation and path/symlink/stat errors; cold Redis initialization and GET/listing failure; first-scan helper SQL/publication partial failure; M3U/EPG creation signals, duplicate refresh descendants and periodic-row effects. Those branches are not covered by this logo subset. No Main/default registry, Rust implementation, image-decoding, provider/network, recording, performance or full-family acceptance is claimed.

The public case dataset omits only each `parent_publication` envelope wrapper; all other case fields and all102 checks equal the raw capture. The raw source report hash is retained. Parent-envelope wire parity is not established here.
