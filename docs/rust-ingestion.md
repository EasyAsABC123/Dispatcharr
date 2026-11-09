# Ingestion migration and incremental verification

Full credit for Dispatcharr and its ingestion behavior belongs to the original
authors and contributors. The original code and tests remain the reference.
This document refines M6; it does not mark the milestone complete.

## Parser and file reader

`dispatcharr-ingest-core` ports `parse_extinf_line` and `iter_m3u_entries` from
`apps/m3u/tasks.py`. The first differential run passed 970 source cases covering
both quote forms, duplicate attributes, Unicode, display-name fallback, extended
directives, malformed entries and literal URL-prefix behavior. The
[report](../scripts/rust-ingest-parity/verified-parser-1.json) records the exact
tested implementation and oracle hashes. Existing source functions execute from
their AST; the oracle is not a new interpretation of their behavior.

The expanded [component comparison](../scripts/rust-ingest-parity/verified-components-4.json)
passed 1,733 source cases: 810 EXTINF, 160 assembled playlists, 500 stream hashes
and 263 download-head validation cases. Seven Rust unit/reader regressions passed.
The file reader bounds a raw line before allocation and preserves UTF-8
`errors="ignore"` plus universal newlines. Stream identities preserve sorted,
spaced, ASCII Python JSON, XC stream-ID identity and forced account separation.
The report records the host Python runtime used for AST execution; the separate
database proof runs the original writer inside its pinned Python container.

Next verification commands, after the isolated playback trials finish:

```sh
cargo test --manifest-path rust/Cargo.toml -p dispatcharr-ingest-core
cargo build --manifest-path rust/Cargo.toml -p dispatcharr-ingest-core --example ingest_parity
python scripts/rust-ingest-parity/run.py --output artifacts/rust/NEW-INGEST/parity.json
```

The expanded oracle adds original `Stream.generate_hash_key` cases. File-reader
tests use tiny buffer capacities to split UTF-8 code points and CRLF boundaries,
and reject an oversized line before reading the entire source. Compressed-file
decoders and network readers must preserve the same bounds before being wired.

The unwired `import_m3u_batch` sink passed its first
[real-schema comparison](../scripts/rust-ingest-parity/verified-batch-sql-4.json).
It bounds each call to 2,000 rows and 8 MiB of
serialized input, deduplicates hashes in encounter order, separates creates,
changes and last-seen touches, and commits those writes in one transaction.
Reported creation counts intentionally describe source attempted creates even
when a concurrent unique conflict skips a row. Existing account ownership,
profile selection, local files and statistics remain outside the changed-field
list, as in the original batch writer. Five source scenarios passed, including
unchanged-row timestamp behavior, cross-account hash ownership and complete
rollback on a foreign-key failure. Fetching,
projection, scheduling and progress still need integration before any refresh
route can call the native sink.

## Fetch, cache and compression

The next slice ports `fetch_m3u_lines` and `_open_m3u_text_source` using the actual
provider/user-agent selection, 30-second connect and 60-second read timeouts,
cache-hit behavior, download status messages, content validation and atomic
temporary-file promotion. Plain, gzip, XZ and ZIP uploads need separate original
captures, including member-selection and malformed/truncated input behavior.
The source streams normal downloads to disk; a Rust comparison must include that
optimized source path rather than compare against an invented whole-file loader.

Verify rows, statuses and progress events against both backends using local
fixture servers. Cover redirects, disguised HTTP-200 HTML, empty files, invalid
UTF-8, authentication errors, nonstandard status codes, read stalls, interrupted
downloads, stale caches and restart while a temporary file exists. Bound total
download/extracted bytes and concurrent jobs explicitly; report resource-limit
errors instead of silently importing truncated data.

## Row projection and database batches

Port `process_m3u_batch_direct`, `process_xc_category_direct` and their actual
field semantics before changing scheduled refresh routing. Golden checks must
include field-length truncation by Unicode character count, provider URL limits,
ordered first-matching include/exclude filters, disabled groups, catch-up/radio
flags, XC stream IDs, decimal channel numbers and custom VLC options. Python
regular-expression behavior remains a compatibility requirement; Rust's simpler
regex syntax must not silently replace patterns with lookarounds or backreferences.

Each batch must reproduce first-entry hash deduplication, global stream-hash
uniqueness, conflict handling and the difference between changed rows and
last-seen-only touches. Test transaction rollback and the one sequential retry
of failed batches. A post-commit progress-notification failure must not cause
the batch to run again. Use a bounded work queue instead of retaining all pending
batch futures; verify identical committed rows and counts before claiming memory
improvements.

Keep stale cleanup, channel-group ownership, catch-up rollups, automatic channel
numbering/association and provider profile refresh as explicit gates. In
particular, overlapping source stale/group deletions can make reported counts
differ from distinct deleted rows; use source observations, not an assumed count.

## Guide, jobs and recovery

Guide ingestion must preserve complete-file validation, gzip/XZ/ZIP detection,
HTML entities, channel matching, programme timestamps, duplicates, icons and
progress calculation. A truncated guide must not replace valid cached programme
data. The source already checks only an 8-KiB tail for the closing XMLTV root;
retain bounded file operations and measure the real source implementation.

Scheduled execution must reproduce database-backed schedules, Redis job locks
and renewal, cancellation, orphan task cleanup, final account/source statuses,
notifications and cross-worker WebSocket progress. Capture source lock keys and
failure ordering before choosing a Rust worker transport. Until these contracts
pass, task-triggering APIs keep explicit compatibility guards.

## Performance acceptance

Run repeated paired imports at increasing playlist/guide sizes, then repeat
while valid playback is active. Record parsed/created/changed/touched/deleted
counts, database rows and side effects as well as elapsed time, per-service CPU,
container memory, queue depth, spool-disk use and cleanup after cancellation.
Include PostgreSQL, Redis, worker and fixture resources. Parser-only throughput
does not establish import-job savings or full-application performance acceptance.

The latest component gate passed [2,133 source comparisons](../scripts/rust-ingest-parity/verified-components-5.json), including 400 row projections. The [six-scenario real PostgreSQL gate](../scripts/rust-ingest-parity/verified-batch-pipeline-sql-5.json) now joins the streaming parser, typed projection and batch sink, and checks the resulting rows against the original container. These gates do not enable scheduled refresh APIs.

The next working-tree increment stages decoded HTTP chunks to disk with only a
32-KiB validation prefix in memory. Empty, invalid, oversized and dropped
downloads retain the old cache and remove their staging file. An existing
`.tmp` file is preserved until account-lock recovery establishes ownership;
unowned staging-file recovery is an explicit remaining job integration gate.
HTTP status handling, authentication, download progress and final job events are
still separate from this disk component.

Uploaded files now have a streaming reader for case-sensitive `.gz`, `.xz` and
`.zip` suffixes. ZIP selection preserves the first matching lowercase `.m3u`
name and Python's last-entry lookup for duplicate names, without extracting
provider-controlled paths. Stored, deflate, bzip2 and ZIP-LZMA members are covered.
The XZ reader uses statically linked liblzma through Rust and accepts legacy
LZMA streams, concatenated members and the original trailing-data behavior.
Expanded bytes, source bytes, XZ dictionary memory and ZIP metadata have explicit
limits; an exhausted limit fails the refresh. The calling job must consume the
reader completely before committing rows, so a late CRC error cannot publish a
partial refresh. The [original-runtime comparison](../scripts/rust-ingest-parity/verified-preparation-original-runtime-14.json) passed all 2,283 cases under the pinned image’s Python3.13.18/Unicode15.1, including 40 upload, 30 download-cache and 80 parsed-cache cases. Seventeen focused Rust tests cover bounds and late failure recovery. Container job integration remains pending; this is not a completed M6 or import performance result.

Provider group synchronization also passed [eleven real-source SQL scenarios](../scripts/rust-ingest-parity/verified-groups-batch-sql-13.json), alongside the six stream-batch checks. The sink preserves existing user settings and disabled flags, normalizes legacy JSON only where the source does, merges only `xc_id`, and retains the source’s separate group/create/update write boundaries on failure. Stale cleanup, channel rollups and full refresh orchestration remain open.

Stale stream cleanup passed [four additional real-source SQL cases](../scripts/rust-ingest-parity/verified-cleanup-groups-batch-sql-15.json), retaining overlap in reported counts, null-group handling, exact cutoff boundaries, account isolation and channel catchup rollups. An old stream in a disabled group contributes twice to the source’s reported total and is deleted once. Native deletion uses bounded ID batches inside each original deletion transaction. Public job triggers remain gated.

The [latest real-schema suite](../scripts/rust-ingest-parity/verified-group-cleanup-sql-17.json) passes 27 scenarios including stale group/orphan cleanup and Python date overflow. Other provider associations and direct channels protect groups; streams and overrides alone receive SET_NULL while preserving timestamps. The [real HTTP/cache comparison](../scripts/rust-ingest-parity/verified-http-fetch-original-16.json) passes thirteen pinned-runtime cases. The shared codec extraction has [separate lifecycle regressions](../scripts/rust-integration/verified-shared-upstream-extraction-1.json). Full refresh orchestration, progress/events, ordered filtering and import resource acceptance remain open.

Filter implementation selection has an explicit [rejected-candidate report](../scripts/rust-ingest-parity/verified-filter-candidate-rejection-18.json): 500 ordered-policy cases agree, but an unmodified rich Rust regex candidate disagrees on 50 of 154 builtin-Python-re cases. It remains test-only. Anchors, Unicode categories/boundaries, ASCII modes, octal escapes and syntax rejection need exact compatibility; native refresh routing stays gated. Preview/transform uses the separate third-party Python regex package and is not accepted by this corpus.

The [bounded parsed-cache reader gate](../scripts/rust-ingest-parity/verified-cached-reader-19.json) reruns all 2,283 original-runtime cases with the new reader and passes 22 core tests plus strict all-target Clippy. It validates the complete JSON file before exposing any entries to a database consumer, then reads the same descriptor in a second bounded pass. Truncated tails, field/count limits, atomic replacement and callback failure/retry have explicit recovery coverage. Refresh orchestration and its resource comparison remain open.

The internal [parsed-cache/batch bridge](../scripts/rust-ingest-parity/verified-cache-batch-pipeline-20.json) passes 28 real-schema source scenarios, including 1,501 raw entries whose first batch belongs entirely to disabled groups. Raw-entry slicing remains 1,500 before projection; a bounded channel carries projected batches into the existing SQL sink. Corrupt-tail and configured-filter guards leave existing database rows unchanged. Four workers, byte-capped batches and a one-batch queue bound the work retained by this component. That first version stopped on the first SQL error. The [retry gate](../scripts/rust-ingest-parity/verified-cache-retry-25.json) now passes 30 real-schema scenarios: all first-pass batches are attempted, then failed batches are retried once in order by rereading the same validated descriptor. Actual PostgreSQL faults verify permanent and transient failures and commit order. Only bounded failed indices are retained; a nonzero failed-batch count forbids completion and destructive stale cleanup. Cancellation is checked during validation and replay. Full task completion, cleanup orchestration, auto-sync and progress/event policy remain open; this component is not routed as a refresh job.

A separate [unselected compatibility prototype](../scripts/rust-ingest-parity/verified-regex-research-21.json) matches 2,252 original-runtime regex cases after pinning Unicode categories and case maps. Nine external-supervisor checks distinguish normal matching, compile errors, input caps and resource termination. This research executes trusted CPython standard-library compiler source inside a Rust interpreter, with a native Rust matcher; it is not a pure-Rust compiler implementation or a selected product dependency. A production Rust supervisor, full packaging/provenance, broader dialect tests and real refresh-job evidence remain open. No filter or refresh route uses it, and no performance gain is claimed for it.

The [job-lock component gate](../scripts/rust-ingest-parity/verified-task-locks-23.json) exercises actual original helpers and native leases on disposable Redis. Shared key naming, default 300-second TTL and mutual exclusion work in both directions. Delayed-write cancellation retains physical admission through cleanup; old-owner renewal/release preserves a replacement owner. Native owner tokens and atomic compare operations deliberately strengthen the original literal `locked` value and unconditional delete. Legacy workers can still delete a native lock, so overlapping old/new job execution remains a cutover gate. This component does not enable a scheduler or account-refresh endpoint.

The [frozen preparation replay](../scripts/rust-epg-parity/verified-frozen-preparation-28.json) matches 2,319 original-runtime playlist cases and 1,594 XMLTV timestamp, completeness and prefix cases, with 23 core tests and strict Clippy. Numeric conversion preserves Python's 25 numeric whitespace characters separately from its 29 string-strip characters. XMLTV completeness remains the original suffix heuristic, and prefix injection preserves the exact HTML4 doctype. XML decoding and programme/database replacement have the separate component evidence below; refresh-job integration remains open. The replay is built from an immutable archive; it supersedes an earlier numeric proof whose post-run source hashes could overlap a later edit.

The [XMLTV metadata projection gate](../scripts/rust-epg-parity/verified-metadata-30.json) matches 1,208 actual original-runtime programme-tree cases and passes 27 core tests plus strict Clippy from a fresh dedicated build target. It preserves categories, episodes beyond signed 64-bit range, rating/credit/media fields and original-air-date precedence. Trees have explicit node, depth and byte caps. This is projection after XML decoding; the native streaming XML parser has separate component and Compose evidence below; full guide/job acceptance remains open.

The [native XML library](../scripts/rust-epg-parity/verified-native-library-33.json) matches 1,144 original lxml recovery/entity/chunk cases. The [immutable Compose gate](../scripts/rust-epg-parity/verified-compose-image18-1.json) repeats them from image18's exact retained source archive in a read-only, nonroot, network-isolated helper image. Original libxml2 2.14.6 is vendored with its verified upstream checksum and full notices. Nine failure/cancellation/concurrency/safe-reader tests and 1,144 production-bridge address/leak sanitizer cases pass. The standalone Rust scanner peaks near4MiB RSS on53.8/107.6MB inputs; this is a parser bound, not a full refresh-job resource comparison.

The [programme projection](../scripts/rust-epg-parity/verified-programme-projection-35.json) matches 2,432 rows produced by the original streaming-loop AST. The [history policy](../scripts/rust-epg-parity/verified-history-policy-36.json) matches1,125 original coercion cases. The [mapping/staging gate](../scripts/rust-epg-parity/verified-guide-mapping-swap-39.json) matches15 actual PostgreSQL cases, including base/override mappings, null/empty identifiers, retention and first-start boundaries, empty-result preservation, live orphan cleanup and atomic rollback. SQL operations retain their private session, admission and supplied job guard through actual replies after caller cancellation. Full file-to-database orchestration, progress/events, job dispatch, lock-loss/retry rules and resource acceptance remain open.
