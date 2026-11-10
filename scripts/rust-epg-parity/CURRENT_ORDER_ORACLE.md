# Current-programme ordering oracle: Source97

Original Dispatcharr authors and contributors retain all application and algorithm credit. The accepted frozen96 file-reader report and goldens are unchanged. Source97 ran after the coordinated resource experiment ended; its isolated project was removed. Native adapter execution remains a separate gate.

Source-only command used:

```sh
source artifacts/rust/tooling/environment.sh
python3 scripts/rust-epg-parity/run_file_job.py \
  --source-helper current_order_source.py \
  --output artifacts/rust/m6-current-order-source-97
```

Use a fresh output suffix if a prior attempt exists. This is a Source-only command, not a native test. Its runner captures runnable tools and the pinned original source, starts an isolated original Compose fixture, and removes only that project. Native flags are deliberately rejected for this helper.

The fixture creates 64 distinct wide Source rows and 64 EPG entries, then moves selected heap tuples. Nine actual original Factory request cases include 64 ascending/descending/shuffled IDs, 50 shuffled IDs, duplicates before truncation, shuffled duplicates, missing IDs, integer strings and an invalid value after position50. Source converts the entire input list before taking its first50 positions; deduplication happens later in the IN lookup. Every retained array order remains observable. No body sorting or order normalization is allowed.

Each successful request captures its original SQL execution sequence and full response. The oracle compares actual response EPG IDs to the original full joined metadata query, records the narrowed native query shape only as a diagnostic, and captures same-database EXPLAIN/observed IDs under direct execution and prepared custom/generic/auto modes. The narrowed query is not actual native execution. Differences cannot be fixed by inventing an ID sort; derive a compatible query only after measuring these plans.

Eleven controlled schedules interpose writes after the original programme SELECT completes and before its lazy metadata SELECT: delete an EPG, delete a source, delete a cached stored programme, create a requested EPG and programme, create only a programme, change dummy to XML, null a source relation, replace an extracted path, delete/replace an index and atomically replace a file. Two further schedules change the fresh file clock or replace the index at that clock. Original view/helper bodies remain unchanged. The hook uses completed autocommit statements on the disposable connection; this establishes controlled statement ordering, not independent-process overlap timing or arbitrary concurrent-write acceptance. Capture clock/index/dispatch events alongside queries. The separate `current_lookup_source.py` draft covers missing/null/malformed index and source guards.

Recommended Store handoff:

1. Fetch the original ProgramData batch first using the original retained-ID subquery, retaining its first encountered stored programme per EPG.
2. Execute the full joined EPG/source metadata query afterwards; preserve its encountered order and the original cached-programme priority.
3. Materialize an ordered `Stored { epg_entry_id, source_dto: Bytes }` or `Lookup(CurrentFileRequest)` sequence, omitting only original proven skip branches.
4. Await transaction rollback and release its pooled connection before invoking any file callback. No nested file lookup may retain that transaction.
5. A `Lookup` callback receives the source snapshot, owning caller guard and EPG entry ID. It resolves path guards, samples a fresh clock, reads the index fresh, and returns retained `Found(Bytes)`, `Missing`, or `Parsing` only after accepted durable dispatch. Do not reuse the view's initial clock or an earlier index across entries.

The full projection affects PostgreSQL join width and observed order. Large source fields therefore need a separate physical row/transport budget gate; narrowing the projection to solve allocation cannot silently change ordering. Prepared-plan diagnostics, Source body order and resource limits must all remain explicit. The existing stable64 current-programme gate does not establish these statement schedules or concurrent-mutation behavior.

Source97 passed all nine request cases and thirteen controlled schedules. The original full metadata projection explained all eight valid response orders. Narrowed diagnostic SQL matched those orders in all 32 direct/prepared comparisons on this dataset; no projection mismatch was observed. The duplicate-before-truncation case returned `[2,5,1,6,4,3]`, demonstrating why sorting or retaining request order would be wrong. The query sequence and mutation effects above were observed; they are not inferred from the plan. See `verified-current-order-source-97.json` for raw evidence/tool digests and the explicit native/concurrency limits.

Source102 additionally passed a real no-hook source-relation lock/delete witness, restored the exact programme row and rechecked all nine initial bodies before native replay. Its four native Store tests cover nine original response orders, the mirrored lock schedule, pool-one callback reuse/capacity and canceled SQL rollback ownership. Source101 observer setup failure remains preserved. This does not accept all observed concurrent schedules.

Source104 adds two malformed stored-property schedules: deleting the EPG before metadata yields `200 []` without serialization; retaining it raises AttributeError only after metadata. A separate full-query-order fixture observes missing-index dispatch before a later malformed Stored serialization error. Fixture roles are assigned to actual encountered full-metadata order, with no result sorting or EPG-row updates. Its full query yielded `[80,79]` while a plain `values_list(id)` yielded `[79,80]`; the latter drops the join and is not identical to the former native id-plus-source_type projection. Source103 failed the original fixture assumption that insert order would determine output, and remains preserved. Native deferral correction needs its separate fresh gate.

Frozen106 passes all six native pipeline tests, nine response-order cases, 97 default Store tests (four ignored in that lib run) and strict all-target Clippy. It matches deleted-malformed-EPG omission and prior lookup effects before stored serialization failure. Real failed105 is retained: a SELECT used to set a local timeout pinned the repeatable-read snapshot before its relation lock, unlike the original. The correction uses utility SET LOCAL and explicit phase ACCESS SHARE locks before the first snapshot/preflight, then defers PgRow encoding to the ordered metadata loop. This is a post-image26 component correction, not application attachment or a new resource measurement. See `verified-current-order-source-native-106.json`.
