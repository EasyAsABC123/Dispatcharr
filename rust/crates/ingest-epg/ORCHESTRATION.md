# Remaining guide refresh orchestration

All application and algorithm credit belongs to the original Dispatcharr authors. This is a Source audit for the next integration slice, not an implementation or acceptance claim. The accepted file/SQL and retained cache components do not yet execute these job effects.

The reference is `apps/epg/tasks.py::parse_programs_for_source`, `apps/epg/utils.py::send_epg_update`, `core/utils.py::log_system_event`, and `apps/output/streaming_chunk_cache.py::invalidate_epg_chunk_cache` at the pinned original baseline.

1. Publish `parsing_programs` progress0 before mapping lookup. Mapping IDs are sampled once; TVG lookup excludes NULL/empty identifiers.
2. With no mapped IDs, count all source EPG entries, save `status=success` and `last_message="No channels mapped to this EPG source (N entries available)"`. Preserve `updated_at`. Publish progress100/statussuccess, without message/timestamp. Return without parsing, cache invalidation, success audit or late-mapping dispatch.
3. Otherwise resolve extracted/file/cache path, fetch when a missing path has a source URL, and reject an incomplete footer before staging. Publish progress10/message`Parsing programs...` only after those checks.
4. Stage valid mapped rows; skip unmapped records before projection. Invalid mapped projections do not increment either accepted or unmapped counts. Every5000 accepted rows, after the applicable staging flush, publish processed/channel counts with progress`min(85,10+int(n/(n+10000)*75))` and message`Staging programs... N` using comma-separated integers.
5. Publish progress90/message`Updating database...`, then perform the atomic staged swap. Only a complete, uncancelled catalogue may reach this point.
6. After successful commit, increment raw Redis key`epg_content_generation`. Invalidation failures are logged and swallowed. Retired XMLTV chunks expire under their existing TTL; in-flight readers are not deleted.
7. Save source status, the literal success message below and a fresh `updated_at`; then write the success system event, publish progress100/statussuccess/message/ISO timestamp, and dispatch per-channel work for mapping IDs added since the initial snapshot.

The literal success message is:

```text
Parsed {accepted:,} programs for {channels_with_programmes} channels (skipped {unmapped_skips:,} programs for {all_source_epg_entries - mapped_nonempty_tvg_ids} unmapped channels)
```

`send_epg_update` sends to group`updates` with full outer payload`{"type":"update","data":{...}}`. Data contains `progress`, `type="epg_refresh"`, `source` and `action`, plus the explicit kwargs. It does not add a success boolean. The success audit type is `epg_refresh`, with details `source_name`, `programs`, `channels`, `skipped_programs`, `unmapped_channels`; channel ID/name remainNULL. Preserve original event retention, integration dispatch and error swallowing rather than inventing a new audit path.

Error status updates deliberately use a fresh DB connection after staging/transaction errors. `_set_epg_source_status` updates only status/last_message and catches its own failures. It publishes progress100/statuserror/error and logs `epg_error` with source name/message. `ws_error` takes precedence over last_message for that payload/audit: missing file without URL stores `No URL provided, cannot fetch EPG data` but publishes/audits `No URL provided`. Incomplete-file text, XML/SQL exception formatting, refresh-level terminal status, retries and cancellation need actual Source cases before native wiring.

The present importer report lacks the all-source EPG entry count and mapped nonempty TVG count needed for the literal status text. These should be returned by an owned bounded source-context/mapping snapshot rather than reconstructed from accepted rows. A future reporter must publish after actual staging replies, retain the job context through physical DB/Redis/publication work, and avoid a strong reporter→importer ownership cycle.

The next testable gates are actual Source publication/status/audit/invalidation ordering, empty/error branches, a5000-row progress boundary, mapping changes during parsing and error/connection recovery. Full download/extraction, first/late mapping dispatch, task retry/completion, cross-Redis/PostgreSQL fencing and app/resource acceptance remain open.

The nine-case actual original runtime oracle at `artifacts/rust/m6-guide-reporter-source-53` now captures full publication envelopes at the original synchronous wire sender, source status/programme count/generation at every effect, committed versus in-transaction state, and actual audit rows. The durable summary is `scripts/rust-epg-parity/verified-reporter-source-53.json`. It is Source characterization only; WebSocket delivery and external integration/plugin dispatch are excluded.

The 5,001-row case flushes 2,500 rows twice before progress35 with `processed=5000`, then stages the final row before progress90. Real Redis generation `INCR` failure (non-integer content) is swallowed and success still follows. Missing-file, incomplete-footer and swap-error paths publish the error completion before the error audit; success audits precede success completion. No-map success preserves the old `updated_at` and has only progress0/100, with no generation increment, audit, or late-mapping dispatch. Null/empty mapped identifiers still reach the staged-swap/orphan cleanup even with zero accepted programmes.
