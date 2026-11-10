# Shared-guide cold XMLTV code audit

All original application credit belongs to Dispatcharr authors and contributors. This is a static audit during the resource quiet window, not a new benchmark or a causal explanation of its active measurements. No runtime or measured configuration was changed.

The measured candidate is canonical image23 (`candidate-build-m5-current-m7-xmltv-m8-backup-image23-1/source.tar`). Its three relevant files were read directly from that immutable archive and match the current worktree byte for byte:

| File | SHA-256 |
|---|---|
| `xmltv-http/src/content.rs` | `4f57a62eb0cfd99bfa964b77f095dddf119f6f1a5fd2f2bb48f08d91acd011da` |
| `xmltv-http/src/rows.rs` | `ace602fc640a5ba918e4bd781892455155bd9c7247995da9558f5c6e4724174a` |
| `xmltv-http/src/render.rs` | `9926d38a3246104c3a74cce7c7880ddc422cb7d299b826be51dd2b53446c18cb` |

Original `apps/output/epg.py` also matches pinned original commit `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`; its SHA-256 is `e2df77cf0e84acd51ad9e8e6ea31793a7337e90c008ca7f5d01d43cf503254b3`. These comparisons use file reads and git object reads only, not a rerun of either backend.

## Programme fetch multiplicity

The suspected per-channel SQL fetch or repeated DTO decoding is **not present** in image23.

Native `content.rs:50` creates `BTreeMap<epg_id, Vec<channel_id>>`; each visible channel contributes its output ID to that map. At line76 it calls `rows::programmes` once with the unique map keys. `rows.rs:230` issues one SQL stream selecting programmes for `epg_id=ANY($1)`, applies the common window, and orders by `(epg_id,start_time,id)`. It creates one `XmltvProgramme` and decodes its JSON metadata once per returned row. Repeated polling of that SQL stream is not a new SELECT per channel or per row.

Original `epg.py:244–319` likewise queries unique EPG IDs. It fetches keyset pages ordered by `(epg_id,end_time,id)`, each with at most20,000 rows, and sorts each EPG's pending programmes by `(start_time,id)` before emitting them. The original page loop also executes an empty final query to detect exhaustion. For a stable2250-row fixture, its code therefore traverses one nonempty page plus the final empty page; the native implementation has one programme SELECT. These are code-path counts, not an observed pg_stat_statements measurement.

The fixed guide fixture (`scripts/rust-guide-perf/fixture.py:24–35`) has500 visible channels,50 real EPG IDs,10 channel mappings per EPG,45 stored programmes per EPG,2250 unique programmes, and22,500 output programme elements. It has no dummy sources or remote fetches.

## Repeated rendering: a concrete difference

Native `content.rs:78–80` calls `render::programme` **inside** the loop over every channel ID mapped to that programme. It renders22,500 complete programme strings for the2250 unique stored rows. Metadata JSON is already decoded; it is not parsed again there. What repeats is the bounded metadata traversal (`render.rs:bounded`), timestamp formatting, escaping of titles/descriptions/metadata, property formatting, and output allocation. `render.rs:641–684` formats both datetimes via chrono format for each duplicate, then renders the same body with a different channel attribute.

Original `epg.py:319–537` formats/escapes a programme once for its EPG's primary channel. Its `flush_pending` at lines280–300 copies that string and replaces the first exact channel attribute for the remaining mapped IDs. It therefore performs2250 complete programme renders plus20,250 channel substitutions for the fixed fixture. Its timestamps use direct numeric formatting, and a nearby original comment explicitly identifies datetime formatting as costly in its larger exports.

This proves repeated render work, not that it accounts for any particular measured CPU/completion ratio. Native safety traversals, cache transport, XML copying/output, SQL sort plans, singleflight/follower behavior, and observer/container scopes can also contribute. The active image23 matrix must finish unchanged; no ratio should be attributed to this difference without a later isolated experiment.

## Bounded optimization option

Prefer one **per-programme render template**, released before advancing to the next programme, over a whole-EPG, whole-export, cross-request, or channel-count-sized cache. Render common timestamps/body/properties once and retain a proven channel-attribute split; emit one bounded output string for each channel in its existing vector order. Only the escaped channel ID and total-length check vary. This matches the original reuse idea while avoiding string replacement that might alter text in titles/descriptions/metadata.

The template must account for its actual String capacities in the existing physical build memory reservation. Keep only one row, one template, the bounded batch/current output, and existing delivery/cache frames; never collect duplicate programme strings or all programmes for an EPG. The current build reservation already includes programme-metadata and frame headroom, but any revised peak allocation must be calculated and tested explicitly rather than assuming spare capacity. Response frames and physical Redis commands retain the existing guards. No resource lease can be released on logical timeout while a physical SQL/cache reply still owns its buffers.

A split template can keep field boundaries structurally rather than searching for `channel="..."` in a complete string. Escaping remains per channel. Common input-metadata validation can be done once, but cheap per-channel raw/escaped-length and final-frame checks must preserve the current limit/error boundaries, especially a short primary ID followed by an oversized duplicate ID. Cache keys, generation fences, TTLs, audit events and publication order must remain unchanged. A global template cache is unnecessary: existing cache-key singleflight already limits leaders; a new shared cache would add invalidation and ownership risks without proving benefit.

## Required gates before implementation acceptance

1. Run existing378 renderer cases,36 complete generator bytes, cache/delivery/physical guard tests, HTTP47 and packaged output regressions against a new immutable image. Retain image23 results unchanged.
2. Add shared-EPG fanout cases with distinct primary/secondary IDs containing XML metacharacters, UTF-8, empty/numeric IDs, and attribute-looking text in title/description/metadata. Compare complete bytes and ordering. Preserve programme-specific local poster URLs using row ID and request origin.
3. Prove one common render per unique row and unchanged output multiplicity using bounded instrumentation in a test fixture, not production per-row logging. Test one channel/EPG, ten channels/shared EPG, many unique EPGs, tied start times with ID ordering, overlapping rows whose end/start order differs, and overrides that change mapping/visibility.
4. Prove per-channel resource checks for a long secondary ID, boundary-sized escaping, deeply nested/tiny-node metadata, huge strings, frame overflow, and multiple concurrent cache leaders. Charge actual capacities and detached frame owners; abort midway and hold final clones through shutdown.
5. Keep one SQL stream and original ordering. Template reuse must not introduce a second fetch or later per-channel re-read. A concurrent programme update after row decode must not make different channel duplicates show different versions; all duplicates of that row use the same decoded template. Later rows may observe the existing query's semantics. Do not silently change isolation to a whole-export snapshot or read-committed per-channel queries.
6. If SQL batching/index changes are considered separately, compare EXPLAIN/actual query counts outside measurement, stable complete bytes, inserts/updates/deletes at keyset boundaries, and transaction/cancellation cleanup. Native `(epg,start,id)` versus original index-friendly `(epg,end,id)` plus per-EPG reorder is a separate investigation; this audit does not prove one plan faster.
7. After functional gates, measure a new source-bound candidate in an isolated paired window with the same500/50 fixture, cold/hot states, one/ten viewers, equal workload/pacing and whole-fixture resource counters. Keep render/transport/SQL changes separate so any improvement can be assessed. Do not retune the active matrix or attach old gains to the new code.
