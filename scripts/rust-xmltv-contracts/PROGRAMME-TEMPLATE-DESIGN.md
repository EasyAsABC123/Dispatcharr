# Proposed per-programme XML template

All application credit remains with original Dispatcharr authors and contributors. This is a source-only design, not implementation or acceptance. Image25 and its cache-batch performance workload remain unchanged. This optimization concerns cold generation only; a ready cache hit does not render programmes.

## Evidence and intended boundary

`content.rs` groups channel IDs by EPG, fetches/decodes each stored programme once, then calls `render::programme` per mapped channel. The original Python renderer formats a programme once and substitutes its primary channel attribute for duplicate mappings. The fixed500/50 fixture therefore does2250 common programme renders in the original versus22,500 native full renders. See SHARED-GUIDE-CODE-AUDIT.md for exact image23 hashes; image25 retains the same cold rendering implementation.

Only two places in the current programme renderer use the channel string: its bounded-input charge and the escaped channel attribute. Metadata/property rendering also depends on the immutable row, row ID and request origin; those are constant across duplicate mappings. No per-channel SQL, new DTO cache, regex engine, clock lookup, parallel rendering or cross-request state is needed.

## Proposed representation

Prepare one owned rendered programme string using the actual first channel ID and current renderer. Record the exact UTF-8 byte span containing the escaped channel attribute value structurally while writing that attribute. Store the complete string, span and common validated input charge as an internal ProgrammeTemplate. Do not find the span with a global string search/replacement: titles, descriptions, credits, image URLs and channel IDs can contain attribute-looking text.

The primary element can be borrowed directly from that string. For subsequent IDs, preserve the prefix and suffix around the recorded span and escape only the new ID using the existing XML escape implementation. Recompute checked final element length and the cheap raw channel-input charge for each ID. Common metadata/node/depth validation, datetime formatting and property rendering happen once per unique programme.

Prefer appending prefix/escaped-ID/suffix directly to the existing bounded batch writer after all element-length checks, rather than allocating a second complete programme String. Keep the public render::programme behavior available for current tests/callers; any extraction must preserve its exact errors and byte formatting. A single-channel programme must not pay an additional full XML copy just to construct a template.

Keep one programme template alive, never a whole EPG/export or a global template map. Once the template contains all common text, the decoded row can potentially drop before duplicate elements are emitted; preserve only needed scalar IDs. This is an explicit lifetime choice to verify, not an assumed reduction. No Arc-sharing across requests is required.

## Bytes, ownership and ordering

Calculate peak actual capacities: current decoded row/JSON scratch, template String, bounded output batch, pending delivery String/cache frame, physical Redis arguments/replies, and detached HTTP frames. The template plus batch should replace the current element String plus batch, not add an uncharged second element buffer. Validate reserve growth before allocation and use the existing build/IO/final-frame owners; do not release a template/build lease while physical SQL/cache work is still pending. Existing render/frame/export/catalogue limits remain unchanged.

Use existing EPG ordering, per-EPG channel vector order, programme `(start_time,id)` order, newline placement and all child-element/attribute formatting. Keep per-channel length/error checks at the same emission point. Do not preflight every channel or entire export early, because an oversized secondary ID can currently fail only after earlier generator output. A valid prefix must not be silently omitted because later duplicate output fails a resource check.

One decoded row/template supplies every duplicate, so a concurrent database update cannot produce different programme versions for different channel mappings. Retain the existing single SQL statement/cursor snapshot semantics; do not re-read for each channel, change transaction isolation or add a process/global cache with stale invalidation. Generation fences, cache storage, TTL, audit/Connect boundaries, body demand and cancellation behavior remain separate unchanged paths.

## Required acceptance gates

1. Run existing378 original render/error vectors and36 complete generator cases before and after extraction. Include timezone/date formatting, Unicode escaping, property scalar/type failures and local poster URLs derived from row ID/origin. Keep the public renderer regression surface.
2. Add actual original complete-byte cases for one channel/EPG, ten shared IDs, many distinct EPGs, channel/EPG overrides, tied programme starts/IDs and end-order versus start-order overlaps. IDs must include XML metacharacters, Unicode, empty/fallback values, long secondary IDs and identical IDs produced by shared tvg_id/gracenote settings. Identical IDs still require repeated elements; do not deduplicate output. Put attribute-looking text inside metadata and ensure it is not substituted.
3. Prove one common render per unique row with bounded test-only counters and unchanged output multiplicity/order. Do not infer this from wall-clock speed or add per-programme production logs.
4. Check exact-limit and over-limit escaped IDs, large/deep/tiny-node metadata, oversized programme/body frames, error precedence, partial generator output and cache abort/fencing. Preserve explicit errors instead of omitting programmes.
5. Test actual capacity ownership across yields, caller cancellation, a delayed Redis reply, SQL rollback, one retained slow frame, final clones and global shutdown. Run concurrent leaders for different cache keys and shared-key followers. Keep existing100-reader progress and byte-return tests.
6. Repeat XMLTV47/Python-written-cache interoperability/audits, actual500/50 Source/native full XML, and packaged immutable-image shutdown. Preserve exact bytes and source hash bindings; existing image25 evidence must not be relabeled for changed code.
7. Only after functional gates, schedule a new isolated paired cold-export experiment with unchanged accepted budgets, workload, pacing, schema/window and full service scopes. Include a one-channel control to detect overhead and the shared50-guide case to test the predicted reuse opportunity. Confirm hot ready replay remains unchanged; do not attribute hot-cache improvements to this cold-only design. Accept measured results only for the tested slice, not the whole application.

Implementation requires a new explicit parent go-ahead after the current image25 review/performance work. No source file in the renderer/generator was changed by this design.
