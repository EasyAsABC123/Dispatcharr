# XMLTV ready-cache path audit

All application credit remains with original Dispatcharr authors and contributors. This static audit addresses the reported image23 hot-one-viewer completion-budget failure. It does not change the active/failed workload, attribute a measured percentage to a code path, or extend acceptance. No tests, database calls, Docker commands or new measurements were run for this audit.

## A ready hit does not execute the cold renderer

Original `apps/output/streaming_chunk_cache.py:242–247` reads generation/ready and chooses `_stream_ready`. It does not invoke the source generator. Native `xmltv-http/src/gateway.rs:306–377` creates build admission, channel preflight and the source generator only for EntryState::Leader. Ready hits have no build admission, no channel/programme SQL and no XML rendering. The cold per-programme-template opportunity in SHARED-GUIDE-CODE-AUDIT.md therefore cannot explain a genuinely ready-cache response.

The hot request still pays normal network-policy/settings/cache-key setup on both backends. Native dispatch reserves physical request admission and `3*64KiB+1MiB` byte-budget units by default, not the large cold build/numbering reservation. Semaphore units are accounting, not allocation of that many physical bytes. Audit/event publication is build-only; a ready hit should not introduce programme audit writes. These branches must be confirmed in later diagnostics, not inferred from endpoint name alone.

## Original ready replay

`streaming_chunk_cache.py:_stream_ready` refreshes three ready TTLs initially, then performs one ordinary Redis LINDEX per complete cached list member, decodes UTF-8, yields it to StreamingHttpResponse, and advances the member offset. It refreshes TTLs again only after75seconds (300//4), and terminates on a missing list member. Original cache construction keeps generator chunk sizes: programme yield batches are1000 output elements; channel chunks contain200 channels; preamble/footer are separate chunks.

For the fixed500-channel/50-guide/22,500-output-programme fixture, the valid normal generator code implies23 programme members,3 channel members,2 preamble members and1 footer member:29 data members. This is a code-derived count, not a saved LLEN observation of the failed phase. A ready replay then entails29 data LINDEX plus one EOF LINDEX, generation/ready GETs and the initial three EXPIREs (35 cache-related network round trips before unrelated settings/policy accesses). No Redis pipeline is used by these helper calls.

## Native ready replay

`xmltv-http/src/content.rs` batches around64KiB and `delivery.rs` further splits any large generated frame at CacheConfig.chunk_bytes, default64KiB. Native-built members therefore have a different internal shape from original members despite identical joined XML bytes. `Cache::read` also splits original legacy large members into64KiB slices, so a native reader of an original-created entry still requires multiple reads per large list member.

`delivery.rs:next` invokes `Cache::read` for every data slice and EOF. Every READ:

- Acquires the per-cursor single-frame permit and128KiB default physical-I/O budget before issuing Redis work. The data frame retains those leases until the final owner drops.
- Formats PRELUDE+READ, builds argument vectors, and calls the bounded shared RedisTransport.
- RedisTransport constructs redis::Script each call. Cached redis0.32.7 `src/script.rs:Script::new` hashes the entire Lua source with SHA1 and owns a source String; transport also copies arguments, spawns an owned physical invocation task, and retains guards through the actual Redis reply even if its caller times out.
- Runs READ Lua, then returns a bounded Vec plus cursor. HTTP uses a bounded demand channel/oneshot handshake per frame and Bytes::from_owner to retain physical frame ownership.

For the functional-smoke XML body13,015,448bytes,64KiB maximum data slices imply at least199 data READs plus EOF, versus the code-derived29 original data members. That bound is arithmetic on a recorded smoke body, not the observed failed-phase command count. OPEN adds one Lua invocation before streaming. Exact list/slice counts from the failed matrix still need saved cache diagnostics.

Native READ is more than a LINDEX. Its PRELUDE calls Redis TIME and defines validation/fencing/catalog helpers. READ invokes sweep(), which validates catalog types/counts, reads global byte accounting and expired-entry candidates, and **SETs the byte-accounting key on every read even when no entry expired**. It then validates five entry-key types, reads status, optionally renews TTL/catalog state, calls LINDEX and string.sub, and returns cursor/status information. It performs the ledger/sweep/type logic on every64KiB slice. These are code-proven operations; their CPU, latency, persistence, or contention contribution has not been measured. Do not assume the fixture enables AOF or infer disk latency from the SET alone.

For a normal ready single viewer, the50ms Wait/Capacity retry is not intrinsically part of each read. Available data returns immediately. Sixteen physical operations and64MiB read-I/O budget are configured, so a single ordinary read is below those limits. Any observed wait/retry requires diagnostics; it must not be presumed from the existence of the sleep branch.

## Diagnostics before changing anything

Preserve the failed pair and exact plan/image/source binding. In a later functional diagnostic on its owned fixture, record:

1. Source/native cache member counts, total bytes, bounded member-size histogram/max, ready state and generation before/after the request. Confirm both requests are genuine READY hits, not followers or rebuilds.
2. Absolute completion and TTFB durations, delivered byte/hash equality, and one-versus-ten concurrency. The reported ratio alone does not identify which phase grew or whether a small fixed cost dominates.
3. Per-request OPEN/READ/slice/EOF counts, requested/returned bytes, READY/WAIT/CAPACITY outcomes, renew counts, and physical operation/I/O occupancy. Use bounded test diagnostics, not per-chunk production logging.
4. Separate elapsed time in request setup, queue/admission, Redis physical replies, actor demand/response handoff, body final-frame hold, and client transfer. Sampling must not enter a new acceptance measurement unnoticed.
5. Redis commandstats/script timings and selected service CPU, plus original request/process counters, with declared observer overhead. A cached Lua invocation is not a programme SQL query; count those separately. Inspect statement/SQL activity only to confirm the ready path did not unexpectedly build.

## Bounded options, after diagnostics

The direct candidate is a bounded multi-member/slice read with one physical byte reservation and one final-owner lease covering its returned batch, subsequently delivered in bounded HTTP frames. A small independently configured batch can amortize Redis/actor/Lua overhead without making an entire13MiB export resident per viewer. A second option is caching an immutable prepared Redis Script to avoid rehashing/source allocation, preserving existing supervision. A separate option moves catalog expiry sweeping to OPEN/write/renew or a bounded maintenance actor, leaving cheap relevant-entry validation in ready reads; it must not remove global storage accounting/fencing or allow expired entries to evade byte limits.

None is implemented here. Do not enlarge chunk size or reduce validation to make the failed run pass without new source-bound functional proof. Required gates include cancellation with Redis reply delayed, leased bytes through final clones, one slow reader versus fast readers, bounded global memory under100viewers, corrupt/wrong-type/oversized legacy members, generation changes during reads, stale owners, catalogue limits and expiry cleanup, same wire bytes and TTL behavior, ready/follower/error distinctions, and global shutdown. A batched read must preserve one immutable response sequence and member/slice cursor semantics; it cannot silently skip missing/error chunks or follow a successor generation midway.

Run functional diagnostics and proof first, then a new isolated paired performance experiment with unchanged accepted budgets and explicit new image/config identity. The failed image23 hot pair remains failed; cold rendering improvements are a separate hypothesis.
