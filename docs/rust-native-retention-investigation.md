# Native input retention investigation

Original Dispatcharr application authors and contributors retain full credit.
This is an implementation investigation, not an accepted performance claim.

The frozen image1 ten-channel diagnostic failed its declared working-memory
gate (ratio1.009465). The failure and component breakdown remain in
`scripts/rust-integration/verified-many-retention-diagnostic-1.json`. PostgreSQL
RSS sums include shared pages; pool size and duplicate media storage were both
unproven explanations at that point.

The current admin-only `/internal/proxy/ts/v1/memory` reports local retained
payload separately from shared Redis logical payload and known Redis key
allocation. A24,064-byte real HTTP/Redis test validates both counters. A readonly
PostgreSQL collector separately reports active session counts, private memory and
PSS, with no SQL texts, credentials or client addresses. Component evidence is
`scripts/rust-integration/verified-retention-telemetry-1.json`.

Frozen image10 then passed a candidate-only75second functional capture with ten
channels/readers, ten75-frame decoder primes and ten75-frame video/audio capture
decodes. At75seconds, each local ring held15,096,212bytes:150,962,120bytes total
(143.97MiB). Shared Redis held the same logical payload again; known Redis input
keys allocated154,741,689bytes (147.57MiB). Every owner fence matched. This proves
duplicate retained payload in image10 without attributing a causal share of the
older image1 working-memory result. Sequential local/Redis sampling can straddle
TTL/timer expiry; the60second snapshot differed by one chunk on some channels.

The final PostgreSQL snapshot had eight client backends,50.43MiB PSS and23.93MiB
private memory; its diagnostic RSS sum was254.82MiB. This is a concurrent
functional observation, not a source/Rust database comparison. The isolated
Compose project was removed. Source retention/pool settings were unchanged and
no resource gate was evaluated. Exact source/image binding and all four samples:
`scripts/rust-integration/verified-retention-image10-1.json`.

## Bounded hot cache with shared history

The working tree now implements this design. Frozen image11 passes native
replica and HLS/fMP4 real-media gates described below. Redis remains the authoritative60second,
16MiB-per-channel history for Rust producers. A smaller bounded in-process cache
would serve recent media without retaining a second full history. Deployments
without the shared input store retain the existing full local history. The cache
must never shorten a viewer's accessible history.

The implementation combines these changes:

1. Local shared-input chunks use the Redis sequence number and original receive
   timestamp, including followers that first join at a nonzero index. Publishing
   a local counter and translating it by a guessed offset is unsafe after
   startup skips, age expiry and source changes.
2. Startup position comes from the authoritative input timestamp snapshot after
   readiness. The existing source-derived age lookup and tie behavior apply;
   behind-zero starts after the live head. Four-chunk readiness and metadata
   activation still precede the first response.
3. A cache miss retains the requested cursor and reads that exact Redis chunk.
   It must not silently advance to the oldest cache entry. A genuinely expired
   Redis chunk uses the original generator's oldest-available recovery, then its
   near-head fallback if no retained chunk remains.
4. Every historical read verifies the session's owner era against the returned
   input fence. A successor's media cannot be delivered to an old session. Source
   discontinuity flags and original packet bytes remain attached to each chunk.
5. An HTTP frame keeps the viewer/channel admission and exactly one delivery
   slot until its final byte owner drops. Redis historical bytes keep their
   transient read-byte permit through that same lifetime. Fast cached viewers
   share Arc payloads; historical reads cannot create an unbounded second cache.
6. Historical-read admission has bounded waiting and cancellation. A blocked
   transport frame cannot trigger additional reads; timer/keepalive polling
   cannot cancel and recreate Redis operations repeatedly. Physical read
   accounting remains held until the actual Redis response completes.
7. Remote follower fanout continues reading each current Redis chunk once per
   replica. Cache misses use bounded historical reads without slowing the live
   publisher. Stop, final viewer shutdown, owner crash and stale cleanup retain
   their current fenced behavior.

Required functional gates before any new isolated resource matrix include:

- Explicit nonzero global sequences, timestamp age ties and behind-zero startup.
- A viewer starting beyond the hot-cache window receiving exact retained bytes.
- A held/slow frame across cache eviction followed by exact Redis history;
  another viewer continues live delivery.
- Expired-history recovery matching the original generator, with no stale-owner
  or successor data delivered.
- Owner and follower paths, source failover/discontinuity,100viewers, bounded
  historical-read capacity, cancellation storms and prompt provider/client
  cleanup with held bodies.
- Full Compose real-media decoding and direct accounting showing smaller local
  cache while Redis retains the full history, followed by a separately approved
  paired resource study.

Changing PostgreSQL pool capacity is a separate hypothesis and must preserve
management and admission concurrency. It must not be bundled into the cache
change without independent evidence and tests.

Component validation currently passes core/runtime suites,20ordinary HTTP
regressions,11real-Redis HTTP lifecycle gates and three additional historical
read tests. The owner/follower HTTP gates force a two-chunk local cache and prove
Redis retains additional history while stop/provider cleanup still succeeds.
Historical tests prove exact global701+sequence bytes and discontinuity,
behind-zero positioning, expired recovery and successor rejection. With a Redis
read-byte budget of exactly one frame,100historical waiters remain bounded until
the held frame drops, while a live cached reader continues; cancelling a waiter
does not free the held frame's accounting. This is not full application or
performance acceptance.

Frozen image11 passes owner/follower H264 media with one upstream/provider,
metadata activation, shared stop and owner-crash cleanup in26.659seconds against
a35second deadline. Six captures decode74frames with audio. Source and Rust
HLS/fMP4 protocol captures also all decode74frames with audio. The ten-channel
75second accounting gate observes40,938,880local payload bytes (39.04MiB) with
the full150,962,120Redis history bytes (143.97MiB). It uses the same60second/
16MiB authoritative-history contract as image10. All projects were removed.

Exact image/source bindings and scope are in
`scripts/rust-integration/verified-hot-cache-image11-1.json`. This establishes
functional storage and media behavior without accepting a Docker working-memory
or CPU gain. A separate100-viewer late-history image gate also passes: after75s
of retained input, new viewers seek45seconds back across ten channels, using
32concurrent requests. Every first64KiB prefix matches an actual Redis chunk
received35–55seconds earlier, beyond the local cache. All100viewers read1.5MB
without another upstream connection; ten representative MPEG2/audio captures
decode69frames each. These late seeks can start mid-GOP, so initial reference
warnings are permitted; byte-prefix equality is independently exact. The project
was removed. Evidence:
`scripts/rust-integration/verified-hot-cache-late-history-image11-1.json`.

The older image1 memory failure remains preserved. A new isolated study of
frozen image11 now passes all three paired ten-channel trials, with alternating
order, equal decoder priming and15second/60second warmup/measurement. Median
paired Rust/source ratios are0.22128CPU,0.82405Docker working memory,
0.06490first-byte p95 and0.98898throughput. This satisfies the declared warmed
native-TS slice budgets. It does not close whole-application parity or performance:
browser/React and missing live WebSocket/event side effects are excluded.
Exact configuration, all six measurements, component breakdown, observer scope,
source/image hashes and captured tool bytes are recorded in
`scripts/rust-integration/verified-hot-cache-many-channel-image11-1.json`.
