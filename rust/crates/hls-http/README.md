# Native HLS capability transport

This derivative is a port of Dispatcharr's original HLS manager, views, session
scripts and segmenter. Full original authorship belongs to the upstream
Dispatcharr authors and contributors; see [ATTRIBUTION.md](../../../ATTRIBUTION.md).
The original AGPL-3.0-only license is retained. No original application credit
is claimed by this rewrite.

`HlsGateway` accepts already authorized `PlaybackSource` values and registers a
real Redis HLS client before minting the original 24-byte, 32-character URL-safe
capability token. Its router serves `/proxy/hls/{token}/index.m3u8`, numbered TS
segments, and owner-authorized session DELETE. Capability GETs use the original
Redis touch scripts, without authentication or ORM work on steady requests.
DELETE delegates to the original exact-owner authorization boundary; admin role
alone grants no override. Malformed authentication headers are rejected.

One native TS input and segmenter serve token sessions on a channel. The app
configures a shared `OutputStore`; a second gateway follows that output without
opening a second origin. Capability reads on either gateway validate the same
Redis token and fetch shared media without registering another client.
The segmenter receives source discontinuities, uses the original 3.9-second
cut preference / 6.45-second hard ceiling, startup cuts and four-second initial
publication gate. Playlists freeze advertised target six and startup offset.
Retention follows wall-clock segment TTL, prefix pruning, four-second expiry
slack and discontinuity sequence accounting. The shared backend stores fenced ownership, binary media, timestamps/index and
the original playlist descriptor. Publication precedes local wakeup.
The original atomic abandoned-cold-session claim therefore observes publication.
Cold playlists send the initial marker immediately, periodic comments, touch the
session every three seconds, and enforce initialization/ready budgets. Dropping
an unpolled cold body also decrements held requests; the final disconnected
request allows a reconnect grace before an atomic abandoned claim.

Admission is bounded independently for managers, sessions and concurrent token
requests. Shared payload owners retain global byte permits and manager/session
admission through the final cloned HTTP `Bytes`, including after a stop or cache
eviction. Segmenter open data also has a per-manager limit; its memory is separate
from published-payload byte accounting. Defaults allow 32 managers, 12,800 token
sessions, 512 MiB of retained published payload, 32 MiB per open segment, 256
stored segments per manager and eight concurrent requests per token. Retention
is 100 seconds and idle session expiry 12 seconds. Binary segments are not duplicated in a local cache when shared storage is
enabled. Normal read capacity contention waits up to 15 seconds while keeping
request admission and refreshing activity; held read byte permits remain
bounded. Hard writer capacity exhaustion ends the manager rather than silently
omitting promised media. These resource limits
are deliberate safety differences, not evidence of arbitrary-source parity.

## Verification

Run the real Redis / loopback HTTP gates in a disposable Docker container:

```sh
python scripts/rust-hls-parity/http.py --output artifacts/rust/NEW-HLS-RUN \
  --fixture artifacts/rust/m3-hls-decode-1/h264/input.ts
```

The optional fixture mode sends actual H264 TS through the HTTP engine and HLS
routes, then independently decodes each captured HTTP segment with pinned FFmpeg
and `-xerror`. Without `--fixture`, synthetic transport packets exercise lifecycle
contracts only. The runner records source hashes and refuses changed sources.
Five real Redis gates cover shared upstream, token scopes, exact-owner stop,
missing segments, cold comments, malformed auth, request capacity, unpolled body
cleanup, reconnect grace, idle expiry, fenced descriptor cleanup, and retained
frame clones preventing admission reuse after stop. Strict Clippy passes.
Evidence: `artifacts/rust/m3-hls-http-decode-1/evidence.json`.

## Shared storage and remaining parity gates

`with_output_store` enables the shared backend. An owner restart reads the saved
index and playlist window before publishing new sequences, retains unexpired
old segments and marks new media discontinuous. Local idle checks inspect Redis
activity so valid polling through another replica keeps the original client
alive. Shared demand checks use the original ten-second/two-empty-check fallback;
last local-client teardown also checks authoritative shared demand. Shutdown
signals every manager first and has one global five-second budget, with at most
32 concurrent client releases.

The separate real Redis shutdown gate pauses Redis for six seconds with forty
sessions; shutdown remains within its five-second deadline. Shared transport
tests add two-gateway token routing, one upstream, owner restart rehydration,
100 simultaneous token viewers, and a slow response whose final cloned frame
must release its byte budget before another read proceeds. Captured fanout
media is decoded independently with pinned FFmpeg.

Live takeover of a failed owner by an already-running follower, cross-worker
native TS input distribution, profile-specific transformed output creation,
dynamic source configuration defaults and full Compose differential fault/TTL
coverage remain open. Existing profile-scoped HLS capabilities can read their
legacy Redis output keys; that does not implement the transform pipeline.
This component does not establish M3 completion, the full backend rewrite or an
application performance gain. Full Compose native HLS and fMP4 protocol/decoder gates passed for both the
Python baseline and shared-media Rust image at
`artifacts/rust/m3-shared-hls-fmp4-compose-1/run.json`. Each path decoded 74
frames with video and audio, using the exact pinned FFmpeg runtime. The image
and source tree are bound to `candidate-build-m3-shared-hls-fmp4-2/build.json`.
Native TS shared input publication/follower changes are newer source and require
a separate image and verification.

The app exposes positive configuration variables
`RUST_OUTPUT_SEGMENT_BYTES` (32 MiB), `RUST_OUTPUT_CHANNEL_BYTES` (256 MiB),
`RUST_OUTPUT_READ_BYTES` (256 MiB), `RUST_OUTPUT_MAX_SEGMENTS` (256),
`RUST_OUTPUT_OPERATIONS` (16), `RUST_HLS_CACHE_BYTES` (512 MiB),
`RUST_HLS_REQUESTS_PER_SESSION` (8), and `RUST_HLS_SEGMENT_TTL_SECONDS` (100).
Manager/session capacities inherit `RUST_MAX_CHANNELS` / `RUST_MAX_CLIENTS`.
