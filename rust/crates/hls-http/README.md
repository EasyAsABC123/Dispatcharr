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

One native TS input and segmenter serve all local token sessions on a channel.
The segmenter receives source discontinuities, uses the original 3.9-second
cut preference / 6.45-second hard ceiling, startup cuts and four-second initial
publication gate. Playlists freeze advertised target six and startup offset.
Retention follows wall-clock segment TTL, prefix pruning, four-second expiry
slack and discontinuity sequence accounting. Redis stores only fenced ownership
and the original playlist descriptor, with publication preceding local wakeup.
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
is 100 seconds and idle session expiry 12 seconds. Capacity exhaustion ends the
manager rather than silently omitting promised media. These resource limits
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

## Remaining parity gates

Media storage and live session lookup are process-local. Cross-replica capability
routing and segment reads, persisted segment/index rehydration after a restart,
profile-specific transformed outputs, dynamic source configuration defaults, and
full Compose differential fault/retention coverage remain open. Fenced Redis
metadata does not supply distributed media. This component does not establish
M3 completion, the full backend rewrite, or an application performance gain.
