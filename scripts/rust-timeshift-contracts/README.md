# Catch-up contract evidence (M4, in progress)

All original application authorship belongs to upstream Dispatcharr authors
and contributors; see LICENSE and ATTRIBUTION.md.

`capture.py` freezes the pure timestamp/timezone/duration/provider-URL helpers
from the pinned immutable Python source. `probe.py` records 24 native session
API/error wire cases. `media_probe.py` characterizes actual native archive
playback, range bytes, URL-format fallback, error classification, provider
capacity and post-disconnect lifecycle. Opaque capabilities and provider
credential URLs are never persisted.

Run the media capture using
`python scripts/rust-vod-contracts/run.py --output artifacts/rust/<new-directory> --timeshift-media`.
The shared isolated fixture generates a timestamp-stable H264/AAC MPEG-TS
archive and serves it at 500,000 bytes/second with Range/HEAD support. It also
provides deterministic PHP200, query-only, 403, 404 and 500 failure modes and
credential-free request observations. `python -m unittest discover -s
scripts/rust-timeshift-contracts -p 'test_*.py'` verifies fixture range/fault
behavior. Docker/loopback access is required.

The durable `python-media-goldens.json` records three successful 1,880,000-byte
206 responses, identical payload hashes and strict audio/video decode (74
frames), plus exact provider-error statuses and lifecycle differences. Format
selection caching persists between cases, as in the source. Successful native
playback eventually removes its API session after grace; initial provider
errors retain the authorization record while releasing provider capacity.
Capturing these behaviors does not assert that Rust media routes exist, full
application parity is met, or performance has improved.

`capture.py --wire-ranges --output <new-file>` extracts only the original pure
range/header functions from its immutable AST inside a read-only, network-none
pinned container. The 246 durable vectors in `python-range-goldens.json` match
Rust's pure length/range helpers. A separate regression rejects arithmetic
overflow explicitly. Python's arbitrary precision integer and unusual Unicode
numeric spellings remain outside this bounded helper coverage.

For a frozen application image, use
`python scripts/rust-vod-contracts/run.py --candidate-timeshift-media --candidate-overlay docker/compose.rust-candidate.yml --candidate-build-manifest <build.json> --output artifacts/rust/<new-directory>`.
This compares all24 captured API/native-error wire cases and six archive media
scenarios against original source with the same actual database and origin,
stopping source web and clearing only isolated fixture Redis before candidate
startup. It verifies image/source binding, exact media hashes/headers/provider
fallback order, stable lifecycle state and bounded TTLs, then strictly decodes
all three candidate media captures. Wall time and observer remaining-TTL jitter
are reported separately, never used to normalize payload/quota errors. Candidate
comparison remains pending until run against the next corrected image.

The frozen candidate comparison has now passed; see `verified-compose.json`:
24 API/native-error cases, six media/provider/lifecycle scenarios, three strict
A/V decodes (74 frames), image/source binding and cleanup, all with zero captured
differences. Native VOD seven-case and literal profile/React alias checks also
passed in the same run. Shared positive caps, adoption/seeks/cross-worker state,
complete telemetry and performance remain open.

`--timeshift-admission-rust-test` on the shared runner now passes six original
policy/accounting helper cases with actual Redis, including literal
hop-before-refusal, another-channel refusal, VOD preemption, all generation stop
keys and same-session exemption. Explicit policy inputs and owned synthetic
accounting records keep this scope distinct from auth/media or WebSocket parity;
see `verified-admission.json`.

`--timeshift-native-limit` captures actual JWT/ORM positive-limit refusal and
same-capability retry; when a candidate manifest/overlay is supplied it compares
both apps and strictly decodes each retry. Source-only capture passed in
`python-native-limit-goldens.json`, and the host Rust actual-schema positive gate
passed separately in `verified-native-media.json`. The frozen positive-cap app
comparison remains a subsequent recorded result.
