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
