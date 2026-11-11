# Cached provider information compatibility

Original Dispatcharr authors retain all application credit and AGPL licensing.
The existing React frontend remains unchanged.

The Rust routes `/api/vod/movies/{id}/provider-info/` and
`/api/vod/series/{id}/provider-info/` now serve the captured cached-only domain.
The image42 checkpoint compares90 actual Original/native responses, including
status, selected headers and exact response bytes. Each request audits18 tables
with full timestamp precision. Both standalone native TCP and the packaged Main
API pass the same corpus and a simultaneous10-request barrier.

The shared VOD SQL budget remains bounded. Actual PostgreSQL lock/caller-abort
controls retain68MiB of request ownership until the reply and rollback complete.
The queue, expired/canceled waiters, final output-frame clones, shutdown and
output-capacity503 controls also pass. No partially serialized success response
is substituted for an output-capacity failure.

Stale or absent provider information that requires refresh, and forced refresh,
remain explicitly unsupported. [Image/cache routes](rust-vod-images.md) and the captured
[fresh-cache React modal workflow](rust-provider-modal-browser.md) now pass.
Uncaptured query grammar and stale/forced provider refresh remain open.
This checkpoint makes no new performance claim.

To reproduce the packaged check after building an image with the documented
immutable build runner:

```sh
python3 scripts/rust-vod-api-contracts/run_packaged_cached_info.py \
  --mode http --packaged-cached-info \
  --build-manifest artifacts/rust/BUILD/build.json \
  --candidate-image YOUR_MATCHING_BUILD_TAG \
  --output artifacts/rust/cached-info-NEW
```

The runner creates owned local Docker Compose fixtures, compares against the
pinned Original application and verifies restoration before removing its project.
Use a new output directory for each run; retained failures remain evidence.
