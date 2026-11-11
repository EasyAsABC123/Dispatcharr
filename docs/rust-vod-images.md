# VOD image and cache compatibility

Original Dispatcharr authors and contributors retain all application credit.
The AGPL license and original notices remain unchanged.

Image44 attaches the shared image router to the actual Main application. The
packaged API matches240 actual Original responses:96 successes,48 not found,
32 negotiation failures,32 forbidden responses,20 authentication failures and
12 method rejections. Comparisons include selected headers, every binary body
byte and origin-request counts. Six predeclared image/ordinary/cached-info
paths also pass simultaneous10-reader Source and native mixed traffic.

The routes cover captured movie, series and episode image selection and the
VOD-logo cache action. Local files and remote caching use the existing shared
image runtime. Four image admissions and64 waiters remain bounded; remote
downloads retain the5MiB limit. Local files stream without inventing a total
size restriction absent from Original. Metadata uses the existing shared
two-worker192MiB budget.

Four actual physical test groups retain caller/work ownership through cache
lookup, PostgreSQL replies, blocking file work, canceled callers, held output
frames and shutdown. Both Original and the actual Main API qualify the owned
FIFO audit: a physical opener survives HTTP cancellation, then closes after an
owned writer releases it. All18 database tables and owned local files are
restored. Existing channel-logo CRUD/upload/cache/throttle/proxy behavior also
passes101 packaged cases on the same image.

The retained image43 failure is an assembly failure: its standalone image
component passed, but Main omitted the image-router merge and returned404 for
all240 requests. Image44 adds that merge and repeats the complete packaged
regressions; no image43 acceptance is transferred.

The [fresh-cache provider-modal workflow](rust-provider-modal-browser.md) now
passes against actual unchanged React: four provider selections and six decoded
images per phase, with provider-specific episode replacement. Stale/forced
provider refresh remains open. Uncaptured stored-property, query, suffix, cookie and
fault schedules remain open. These functional results claim no resource gain.
See [the source-bound checkpoint](../scripts/rust-integration/verified-checkpoint-image44-1.json).

To repeat the packaged image check using a successful immutable build:

```sh
python3 scripts/rust-vod-api-contracts/run_packaged_vod_images.py \
  --mode http --packaged-vod-images \
  --build-manifest artifacts/rust/BUILD/build.json \
  --candidate-image YOUR_MATCHING_BUILD_TAG \
  --output artifacts/rust/vod-images-NEW
```

The runner creates isolated owned Docker Compose fixtures and removes only its
project after recording protocol, restoration and shutdown evidence.
