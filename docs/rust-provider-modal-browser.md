# Image44 provider modal browser proof

Original Dispatcharr authors and contributors retain full application credit and
original licensing. The React implementation remains unchanged.

Actual unchanged React on Original and canonical image44 passed normal UI login,
Movie and Series provider switching, provider-specific two-episode replacement,
episode expansion, and six actual HTMLImageElement.decode() checks per phase
with 2×3 proxied SVGs. Ordered structured observations match exactly, without
sorting or response normalization. Zero VOD HTTP failures and uncaught exceptions.
Source default priority account differs deliberately from the UI first provider.

The dedicated runner creates a fresh owned Compose fixture using pinned Original
archive f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff
and extracts exactly the manifest-bound image frontend for Original. No Accept
override, playback, remote provider or forced refresh. Only a local four-path
SVG origin is allowed. Seven metadata-table row fingerprints and the complete
user row (microseconds preserved) restore exactly. Normal login audit writes and
sequence advancement are genuine disposable effects; exact project teardown is
required and independently verified. This is not performance/fullVOD acceptance.

Portable fresh-clone invocation after selected tools are materialized:

```sh
python3 scripts/rust-vod-api-contracts/run_provider_modal_browser.py \
  --source-only --build-manifest <successful-image44-build.json> \
  --output artifacts/rust/<unique-source-output>
python3 scripts/rust-vod-api-contracts/run_provider_modal_browser.py \
  --build-manifest <successful-image44-build.json> \
  --output artifacts/rust/<unique-paired-output>
```

Requires pinned Original git object, Docker/Compose, matching built image and
Node with WebSocket plus Chromium (RUST_BROWSER_CHROMIUM override supported).
Runner snapshots every tool/input, asserts full Original archive SHA, records
frontend hashes and image labels, and tears down owned resources in finally.
No Rust source, build, sampler or private historical fixture is required.

Retained failures: Source1 invalid Source-only Compose declaration before any
protocol observation; Source2 unrelated deliberately broken oracle logo404
after all modal work passed. Source3 qualified corrected tools; identical five
tool hashes ran successful paired image44. Raw private artifacts remain intact.

[Scoped image44 evidence](../scripts/rust-vod-api-contracts/verified-provider-modal-browser-image44-1.json) binds the original archive, exact tools and manifest.

## Current 8b0c package

The cached provider modal replay passed on package
`8b0c76ca67ade0ee65f0900a1c22792dc7510d90`. Original and Native used the same
112-file React bundle. Both browsers signed in, switched both Movie and
Series providers, verified the provider-specific two-episode lists and
decoded six 2 × 3 artwork images. The ordered observations matched exactly
on the same live fixture. Neither phase recorded VOD HTTP failures or
uncaught exceptions.

The Original browser matched the retained Source3 and image47 outcomes.
For that cross-run comparison, the six artwork version values were each
verified against the exact owned artwork URL and compared by artwork path.
The live paired comparison included the original version bytes. The local
origin served twelve GETs on its four allowed paths. Seven metadata-table
fingerprints and the complete user row restored exactly. All three fixture
ports bound to loopback, and zero project containers, networks or volumes
remained.

The first attempt exited 1 before Compose startup when Git archived the copied
controller subdirectory. The retry anchored the same Original archive at
the checkout root and exited zero. Full M5 acceptance and performance remain
open. [8b0c browser receipt](../scripts/rust-vod-api-contracts/verified-provider-modal-browser-8b0c-1.json)
