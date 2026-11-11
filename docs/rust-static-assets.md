# Captured static assets and browser HTML

All application, framework and bundled asset credit remains with the original Dispatcharr, Django/DRF, nginx and dependency authors. Exact installed Celery, django-celery-beat author/license notices and Kombu notices are preserved in ATTRIBUTION.md. The pinned DRF license and original asset banners travel with the runtime image.

Image41 exports27 exact DRF3.18.3 files from the pinned Original image, preserving bytes and modification times. The collector verifies the committed file manifest before packaging; the inventory stays under internal runtime documentation. Static file bodies retain ServeDir streaming. A small metadata table preserves Source MIME types, entity tags, conditional responses, ranges and captured error pages without applying Django headers to direct static responses. Static routes take precedence over three-part playback aliases.

The Source-only nginx oracle passes68 observations: all27 GET/HEAD bodies, selected cache/range checks and missing-file/method errors. It uses the actual pinned nginx binary, MIME table and Source static-location rule with identity encoding in a network-disabled container. This isolates the static location; it does not reproduce the entire production proxy or gzip configuration.

The actual Chromium header is captured independently. Original and image41 match32 additional HTML observations for that exact header, alongside the prior56 HTML observations. Each response validates its own cookie, expiry and masked CSRF token; only one declared random token slot is replaced for byte comparison. The native Chromium workflow logs in through the unchanged React UI, exercises Both/All/category selection, then navigates normally to the API HTML page. Its scripts execute, Bootstrap rules apply and Glyphicons load through real HTTP. No Accept override or asset mocks are used.

```sh
source artifacts/rust/tooling/environment.sh
python scripts/rust-static-contracts/run_source.py --output /absolute/new-empty-source-output
python scripts/rust-vod-api-contracts/run.py --mode http \
  --packaged-unified --packaged-html --packaged-static --unified-browser \
  --build-manifest /absolute/build.json \
  --candidate-image dispatcharr-rust-candidate:combined-provider-scheduler-image41-1 \
  --output /absolute/new-empty-native-output
```

[Combined evidence](../scripts/rust-integration/verified-combined-provider-scheduler-recovery-1.json) and the [packaged checkpoint](../scripts/rust-integration/verified-checkpoint-image41-1.json) bind the actual image and source. Earlier failures remain: the export fixture initially denied nginx traversal through its0700 output directory; the first image sent three missing static paths to playback/auth routing; the first normal-browser replay encountered the then-unsupported Chrome Accept header. These were not converted into passes. Broader Accept/query/role/linkification/static grammar, multipart ranges, gzip, complete Browsable API workflows and full M8/performance acceptance remain open.
