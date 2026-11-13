# Image58 immutable build verification

[Image58 build evidence](../scripts/rust-integration/image58-build/README.md) verifies an actual Docker build from committed `46b91d70`, with 854 Rust inputs, 495 unchanged React inputs and original credits. Root checks every source byte against Git and all 114 installed frontend/credit hashes against image57; the isolated probe runs as UID65532 and cleans up.

The image ID is `sha256:c1dc3fb061c105f08a93a8569b252329b8567caf42191c4134282227e6cca1a8`. This build adds no Main, package-group, SDK, soak or resource acceptance, and excludes the later generic500 correction. [Image57](rust-image57-validation.md) remains the selected functional and [measured](rust-image57-live-performance.md) checkpoint. Its gains are not attributed to image58. Original Dispatcharr authors retain all application and algorithm credit.
