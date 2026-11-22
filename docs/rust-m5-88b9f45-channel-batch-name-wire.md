# Canonical Channel Batch name and response parity on 88b9f45

The original Dispatcharr authors and contributors retain full credit for the application. See [Attribution](../ATTRIBUTION.md).

The Rust change admits the canonical Channel Batch name field, validates it against the existing serializer rules, and applies successful updates in the same ordered transaction as other fields. It preserves the atomic response when an administrator submits one missing channel and one valid channel. Both packaged backends returned the same 79-byte HTTP 400 response and left the scoped state unchanged.

The paired Compose run matched literal response bodies and selected headers for all four rejection cases. These covered unauthenticated, Streamer, Standard, and administrator mixed-validation requests. Both backends accepted a normal administrator rename, returned and served the new name through the normal detail route, then restored the original name with matching scoped SQL snapshots. All owned resources retired exactly.

An intermediate Rust image passed typed outcomes, SQL checks and teardown. Its independent proof preserved the differing 400 JSON field order and missing response metadata. The final image matched the retained Original body digest and both measured method and cache-variation headers. The exact package verified 2,159 application inputs and 497 protected inputs. Formatting, strict Clippy, 774 crate tests, both focused regressions, the ordinary Compose build and the fresh paired proof passed.

The [machine-readable receipt](../scripts/rust-integration/m5-88b9f45-channel-batch-name-wire.json) retains both measured wire outcomes and the exact final image identity. Wider Channel Batch operations, full M5, and comparative performance remain open.
