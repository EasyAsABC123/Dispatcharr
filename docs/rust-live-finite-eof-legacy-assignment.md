# Same-profile live fallback keeps the original stream assignment

Original Dispatcharr authors and all dependency contributors retain full credit.

The exact Rust application `c6eeee7dd584648da9d8cc3e3ab63bd9a7089d2d` ran the pinned Original and Native Compose playback pair with two authenticated readers. Both readers received useful audio and video from the primary and fallback input, for four independently decoded windows per backend. The primary completed three finite requests. The distinct fallback opened once and the origin census peaked at one simultaneous upstream.

After switching, both implementations retained the initial stream assignment and its provider profile pointer, while live metadata identified the active fallback. Both retired the original assignment, all three stream pointers and the provider slot, and passed independent owned teardown. The isolated Redis component also verified preservation of the same profile and credential slot and receipts, the active-only write, five excluded scopes, and single release.

Formatting, strict Clippy and the exact workspace test run passed. The workspace reported 1518 passed, zero failed and 814 ignored across 193 reported test targets. The paired terminal proof SHA-256 is `e7daf248a972bd2dfc6015cf7f127ca0e93b0699a0f7036fbe87f9aeec33f636`.

The earlier exact 526260 playback pair had useful media and exact cleanup in both backends. Original kept its initial alias. Native changed it to the active fallback alias. The pinned source audit identified ordinary recording-start Connect enrichment as an external consumer of the legacy alias when the publisher supplies no stream ID. The fresh playback pair validates the alias used by that consumer. Delivery of an actual recording-start event during fallback remains unverified.

This evidence covers the normal warm same-profile raw switch. Cross-profile transfer, cold and expired-owner adoption, wider milestone acceptance and comparative performance remain separate.
