# Planned fMP4 handoff on f960875

The original Dispatcharr authors and contributors retain full credit for the application. See [Attribution](../ATTRIBUTION.md).

On November 22, 2026, Original and ordinary Native both completed the same planned fresh-viewer handoff scenario on Beepboop. The test disconnected the lead viewer, observed both encoder generations and output leases drain, stopped the primary cleanly, then opened a new viewer on a distinct already-running peer. Both arms used real generated media and checked the peer's distinct process generation and local endpoint.

Each of the four clips had six fragments and decoded to 74 video frames with H.264 video and AAC audio. Each arm made two successive upstream connections with peak concurrency of one. Both scoped Compose projects verified their inventories, exact container absence and host process retirement.

The first attempt stopped Original while the lead viewer's encoder was still active and timed out waiting for its generation to leave. The second observed a complete Original drained handoff. Native's fixture awaited selected-worker startup messages even though those roles were disabled. The final attempt verified ordinary Native readiness with the same network and repository fences and retained Original's startup checks.

The [machine-readable proof](../scripts/rust-integration/m3-f960875-planned-fmp4-handoff.json) records the exact candidate digest and the literal execution exit of 0. This covers planned drain and fresh admission. Abrupt owner loss, continuity for an open viewer socket, comparative performance and the complete M3 milestone remain open.
