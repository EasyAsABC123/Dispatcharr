# Shared upstream with distinct output profiles

Pinned Original and the exact Rust package at `c6eeee7dd584648da9d8cc3e3ab63bd9a7089d2d` each served three simultaneous authenticated viewers across two distinct FFmpeg output profiles. Each arm observed one upstream connection and one provider slot, with one FFmpeg process generation per profile. All three readers decoded audio and video. [Exact receipt](../scripts/rust-integration/m3-c6eeee7d-distinct-output-profiles.json)

After viewers on one profile disconnected, the remaining viewer kept receiving fresh media through the same surviving process generation. The other profile process retired. Normal stop retired all owned media resources, and both arms verified exact cleanup.

The first Original controller exited 1 during decoder qualification. Its three fixed video windows contained 74, 74, and 46 frames. The second Original arm exited 0. Its Native peer exited 1 because the observer expected FFmpeg as the first argv token and missed the two processes started through the pinned package loader. Both earlier attempts remain in the receipt. The fresh pair required the exact packaged command prefix and independently proved process ownership, decode, and retirement.

This receipt covers the two tested output profiles. M3, the formal workflow roster, and comparative performance remain open. Full credit belongs to the original Dispatcharr authors and dependency contributors. [Original attribution](../ATTRIBUTION.md)
