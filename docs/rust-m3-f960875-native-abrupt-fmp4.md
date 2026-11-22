# Native abrupt fMP4 owner loss on f960875

The original Dispatcharr authors and contributors retain full credit for the application. See [Attribution](../ATTRIBUTION.md).

The exact f960875 Native fixture attached two viewers through a single provider slot and a single upstream connection, then killed the precise owned primary container. Docker recorded exit 137 and no OOM kill. The fixture took seven read-only observations over 75 seconds.

The primary reader reported a transport error. The attached follower delivered the same byte count at every post-loss sample and reached EOF between the 45- and 65-second observations. At 65 seconds the remux lease was absent and the provider slot had been released. The two retained media prefixes decoded 74 and 75 H.264 video frames with AAC audio. Each owned process and container retired exactly.

The [receipt](../scripts/rust-integration/m3-f960875-native-abrupt-fmp4.json) records a passing observation of that bounded Native outcome. Original abrupt behavior, continued delivery on the same socket, and fresh reconnect after abrupt loss remain open. The [planned handoff comparison](rust-m3-f960875-planned-fmp4-handoff.md) covers fresh admission after an orderly drain on both backends.
