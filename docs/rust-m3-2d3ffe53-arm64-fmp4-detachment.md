# Current ARM64 profiled fMP4 viewer detachment

Original and ordinary Rust `2d3ffe53` each served two independent signed fMP4 sessions over one upstream connection, one transform and one remux. After the lead viewer disconnected, the surviving session received at least 750,000 new bytes from the same pipeline. Both arms decoded audio and video successfully for both sessions and retired all owned Compose resources. [Redacted proof](../scripts/rust-integration/m3-2d3ffe53-arm64-profiled-fmp4.json)

The initial 60-second synthetic source reached EOF too early and triggered upstream reconnection on both backends. The passing fixture used the same encoder, cadence and pacing with a 720-second clip. The two arms independently generated and strictly qualified matching full input bytes before viewing. The playback deadlines and one-upstream gate remained fixed. Both arms observed zero source EOF events during the test. This result covers functional client detachment on profiled fMP4.

Dispatcharr’s original authors and the authors of its dependencies retain full credit. [Attribution](../ATTRIBUTION.md)
