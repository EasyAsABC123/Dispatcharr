# Bounded MPEG-TS history reads on 6e83

Across ten active channels, 100 bounded late reads matched historical media
prefixes without opening extra upstream connections. Four samples verified the
configured 60-second history policy and the 4 MiB local and 16 MiB shared
payload ceilings per channel. Twenty live and historical captures decoded
audio and video.

The workload and independent empty-project inventory both exited 0.
The receipt covers functional behavior on the pinned Rust package.
Formal coordinator coverage and CPU and memory measurements remain open.

The original Dispatcharr authors and contributors retain full application and behavior credit.

[Redacted receipt](../scripts/rust-integration/m9-mpegts-retention-6e83d3d.json)
