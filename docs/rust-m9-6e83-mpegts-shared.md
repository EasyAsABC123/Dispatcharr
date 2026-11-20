# Simultaneous MPEG-TS viewers on 6e83

Two authenticated viewers used separate Rust workers to watch one channel.
The live shared-demand case and the operator-stop case each observed one
upstream connection and one provider slot. In the first case the second
viewer kept receiving media after the first closed. In the second case
the operator stop ended both readers. All four captured responses decoded
audio and video.

The workload and independent empty-project inventory both exited 0.
The receipt covers functional behavior on the pinned Rust package.
Formal coordinator coverage and CPU and memory measurements remain open.

The original Dispatcharr authors and contributors retain full application and behavior credit.

[Redacted receipt](../scripts/rust-integration/m9-mpegts-shared-6e83d3d.json)
