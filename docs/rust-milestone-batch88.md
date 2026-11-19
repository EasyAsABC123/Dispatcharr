# Ordinary DVR playback and legacy HLS readers

The original Dispatcharr authors and contributors retain full application,
workflow and algorithm credit.

Ordinary Rust Main exposes the recording list, completed file playback and
active HLS playback used by the existing React DVR interface. It reuses the
selected worker’s projection, the existing UI and playback permission paths,
and the bounded recording file and response ownership. The paired Compose
fixture covers valid file ranges and active HLS reads. Recording management
and scheduling remain on the selected worker path.

Live HLS readers accept older descriptors with no timestamp and derive a
missing advertised target from the playlist window and target. Publisher
takeover still requires a timestamp. Eight offline Original-to-Rust playlist
comparisons matched after the change, compared with three before it. The
packaged comparison and viewing performance remain open.

Formatting, strict all-target Clippy, three HLS HTTP tests, 530 Store tests,
35 Main tests and 14 operator tests passed. The scoped test commands left
80 external-fixture tests ignored.

The exact Docker build at `3c5063347` preserved all 497 React, license and
attribution inputs and froze runtime packages. Across the locked workspace,
1,302 tests passed and 810 external-fixture cases were ignored. Workspace
formatting and strict all-target Clippy passed. Exact-image preflight passed
for both playback fixtures and the live-viewing measurement. Their runtime
results remain pending.

[Focused results](../scripts/rust-integration/milestone-batch88/focused.json)
· [Docker build and workspace checks](../scripts/rust-integration/milestone-batch88/packaging-and-workspace.json)
· [Milestone ledger](rust-milestones.md)
