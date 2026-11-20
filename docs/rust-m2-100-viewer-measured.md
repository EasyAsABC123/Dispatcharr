# Anchored 100-viewer playback comparison

Original Dispatcharr authors and contributors retain full application credit.

The frozen comparison ran six new phases on the pinned Original and
Candidate105 images. Each phase used the exact 100-viewer, one-channel,
VLC MPEG-TS workload and the same source-packet anchor. The independent
receipt verified 6 complete phase teardowns. The controller exited 0.

Decoded span spread was 1.600 s for video and 1.621 s
for audio. The frozen equivalent-work tolerance was 2.005 s.

All six phases met the matched-work gate. The table reports the
Rust-to-Original ratio for each independently reproduced metric.

| Trial | CPU | Working memory | First-byte p95 | Wire throughput | Useful throughput | Budget |
|---:|---:|---:|---:|---:|---:|---|
| 1 | 0.1736 | 0.2901 | 0.1202 | 1.0149 | 1.0146 | Passed |
| 2 | 0.1726 | 0.2889 | 0.1215 | 1.0155 | 1.0152 | Passed |
| 3 | 0.1736 | 0.2921 | 0.1401 | 1.0134 | 1.0131 | Passed |

Working memory is the peak simultaneous working set of the measured group.
The first-byte metric uses the frozen 50 ms floor. Resource accounting
includes the backend, infrastructure, and origin.

Across the three pairs, Rust used approximately 83% less CPU and
71% less peak working memory. Useful throughput rose 1.3–1.5%.

The result is scoped to these compared images and this workload.
The remaining M2 and integrated release gates have separate acceptance.

[Verified comparison receipt](../scripts/rust-integration/milestone-batch112/anchor-100-viewers-measured.json)

## Shared package repeat

The retained Original and the shared Rust image at
`7adb0c72dae360cc5c3a4b5e4d8f7b100e8f200b` completed three new
pairs in an exclusive zero-swap window. Every phase served 100 viewers of
one VLC MPEG-TS channel through one upstream. Each 60-second sample began
at 42,000 origin packets. The independent reducer confirmed equivalent
audio and video work, all six process exits, and exact cleanup.

Rust used **82.6% less CPU** and
**71.0% less peak working memory**
by median paired reduction. Group medians moved from
0.142 to 0.025 CPU cores and
952.0 to 275.9 MiB peak working memory.
Useful throughput rose 0.86–1.13%.

The table reports Rust-to-Original ratios. First-byte p95 uses the
same 50 ms floor as the earlier experiment.

| Trial | CPU | Working memory | First-byte p95 | Wire throughput | Useful throughput |
|---:|---:|---:|---:|---:|---:|
| 1 | 0.1744 | 0.2897 | 0.1177 | 1.0116 | 1.0113 |
| 2 | 0.1725 | 0.2881 | 0.0869 | 1.0116 | 1.0113 |
| 3 | 0.1802 | 0.2907 | 0.0876 | 1.0089 | 1.0086 |

The measured group included the backend, infrastructure, and origin. The
fixture used its loaded-idle compatibility roles and tracked observer costs
separately. Other workloads and full M2 and release acceptance remain open.
[Independent comparison receipt](../scripts/rust-integration/milestone-batch113/anchor-100-viewers-combined.json)
