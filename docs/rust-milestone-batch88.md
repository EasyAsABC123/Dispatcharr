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
comparisons matched after the change, compared with three before it.
The exact packaged HLS pair matched all four HTTP results on both backends,
with eight descriptor and absolute expiration restorations. The warm and
recovery samples decoded identically and matched byte-for-byte across
backends. The fixture sustained one upstream connection and one profiled
client in each backend, verified normal stop and capability retirement, and
passed exact resource cleanup. Full M3 acceptance remains open.

Formatting, strict all-target Clippy, three HLS HTTP tests, 530 Store tests,
35 Main tests and 14 operator tests passed. The scoped test commands left
80 external-fixture tests ignored.

The exact Docker build at `3c5063347` preserved all 497 React, license and
attribution inputs and froze runtime packages. Across the locked workspace,
1,302 tests passed and 810 external-fixture cases were ignored. Workspace
formatting and strict all-target Clippy passed. Exact-image preflight passed
for both playback fixtures and the live-viewing measurement.

## Ten-viewer live performance

Candidate88 (`3c5063347ca6be710894e8d753110bc344dd4df2`, image
`sha256:3ee795ee96c2cb4c50b9851cd30e4057a7d6cddccb81fe137d0a115b4a58c034`)
was compared with Original `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` in
three paired Docker Compose trials. Ten authenticated viewers watched the
same live VLC-input MPEG-TS channel with idle workers, one upstream
connection and one provider slot on each backend. Each phase used a
15-second warmup, a fixed 60-second measurement window with 61 samples,
and a five-second drain for decoder evidence. Only bytes inside the
measurement window contributed to throughput. The trial order was
Original/Native, Native/Original, Original/Native.

| Metric | Original median | Native median | Median paired change |
| --- | ---: | ---: | ---: |
| Primary CPU over 60 seconds | 2.261056 core-seconds | 1.161156 core-seconds | 48.29% lower |
| Primary maximum simultaneous working-set estimate | 955,867,136 bytes | 285,573,120 bytes | 70.12% lower |
| Whole-fixture CPU over 60 seconds | 3.382792 core-seconds | 1.778186 core-seconds | 47.43% lower |
| Whole-fixture maximum simultaneous working-set estimate | 1,162,727,424 bytes | 499,437,568 bytes | 57.05% lower |
| Wire bytes delivered across all viewers | 92,071,520 bytes | 91,602,180 bytes | 0.51% lower |
| Encoded useful bytes delivered across all viewers | 84,797,510 bytes | 84,373,377 bytes | 0.50% lower |
| p95 time to first byte | 7,096.91 ms | 6,765.52 ms | 4.66% lower |

Primary resource accounting includes the application, subprocesses,
infrastructure and origin. Whole-fixture totals also include the observers.
The last column uses the median of the three Native-to-Original ratios.
All three pairs passed the declared limits for primary CPU (at most 90%),
working set (at most 95%), p95 first byte (at most 110%), and both wire and
encoded delivery (at least 95%). Across all 60 viewer captures, the spread in
decoded durations was 0.440 seconds for video and 0.427 seconds for audio,
within the 2.005-second limit. All six measured phases exited 0 and verified
removal of 21 exact containers and every owned project resource.

The first 100-viewer pilot exited 1. Both physical phases passed, but their
combined duration spread was 2.120 seconds for video and 2.133 seconds for
audio, above the same 2.005-second limit. The unchanged second pilot exited
0 with spreads of 0.480 and 0.469 seconds. It qualified the 10-viewer run.

The 10-viewer controller exited 1 after writing its complete result. Its
final return check raised `AttributeError` because the pair reducer had
overwritten the parsed options with a metrics dictionary. Independent
review verified all 271 frozen inputs and 18 phase evidence files and
recomputed all budgets from the captured samples. Full M3 performance and
milestone acceptance remain open, including the complete viewing campaign
and workloads with active recording, failover or other output formats.

The exact ordinary DVR Compose pair passed 18 recording checks and 19
foundation checks. The list, completed file reads, valid seeks, active HLS
media and heartbeat matched the fixture, with unchanged recording rows
and sequences and exact four-container teardown.
[Ordinary DVR receipt](../scripts/rust-integration/milestone-batch88/dvr-compose.json)

[Focused results](../scripts/rust-integration/milestone-batch88/focused.json)
· [Docker build and workspace checks](../scripts/rust-integration/milestone-batch88/packaging-and-workspace.json)
· [Packaged HLS comparison](../scripts/rust-integration/milestone-batch88/legacy-hls-compose.json)
· [Viewing measurements and evidence digests](rust-candidate88-live-performance.json)
· [Milestone ledger](rust-milestones.md)
