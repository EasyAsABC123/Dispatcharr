# Candidate67 repeated live viewing comparison

The original Dispatcharr authors retain the credit for the application and its
algorithms. The FFmpeg and VLC authors retain the credit for their tools.

The 24-phase comparison exited successfully after 69 minutes 55 seconds.
All 12 paired trials passed the preset CPU, memory, throughput and latency
budgets. The test used one VLC input profile and one live MPEG-TS channel,
with the normal worker graph loaded and background work idle. The Python and
Rust phases were alternated over three pairs at each viewer count. Each
phase used a fresh fixture, a 15-second warm-up, 61 counter samples over
60 seconds, and a fixed five-second drain.

| Viewers | Python CPU, core-s | Rust CPU, core-s | Python peak estimated working set, MiB | Rust peak estimated working set, MiB | Median paired CPU reduction | Median paired working-set reduction |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1.60 | 1.18 | 908 | 273 | 26.7% | 69.9% |
| 2 | 1.70 | 1.18 | 908 | 271 | 30.8% | 70.1% |
| 10 | 2.30 | 1.21 | 911 | 270 | 47.5% | 70.3% |
| 100 | 8.61 | 1.49 | 950 | 276 | 82.7% | 71.1% |

The table uses the predefined comparison boundary of backend, infrastructure
and origin. Absolute values are medians across three runs per backend.
Percentages are medians of the three paired Rust/Python ratios. The working
set estimate subtracts inactive file cache from charged cgroup memory. Each
peak is the largest aggregate sample in its measurement window.

Including the observer, the 100-viewer whole-fixture medians were
22.15 → 5.72 CPU core-seconds and 1,219 → 570 MiB estimated working set.
Raw charged memory, including file cache, was 2,209 → 1,538 MiB at the same
boundary. The evidence retains both accounting boundaries and all 12 pair ratios.
[Machine-readable results](../scripts/rust-integration/milestone-batch67/viewing-full24.json)

All 678 viewer captures passed complete audio and video qualification.
Useful video and audio duration spreads were 0.480 and 0.469 seconds across
all captures, both within the 1.984-second bound. In every pair, the Rust
phase delivered at least 99.4% of the Python phase's window and encoded useful
bytes. The predefined first-byte p95 ratio passed for every pair, with a
50 ms floor applied to each backend before division. Each phase observed
one origin connection and one provider slot at its assigned viewer count.

All phase controllers exited 0 with both output streams at EOF and their
process groups settled. The service manager recorded the campaign exit 0.
Independent inventory found no remaining containers, networks or volumes
for its 24 Compose projects. No input changed during the run. The service log
records a post-exit stop request for a scope that had already unloaded.

This comparison measures steady live viewing on Candidate67, source
a05b6745d651603097417686c18d0fcdad208f83, image
sha256:75af355bcb7f90304ac22a1db2a7ebe6b4da9549bbb5fc8d932257424e00ef8b.
The campaign records scoped budget success. Broader milestone acceptance and
workloads such as active recording, failover, and other output formats remain
open.
