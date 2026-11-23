# AMD64 VOD resource comparison on d41abbb9

The ordinary AMD64 Rust package for
`d41abbb90dec665182f2060804e40a4d522b1743` passed a fresh functional VOD
comparison and four alternating measured pairs against Original
`1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`. The functional and measurement
owners, controllers, and coordinator commands all exited 0. The
independent audit rehashed all ten 1 MiB captures and checked each
separate strict eight-second decode. All captures had the same full
SHA-256. Each decode returned 200 video frames, mapped audio and video,
and emitted empty error output.
[Machine-readable evidence](../scripts/rust-integration/m4-d41abbb9-amd64-vod-only-resource.json)

| Metric | Original | Rust | Rust change |
| --- | ---: | ---: | ---: |
| CPU core-seconds per phase, median | 1.250 | 1.190 | −5.45% |
| Peak sampled working set, phase median | 767.2 MiB | 506.6 MiB | −33.96% |
| Capture throughput, median | 279.624 kB/s | 279.616 kB/s | −0.003% |
| Time to first body byte, nearest-rank p95 | 266.23 ms | 259.19 ms | −2.64% |

CPU, working-set and throughput changes are medians of the four paired
Rust-to-Original changes. The p95 is the largest of four observations
per backend. Each resource window covered the active application,
infrastructure including its decoder, and the fixture origin. CPU is
the sum of their cgroup v2 usage deltas. Working set subtracts inactive
file memory and uses the largest simultaneous sample. Throughput uses
the first-to-last-byte interval of the fixed capture. Resource identities
and limits remained stable, no cgroup OOM was observed, and all examined
origin, provider and session state returned to zero. Compose inventories
were empty after both campaigns.

The separate archive diagnostic exited 1. After Original's decoder
completed, 51 samples over 5.064 seconds still showed one active origin
handler with a TCP socket in `ESTABLISHED` state. The final sample
reported the handler age as 21.937 seconds. The diagnostic cleaned up
its exact Compose scope. The cause remains undetermined, and archive
measurement did not begin.

These results cover the tested VOD fixture only. Timeshift and the
deferred full-scan performance run remain open, along with formal M4
acceptance. The original Dispatcharr, FFmpeg and dependency authors
retain full credit.
