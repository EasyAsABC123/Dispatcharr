# Latest Rust shared streams, guide scheduling, and ARM64 idle diagnosis

## M3 shared streams and worker recovery

On AMD64, the exact ordinary Rust package at `2d3ffe53` passed separate signed MPEG-TS Compose workloads for shared demand, a follower issuing stop, an owner crash, and complete worker loss. Eight signed reads decoded successfully. The full-outage case killed the two owned workers, held the fixture through the required 65-second idle interval, and recovered with a new replica of the same image. The restarted replica cleaned up the last retained provider slot in 6.19 seconds. Both projects passed independent checks of the exact retired resources. [AMD64 proof](../scripts/rust-integration/m3-2d3ffe53-shared-ts-recovery.json)

On ARM64, both Original and Rust authenticated two independent HLS sessions to the same profile. Both sessions received equal complete media segments within each arm, decoded audio and video, and shared one marked FFmpeg process and one upstream connection. Each backend processed two normal session stops and retired its exact Compose resources. The first harness attempt stopped at a Python import. The second reached both Native playlists but the observer expected the AMD64 FFmpeg launcher arguments. The third verified the installed ARM64 launcher and passed all phases. [ARM64 proof](../scripts/rust-integration/m3-2d3ffe53-arm64-signed-hls.json)

These cases leave the separate late-reader retention and wider output-format gates open.

## M6 guide request before scheduled scanner

In both pinned Original and ordinary Rust Compose, a signed request without a guide source ID returned 202 and published exactly one guide job. A real worker claimed and acknowledged it, recorded `SUCCESS`, released its `SET NX` lock, and removed the orphan schedule and interval. Once the guide terminal had been observed, the test armed the existing `scan-files` schedule in the same database and worker generation. Its real scanner parent and its one linked Groups child reached `SUCCESS`. The queue drained and both projects retired their exact resources. The first Rust harness attempt stopped before the workflow because it injected experimental flags. The passing retry used the default flags and scheduler entry point. The wider job graph and fault cases remain open. [Paired proof](../scripts/rust-integration/m6-2d3ffe53-guide-before-scanner.json)

## M9 Original idle-only observation

The stock ARM64 Original backend ran through normal startup and 210 seconds of idle observation. Across 43 samples, median working memory rose by 59,207,680 bytes between the first and last five idle samples. The earlier playback-and-guide run grew by 155,099,136 bytes. All 42 idle intervals had stable process identities and none yielded complete per-process PSS or USS deltas, so the observed allocation remains unattributed. The idle run exercised no playback or guide requests, and its independent review verified exact teardown. The paired release memory gate remains open. [Redacted idle proof](../scripts/rust-integration/m9-original-arm64-idle-attribution.json)

Dispatcharr’s original authors and the authors of its dependencies retain full credit. [Attribution](../ATTRIBUTION.md)
