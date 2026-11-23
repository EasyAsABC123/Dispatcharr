# ARM64 allocator trial stopped at reference certification

The isolated 96 MiB observer diagnostic exited 1 before it launched the allocator process. All 13 reference commands exited 0. The certified output assigned stream index 0 to audio and index 1 to video, while the strict fixture requires video index 0 and audio index 1. The independent audit exited 0 and verified exact teardown and 16 owned absence checks.

Guest support for the requested allocator remains unmeasured. The observer never entered Django setup, so no callback or memory counters were captured. The 96 MiB limit and 86.4 MiB headroom requirement remain unchanged. This attempt supplies no performance result.

[Scoped diagnostic receipt](../scripts/rust-integration/m9-arm64-observer-allocator-reference-failure.json)
