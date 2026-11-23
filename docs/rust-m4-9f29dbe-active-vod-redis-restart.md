# Active VOD after Redis replacement on 9f29dbe

An isolated AMD64 Compose fixture compared Original `1241b3f7` and the ordinary Rust package `9f29dbe`. A signed VOD viewer opened a controlled prerecorded MP4 using the same request shape on each backend. The fixture stopped the owned Redis service without saving its state, restarted it, and reopened the same playback ID while the first response was active.

| Observation | Original | Rust |
| --- | --- | --- |
| Initial response | 200 | 200 |
| Origin-matching bytes from the first response | 278,528 and still streaming | 65,536, then ended |
| Same-ID reopen | 200, first 8,192 bytes match the origin | 503 |
| First response after reopen | Another 8,192 bytes at the expected offset | Already ended |

The capture and exact fixture cleanup both exited 0. The parity reducer exited 1. This reproduces the previously reported active-session recovery gap on the current ordinary package. The selected viewer, origin and Redis were disposable fixture resources. The test compared bounded bytes, so full-file decoding and playback performance remain separate.

The first attempt stopped during image-overlay admission before Compose startup. Its launch, reducer and cleanup callbacks each exited 1. Independent inspection verified no resources for the fixture project prefix. A separate retry used the image ID from the package manifest and reached both applications. The [aggregate receipt](../scripts/rust-integration/m4-9f29dbe-active-vod-redis-restart.json) preserves both attempts.

Original Dispatcharr and dependency contributors retain full credit.
