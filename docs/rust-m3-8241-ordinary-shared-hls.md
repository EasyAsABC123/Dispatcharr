# Ordinary shared HLS on 82414407

Pinned Original and the exact Rust package each served two authenticated
HLS viewers through one upstream and one FFmpeg output-profile process.
Each viewer fetched advertised segments 2 and 3. Complete MPEG-TS
segment bytes matched for both viewers and both backends. Each capture
decoded real H.264 video and AAC audio with a successful FFmpeg exit.

Both normal viewer stops returned HTTP 204 on each backend. The exact
profile process and upstream retired, Rust Main exited 0, and independent
teardown checked owned containers, networks, volumes and process
generations. [Redacted receipt](../scripts/rust-integration/milestone-batch113/8241-ordinary-shared-hls.json)

The original Dispatcharr and FFmpeg authors and contributors retain full
credit. This verifies the scoped ordinary playback workload. Full M3
and equivalent-work performance remain open.
