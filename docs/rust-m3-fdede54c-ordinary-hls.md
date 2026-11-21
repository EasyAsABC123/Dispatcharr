# Two ordinary HLS viewers share one upstream

The pinned Original and the production Rust `fdede54c` image each served
two authenticated HLS viewers through one upstream connection and one
FFmpeg profile process. The viewers received matching advertised segment
bytes by sequence. Both viewers in both backends decoded H.264 video and
AAC audio and stopped normally with HTTP 204. The independent checker
verified exact source and profile retirement and owned cleanup.

This paired workflow is one part of M3. Full media-profile acceptance and
comparative performance remain open.
[Receipt](../scripts/rust-integration/m3-fdede54c-ordinary-hls.json)

The original Dispatcharr authors and contributors retain full credit.
