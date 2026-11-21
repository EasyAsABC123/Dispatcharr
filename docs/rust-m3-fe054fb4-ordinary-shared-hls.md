# Ordinary shared HLS on fe054fb4

Exact packaged Rust revision `fe054fb473fcb22101a56dfe1458638f998a3b2a`.

Pinned Original and packaged Rust each served two authenticated HLS
viewers through one upstream connection and one FFmpeg profile process.
Both viewers fetched advertised segments 2 and 3. Their complete
MPEG-TS bytes matched across viewers and backends. All four captures
decoded H.264 video and AAC audio. Normal stops returned HTTP 204.

The controller exited 0 and independently verified project cleanup
and retirement of the observed process generations.

Full M3 and comparative performance remain open.

Original Dispatcharr and FFmpeg authors and contributors retain full application and media credit.

[Redacted receipt](../scripts/rust-integration/m3-fe054fb473fc-ordinary-shared-hls.json)
