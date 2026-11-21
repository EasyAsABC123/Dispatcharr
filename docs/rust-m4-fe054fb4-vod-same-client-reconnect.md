# Same-client VOD reconnect while session metadata is retained

Pinned Original and packaged Rust `fe054fb473fcb22101a56dfe1458638f998a3b2a` each issued one session
URL to the same authenticated client. The client used that URL twice.
Each GET returned HTTP 200, all 4,299,455 MP4 bytes, and the expected
SHA-256 digest. All four captures decoded video and audio. Each backend
made two requests to the owned origin.

After the first response ended, the provider slot was free while idle
session metadata remained. The second GET reused the same metadata
creation marker and advanced its request count from one to two. After
the second response ended, the slot released and the session metadata
retired. Optional recovery remained disabled.

The physical pair and independent reducer both exited 0. All four owned
role generations retired and all three Compose inventories were empty.
Full M4 acceptance and comparative performance remain open.

[Redacted receipt](../scripts/rust-integration/m4-vod-same-client-reconnect-fe054fb4.json)

All application and algorithm credit belongs to the original Dispatcharr authors and contributors.
