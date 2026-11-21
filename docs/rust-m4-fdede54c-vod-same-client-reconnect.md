# Same-client VOD reconnect with idle session metadata on fdede54c

Pinned Original and packaged Rust each issued one session URL to an
authenticated client. The client made two complete reads of the same URL.
Every response returned HTTP 200 and all 4,299,455 MP4 bytes with the expected
digest. All four captures decoded video and audio. Each backend made exactly
two requests to its owned origin.

The first complete read freed the provider slot while session metadata
remained idle. The next read reused the metadata creation marker and advanced
the request count from one to two. The slot released again and the session
metadata retired after the second read. Optional recovery remained disabled.

The controller and verification exited 0. All four exact owned roles retired
and every Compose inventory was empty. Full M4 acceptance and comparative
performance remain open.

The original Dispatcharr authors and dependency contributors retain full credit.

[Redacted receipt](../scripts/rust-integration/m4-fdede54c-vod-same-client-reconnect.json)
