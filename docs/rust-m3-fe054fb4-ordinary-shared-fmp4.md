# Ordinary shared fMP4 on fe054fb4

Exact packaged Rust revision `fe054fb473fcb22101a56dfe1458638f998a3b2a`.

Pinned Original and packaged Rust each served two authenticated fMP4
viewers through one upstream connection, one FFmpeg transcode and one
remux. Both backends delivered at least six complete shared fragments.
The second viewer received new media through the same pipeline after
the first closed. All four captures decoded H.264 video and AAC audio.

The controller exited 0 and independently verified project cleanup
and retirement of the observed process generations.

Full M3 and comparative performance remain open.

Original Dispatcharr, FFmpeg and dependency contributors retain full credit.

[Redacted receipt](../scripts/rust-integration/m3-fe054fb473fc-ordinary-shared-fmp4.json)
