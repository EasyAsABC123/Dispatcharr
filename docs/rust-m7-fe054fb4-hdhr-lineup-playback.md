# HDHomeRun lineup playback on fe054fb4

Exact packaged Rust revision `fe054fb473fcb22101a56dfe1458638f998a3b2a`.

Pinned Original and packaged Rust each returned the same owned channel
entry in the ordinary HDHomeRun lineup. The client followed the exact
advertised stream path within the disposable Compose network. Each
backend returned 7,000 complete MPEG-TS packets over HTTP 200. Both
excerpts decoded H.264 video and AAC audio.

Each arm used one local upstream connection. The origin retired after
the client closed, with the fallback untouched. Native stopped normally
with exit 0 and the fixture independently verified retirement of all
four owned roles and empty project inventories.

Full M7 and comparative performance remain open.

The original Dispatcharr authors and contributors retain full credit.

[Redacted receipt](../scripts/rust-integration/m7-fe054fb473fc-hdhr-lineup-playback.json)
