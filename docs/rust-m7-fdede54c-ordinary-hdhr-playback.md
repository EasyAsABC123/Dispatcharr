# HDHomeRun lineup and playback on fdede54c

The pinned Original and production Rust `fdede54c` images each returned
a generated HDHomeRun lineup and served MPEG-TS at the advertised path.
Both lineup reads returned HTTP 200 JSON. The paired playback samples
were byte-identical at 1,316,000 bytes and contained 7,000 TS packets.
Both decoded AAC audio and 74 H.264 video frames.

The origin recorded one new connection per backend. Ordinary client
close returned the active count to zero, and the fallback origin was
unchanged. Rust stopped with exit 0. Compose removed the four owned
container and process generations, with empty project inventories.

The earlier attempt exited 1 during local image verification, before
any playback observation. The retry and independent proof both exited 0.
This supplement covers one generated network tuner path. Formal required
nodes, other protocol behavior, and comparative performance remain outside
its scope. [Machine-readable receipt](../scripts/rust-integration/m7-fdede54c-ordinary-hdhr-playback.json)

The original Dispatcharr authors and contributors retain full credit.
