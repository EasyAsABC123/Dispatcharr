# Shared MPEG-TS across two Rust workers on fe054fb4

Exact packaged Rust revision `fe054fb473fcb22101a56dfe1458638f998a3b2a`.

Two authenticated viewers streamed the same channel through separate
Rust workers. In each case the application used one upstream connection
and one provider slot. The second viewer kept receiving media after
the first closed. A separate authorized operator stop ended both
viewers. All four captures decoded audio and video.

The workload and independent project cleanup both exited 0.

Full milestone coverage and comparative CPU and memory measurements remain open.

The original Dispatcharr authors and contributors retain full application and behavior credit.

[Redacted receipt](../scripts/rust-integration/m1-m2-mpegts-sharing-fe054fb4.json)
