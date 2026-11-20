# Administrator VOD statistics during playback on 6e83

Pinned Original and packaged Rust each returned HTTP 200 JSON for
`GET /proxy/vod/stats/` before, during and after one authenticated
movie playback in the isolated Compose project. Before playback and
after natural retirement the statistics were idle. During playback the
response identified the one owned movie, session and provider. All
recorded typed fields agreed. The fixture checked that timestamps were
finite and positive.

Each playback made one complete origin request. The 4,299,455-byte MP4
matched the expected digest and decoded audio and video on both backends.
Both guests and the controller exited 0. Exact cleanup confirmed all
four owned role generations were absent and the project inventory was
empty. The raw controller retains inherited sequential-fixture labels.
The per-guest observations establish one viewer per backend. Full M4
and performance remain open.

The original Dispatcharr authors and contributors retain full application and behavior credit.

[Redacted receipt](../scripts/rust-integration/m4-vod-admin-stats-6e83d3d.json)
