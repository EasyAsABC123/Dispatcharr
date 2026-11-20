# Sequential full-body VOD playback on 6e83

Pinned Original and packaged Rust Main each served two authenticated full reads
of the same synthetic MP4 on one provider slot. Every read returned HTTP 200,
matched the intended complete media digest and decoded both audio and video.
Each read made one complete origin request.

The fixture observed the first session's metadata disappear and its slot and
reader settle before it sent the second request. The Compose controller exited
0. Cleanup verified that all four owned role generations had retired and the
project had no remaining containers, networks or volumes. Full M4 and
performance remain open.

The original Dispatcharr contributors retain full application and behavior credit.

[Redacted receipt](../scripts/rust-integration/m4-vod-sequential-eof-6e83d3d.json)
