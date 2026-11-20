# amd64 PostgreSQL and VLC package portability

Original Dispatcharr, PostgreSQL, VideoLAN, and dependency authors retain full
credit for their application and tools.

The exact amd64 application and standalone backup-component images built
successfully. Fifteen runtime probes then passed against those retained
images with networking disabled. Both packages exposed the same PostgreSQL
17 payload sources and hashes and ran pg_dump, pg_restore, and psql version
checks. The application also passed the packaged VLC and cvlc checks.
The controller verified the selected image identities and retired all
fifteen probe containers.

The first runtime attempt used a platform manifest digest where the local
engine expected the retained image tag. A second attempt reached the VLC
wrapper and selected the wrong version-banner expectation. The final replay
kept both built images and used the observed pinned VLC banner.

This qualifies the packaged amd64 PostgreSQL and VLC tool boundary. ARM64
and the full application release matrix remain open.

[Exact portability receipt](../scripts/rust-integration/milestone-batch111/amd64-pg-vlc-portability.json)
