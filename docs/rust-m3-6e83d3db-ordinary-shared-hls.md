# M3 Ordinary shared HLS on 6e83d3db

Original Dispatcharr and FFmpeg authors and contributors retain full application and media credit.

Pinned Original and packaged Rust revision `6e83d3dba83932fe77420dbfcb3c83f42a2ce88e` each passed the fresh ordinary Compose workflow. Two authenticated HLS viewers per backend shared one upstream and one FFmpeg profile process. Both fetched advertised segments 2 and 3. The complete MPEG-TS bytes matched across viewers and backends. All four captures decoded H.264 video and AAC audio. Each normal stop returned HTTP 204.

The controller exited 0 and both independently observed project and process-generation cleanup checks passed. Full M3 and performance remain open.

[Redacted receipt](../scripts/rust-integration/m3-6e83d3dba839-ordinary-shared-hls.json)
