# M3 Ordinary shared HLS on a33cf4cc

Original Dispatcharr and FFmpeg authors and contributors retain full application and media credit.

Pinned Original and packaged Rust revision `a33cf4cc8af0a7753592fc7cca643a73d758c037` each passed the fresh ordinary Compose workflow. Two authenticated HLS viewers per backend shared one upstream and one FFmpeg profile process. Both fetched advertised segments 2 and 3. The complete MPEG-TS bytes matched across viewers and backends. All four captures decoded H.264 video and AAC audio. Each normal stop returned HTTP 204.

The controller exited 0 and both independently observed project and process-generation cleanup checks passed. Full M3 and performance remain open.

[Redacted receipt](../scripts/rust-integration/m3-a33cf4cc8af0-ordinary-shared-hls.json)
