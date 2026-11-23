# Signed HLS and fMP4 sharing on d41abbb9 AMD64

Fresh Linux AMD64 Docker Compose runs compared exact Rust application
`d41abbb90dec665182f2060804e40a4d522b1743` with Original
`1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`. The ordinary Native package
manifest SHA-256 was
`62e15cdfa5c1504c2d8734e4f625dbd952f59b69c9b62c3eea2bc2f35f1220d0`.
[Machine-readable evidence](../scripts/rust-integration/m2-m3-d41abbb9-amd64-signed-live-formats.json)

- **HLS.** Both signed viewers on each backend shared one upstream connection
  and one FFmpeg output-profile process. Regular MPEG-TS segments 2 and 3
  matched byte-for-byte across both viewers and backends. Each capture
  decoded 100 H.264 video frames and 192 AAC audio frames. All four signed
  session stops returned HTTP 204.
- **fMP4.** Both signed viewers on each backend shared one upstream connection,
  one FFmpeg transform and one remux process. The two viewers on each backend
  received the same initialization bytes and six complete overlapping
  fragments. All four captures decoded H.264 and AAC. The remaining viewer
  continued receiving new media through the same process generations after
  the first viewer detached.

Both preflights, physical runs and proofs exited 0. An independent read-only
audit rehashed the HLS captures and parsed the retained fMP4 boxes to verify
the media receipts. It also matched the exact container and process retirement
records and empty inventories for all four projects. Each format retired two
Original and three Native container generations.

This result covers ordinary same-pipeline playback with synthetic media and
normal detach. M3 still requires broader format and profile coverage,
cross-replica failover and qualifying performance evidence. The original
Dispatcharr, Django, FFmpeg and dependency authors retain full credit.
