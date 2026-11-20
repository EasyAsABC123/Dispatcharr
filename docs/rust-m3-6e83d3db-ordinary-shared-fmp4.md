# M3 Ordinary shared fMP4 on 6e83d3db

Original Dispatcharr, FFmpeg and dependency contributors retain full credit.

Pinned Original and packaged Rust revision `6e83d3dba83932fe77420dbfcb3c83f42a2ce88e` each passed the fresh ordinary Compose workflow. Two authenticated fMP4 viewers per backend shared one upstream, one FFmpeg transcode and one remux. Each backend delivered at least six complete shared fragments. The remaining viewer received new media through the same pipeline after the first viewer left. All four decodes confirmed H.264 video and AAC audio.

The controller exited 0 and both independently observed project and process-generation cleanup checks passed. Full M3 and performance remain open.

[Redacted receipt](../scripts/rust-integration/m3-6e83d3dba839-ordinary-shared-fmp4.json)
