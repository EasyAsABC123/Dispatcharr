# M3 Ordinary shared fMP4 on a33cf4cc

Original Dispatcharr, FFmpeg and dependency contributors retain full credit.

Pinned Original and packaged Rust revision `a33cf4cc8af0a7753592fc7cca643a73d758c037` each passed the fresh ordinary Compose workflow. Two authenticated fMP4 viewers per backend shared one upstream, one FFmpeg transcode and one remux. Each backend delivered at least six complete shared fragments. The remaining viewer received new media through the same pipeline after the first viewer left. All four decodes confirmed H.264 video and AAC audio.

The controller exited 0 and both independently observed project and process-generation cleanup checks passed. Full M3 and performance remain open.

[Redacted receipt](../scripts/rust-integration/m3-a33cf4cc8af0-ordinary-shared-fmp4.json)
