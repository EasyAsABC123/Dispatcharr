# Shared FFmpeg fMP4 playback on 8241

Original Dispatcharr, FFmpeg, and dependency contributors retain full credit.

In both Compose projects, two authenticated viewers shared one upstream
connection, one FFmpeg transcode process, and one remux process. Each backend
delivered at least six complete fragments to both viewers. After the first viewer left,
the remaining viewer received more media through the same pipeline.

All four decoder probes exited 0 and decoded H.264 video and AAC audio.
The observer verified that the exact media processes and all five owned
Compose roles retired. Both project inventories were empty.

The Rust image was built from `82414407`. This run covers shared playback
and resource counts. Release CPU and memory measurements and wider M3
workflows remain open.

[Redacted two-viewer fMP4 receipt](../scripts/rust-integration/milestone-batch113/8241-shared-fmp4.json)
