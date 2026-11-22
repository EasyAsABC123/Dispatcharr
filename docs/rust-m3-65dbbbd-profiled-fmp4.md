# Shared fMP4 playback on 65dbbbd

Original Dispatcharr authors and contributors retain full application credit. See [upstream attribution](../ATTRIBUTION.md).

The Original and Rust Compose arms each served two overlapping fMP4 readers through one upstream connection, one FFmpeg profile transform, and one remux. In each arm the readers received six identical complete fragments while overlapping. When the lead reader detached, the remaining reader continued to receive fresh bytes. Both readers decoded H.264 and AAC without truncating their captures. Exact cleanup passed in both arms.

This comparison covers the normal shared fMP4 workflow on `65dbbbd`. Replica failover and a controlled resource comparison remain separate. [Machine-readable evidence](../scripts/rust-integration/m3-65dbbbd-profiled-fmp4.json)
