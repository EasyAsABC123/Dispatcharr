# VOD same-SID non-range body characterization

A fixed once-run Source/Native image51 pair exited0. Two same-content/SID HTTP clients continuously drained under max2 seeded once. Both returned200 and progressed identically through fixed body checkpoints; four retained1MiB prefixes each decoded75frames over3s with audio/video. No status/counter-conditioned inputs, cap mutation, Redis delay, admin stop or retry was used.

Each backend emitted two overlapping media GETs with literal absent Range/If-Range, upstream200, declared Content-Length4299455 and no Content-Range. Their successful origin writes were1376256/1245184 bytes, partial transfers closed at fixed teardown. Source reported ConnectionResetError and Native BrokenPipeError on final write attempts; these remain distinct. Origin7 accepted sockets comprised5 control and2 media requests, not7 viewers. Actual client bytes, origin writes, handler return, response-finalization and physical close are distinct.

The result characterizes sustained non-range bodies, not short one-byte/header-only probes. Upstream request/client association remains unresolved beyond known role/request records. Same SID may be seek/retry by one person. Pinned per-view Original/Native GET contexts do not establish dynamic identity or justify a pooling change. Existing TS single-origin sharing is separate; no full M4, counter/timer, multi-user or performance claim transfers.

All owned resources settled and independent cleanup/default9-table/sequence audits passed. [Portable evidence, exact87 restoration and25 offline coverage/corruption controls](../scripts/rust-vod-contracts/sharing-characterization/README.md) bind the Source archive, candidate772, Root50 controller controls and raw projections. No Source/runtime business code changed.
