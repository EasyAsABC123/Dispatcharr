# Streaming recovery and 100 simultaneous viewers on c1ff

Pinned Original and exact Rust `c1ff4005` each served 100 concurrent authenticated MPEG-TS viewers. Each backend shared one upstream during the overlap, and every viewer showed media progress and decoded audio and video. All five owned roles retired. [Exact paired and formal receipts](../scripts/rust-integration/m2-m9-c1ff4005-streaming-and-100-viewers.json)

The same Rust image passed three required nodes for shared-viewer recovery, recovery after both workers exited, and bounded retained history for 100 late reads. The scoped coordinator's literal exit was 2 with three of 127 required nodes passing. The formal and combined proof steps both exited 0.

These are functional streaming results. Comparative CPU and working-memory tests and full milestone acceptance remain open. Original Dispatcharr and dependency contributors retain full credit. [Original attribution](../ATTRIBUTION.md)
