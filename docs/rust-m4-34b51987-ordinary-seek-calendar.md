# M4 Numeric seek and calendar Range regression

Original Dispatcharr contributors retain full application and protocol credit.

Pinned Original and packaged Rust revision `34b5198793164aee8077665d73bff9f697904014` each passed five ordinary numeric seek cases and five catch-up calendar cases. All 20 authenticated requests returned HTTP 206 and the matching complete 65,536-byte ranges. Every case made exactly one upstream request per backend with the expected query and session metadata.

The two plans ran in separate internal Compose projects. Both exited 0, and independent cleanup verified all eight owned role generations retired and both project inventories were empty. Rust ran its normal packaged Main. The only changed Rust file in either tested route footprint since 8241 was the reviewed VOD response-body ownership change.

This result covers those ten ordinary Range cases. Full M4, provider-capacity timing, and performance verification remain open.

[Redacted receipt](../scripts/rust-integration/m4-ordinary-seek-calendar-34b51987.json)
