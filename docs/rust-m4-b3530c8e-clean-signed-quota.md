# B353 clean signed VOD session attribution

The clean paired Docker test verified zero anonymous or malformed owners before each signed secondary admission. Both release and cancellation cases completed with real audio and video, exact grants, and exact teardown. The independent replay matched the physical report byte for byte. [Typed evidence](rust-m4-b3530c8e-clean-signed-quota.json)

| Same signed fixture | Original | Rust B353 |
| --- | --- | --- |
| Effective retained session and active owner | Prior session and prior viewer | Requested alias and requester |
| Prior viewer’s secondary request | HTTP 429 | HTTP 429 |
| Requester’s secondary request | HTTP 206 | HTTP 429 |

This confirms a session adoption and attribution difference even with the clean owner census. Both arms maintained one provider grant during the held request and retired all grants afterward. The separate anonymous session and Origin Range observation remains adverse. Rust’s configured quota enforcement remains part of the parity contract. Original Dispatcharr and all dependencies retain full credit.
