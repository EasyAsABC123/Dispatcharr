# Scoped M5, M7 and M8 Compose comparisons

Original Dispatcharr authors and contributors retain full application and algorithm credit. React, Django, PostgreSQL, Redis, FFmpeg and other dependency authors retain full credit for their work. See [upstream attribution](../ATTRIBUTION.md).

| Scope | Tested Rust revision | Paired result |
| --- | --- | --- |
| M5 manual channel edit | `5d9568f` | Set EPG and reorder the existing streams returned HTTP 200 on both backends. One selected programme job completed, XMLTV changed from programme A to B, and two existing stream links and one profile membership survived. |
| M7 realtime | `1124391e` | Five WebSocket observations matched per backend. The disconnect webhook case recorded one disconnect audit event and delivered one webhook. Audit-only recorded one disconnect audit event with zero disconnect deliveries. |
| M8 plugin metadata and settings | `1124391e` | Standard and administrator list, settings write and settings readback returned HTTP 200 on both backends. Cold Native registration and persisted settings matched. The disabled plugin fixture remained unloaded. |

All three campaign owners, physical pairs and independent proof phases exited 0. The proofs bind the exact published Rust package identities and verify owned Compose cleanup. The M5 fixture exercised React's request sequence directly. This evidence applies to the listed images and workflows. Browser acceptance, application performance, the remaining milestone cases, and the newer runtime image have separate gates. [Machine-readable evidence](../scripts/rust-integration/m5-m7-m8-scoped-compose.json)
