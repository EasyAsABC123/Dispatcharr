# Core plugin repository schedule on startup

The paired Compose fixture exercised pinned Original and packaged Rust
`f2db61e5072f86d4215c5d71d44e2a899257f64f` with Selected jobs and Beat disabled in Rust.
The ordinary first boot installed the six-hour plugin repository schedule.
A subsequent boot applied a persisted three-hour setting and preserved the
schedule row identity. A third boot with a zero setting removed the row.
Each backend cleaned up its unreferenced interval and preserved other rows,
apart from Original's own default backup schedule timestamp.

Core used the existing PostgreSQL writer budget and scheduler SQL. A shared
session advisory lock serialized interval lookup and row updates across Core,
Selected, and the repository settings API. Each of the three actual Rust
process generations exited with literal 0. Exact fixture teardown confirmed
four role generations and empty owned inventories.

The first Original fixture exited 1 after treating the collateral backup
schedule timestamp as an error. Its teardown passed. The corrected Original
run recorded that independent timestamp change and verified stable unrelated
row fields. The paired test observed the bootstrap SQL only. Scheduled
repository fetch and full default worker deployment remain separate M6 work.

[Redacted paired receipt](../scripts/rust-integration/m6-core-plugin-startup-f2db61e5.json).

The original Dispatcharr authors and contributors retain full credit.
