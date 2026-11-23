# M8 ARM64 default AIO backup and same-schema restore

Pinned Original Dispatcharr and the exact Rust application `8fc7eef0a702b57d3c6b20dac171478b38e0196e` each completed an ordinary queued PostgreSQL backup and same-schema restore in isolated Docker Compose on Lappy. Rust ran default AIO Selected Core and Beat without owner overrides. Original used its stock Celery worker. The ARM64 candidate image and build manifest are bound in the [sanitized receipt](../scripts/rust-integration/m8-8fc7eef0-arm64-default-aio-backup.json).

| Backend | Create response | Archive | Restore response |
| --- | --- | ---: | --- |
| Original | HTTP 202 | 36,818 bytes | HTTP 202 |
| Rust | HTTP 202 | 37,350 bytes | HTTP 202 |

All four distinct task identities reached `completed` on the signed status endpoint and `SUCCESS` in Redis with matching results. Each generated a version 2 PostgreSQL ZIP, and inventory and signed download matched its own archive. After an owned SQL change, restoring that archive recovered the archived account, typed setting and migration inventory. The restored password accepted a fresh login and the replacement returned 401. The independent control database, file marker and archive remained unchanged across the restore.

Both fixtures observed three consecutive empty queue and reservation samples before creation and after both tasks. Rust reported its default scheduler ready before and after restore. Original's worker logs corroborated task completion and post-restore migration. The apps and their databases retained stable process generations, as did Original's worker.

Cold preflight, actual run, reducer and outer owner exited 0. The audit on Lappy rechecked the private receipts and operation journals, including exact removal of 13 container IDs, two network IDs and names, and four volumes. Both project inventories were empty, and neither fixture published a host port.

This checks one same-schema archive round trip per backend. Cross-backend archive interchange, natural scheduled execution, wider restore and plugin coverage, comparative performance and full M8 acceptance remain open. The [earlier ARM64 retry](rust-m8-0c5133b-arm64-default-aio-backup.md) records the original three-login fixture budget.

Original Dispatcharr, React and dependency authors and contributors retain full credit.
