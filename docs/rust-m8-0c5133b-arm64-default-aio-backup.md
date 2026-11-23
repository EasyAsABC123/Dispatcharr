# M8 ARM64 default AIO backup and restore

On November 23, 2026, the pinned Original application and the exact Rust application `0c5133b62dc9f2df0900f2228e25b75baecb6754` each completed an ordinary PostgreSQL backup and restore on Lappy. Rust used the default Selected Core worker and Beat enabled by `DISPATCHARR_ENV=aio`, with no backup or scheduler owner overrides. Original used its own web process and one stock Celery consumer. Rust's application and database and Original's application, database, and worker retained their respective physical generations throughout the round trip.

| Backend | Create | Archive | Restore |
| --- | --- | ---: | --- |
| Original | HTTP 202, signed status completed, Redis SUCCESS | 36,826 bytes | HTTP 202, signed status completed, Redis SUCCESS |
| Rust | HTTP 202, signed status completed, Redis SUCCESS | 37,367 bytes | HTTP 202, signed status completed, Redis SUCCESS |

Each application created its own version 2 PostgreSQL ZIP. Its inventory and download matched the file it created, and restoring that file recovered the archived account, typed setting, and migration inventory after a guarded change to the fixture database. Both applications accepted a fresh login using the restored password and returned 401 for the replacement password. The separate control database, file marker, and archive remained intact. The two ZIPs had different sizes and SHA-256 digests. This run establishes per-backend round trips for the same contract. Cross-backend archive interchange, scheduled backup execution, broader schema and plugin coverage, and comparative performance remain outside the evidence.

All four task IDs were distinct. Both fixtures observed three consecutive zero queue and reservation samples before create and after each task. Rust reported that its default scheduler was ready before and after restore. Original's stock worker registry and logs corroborated both task sequences and post-restore migration. The pair and independent audit exited 0. Teardown verified exact absence of 13 containers, two networks, and four volumes, with both Compose inventories empty.

The first attempt completed both Original tasks and stopped before Rust when a fourth login received 429 under Original's three-per-minute limit. That attempt also cleaned up exactly. The retry reused the initial bearer for the restore request and qualified the final credential checks within three login requests.

Original Dispatcharr, its React frontend, and dependency authors retain full credit. [Machine-readable evidence](../scripts/rust-integration/m8-0c5133b-arm64-default-aio-backup.json)
