# M8 AMD64 default AIO scheduled backup and retention

Pinned Original Dispatcharr and the exact `8fc7eef0a702b57d3c6b20dac171478b38e0196e` Rust package each accepted an administrator's daily UTC schedule with retention set to one. Both preserved the schedule identity and configuration across a normal restart before the due minute. Original ran its stock Celery worker and DatabaseScheduler. Rust ran its ordinary AIO worker and Core Beat on both application generations with all eleven owner and scheduler overrides absent.

Each scheduler published exactly one due task. The authenticated status agreed with the Redis SUCCESS result, and the persisted Beat run count progressed from zero before and after restart to one after shutdown. Each task removed exactly one earlier owned archive and kept the new version 2 PostgreSQL ZIP. The audit reopened both retained archives locally and verified metadata, the `PGDMP` signature and file hashes. The scoped database projection stayed unchanged.

| Observation | Original | Rust |
| --- | ---: | ---: |
| Role generations restarted before the due minute | 3 | 1 |
| Durable scheduled runs | 1 | 1 |
| Earlier archives removed | 1 | 1 |
| Retained archive size | 41,256 bytes | 41,060 bytes |
| Exact container and host generations retired | 2 | 4 |

Preflight, owner, run and scoped controller exited 0. Both child processes settled and closed their output pipes, their claimed container and process generations were absent, and their owned container, network and volume inventories were empty. [Sanitized receipt and custody hashes](../scripts/rust-integration/m8-8fc7eef0-amd64-default-aio-scheduled-retention.json)

This run qualifies one UTC scheduled backup and retention case on the ordinary AMD64 package. The separate ARM64 pair qualifies manual backup and same-schema restore. Full M8 awaits the coordinator's remaining required backup lifecycle, datastore, admin and plugin scopes and full-service resource accounting. No comparative resource measurement was taken here. [ARM64 backup and restore pair](rust-m8-8fc7eef0-arm64-default-aio-backup.md) · [M8 matrix](../scripts/rust-milestone-batches/m478/matrix.json)

Original Dispatcharr, React, Django, Celery, PostgreSQL and dependency authors and contributors retain full credit.
