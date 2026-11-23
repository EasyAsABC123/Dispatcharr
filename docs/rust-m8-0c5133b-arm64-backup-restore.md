# M8 current paired ARM64 backup inventory and restore

The pinned Original and ordinary Rust application `0c5133b` passed an
isolated same-schema database backup workflow on Lappy. Each arm
created its own normal fixture archive, listed it, and downloaded
bytes matching its own archive. Each authenticated restore request
returned 202 and reached the expected terminal. The restored data
and account were verified. The unrelated control and archive stayed
intact.

Original used its normal dedicated Celery restore worker. Rust used
its explicit standalone restore-owner configuration with the other
workers disabled. Both arms exited 0 and verified exact namespace
cleanup. The result covers the explicitly configured restore role and
ordinary ZIPs. Default rollout and the wider backup matrix remain open.

Original Dispatcharr, its React frontend, and its dependencies retain
their existing attribution and licenses.

[Machine-readable receipt](../scripts/rust-integration/m8-0c5133b-arm64-backup-restore.json)
