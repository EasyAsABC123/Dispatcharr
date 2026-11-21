# Native ARM64 PostgreSQL copy on cf64

The native runner rebuilt the exact Rust application `cf64fbfe776c9470cc62bc90c2e21216f9df204a`. The previous 21da result provided the ordinary workload. This run produced separate results for that workload and the copied data. [Machine-readable evidence](../scripts/rust-integration/m10-native-arm64-cf64-copied-postgresql.json)

All 12 ordinary Compose checks passed before the copy stage. The private package completed fresh install, first-admin login, restart, and two authenticated MPEG-TS readers with one upstream connection. The runner used its owned internal bridge without a published host port. [Hosted runtime](https://github.com/EasyAsABC123/Dispatcharr/actions/runs/44896188921/job/136822242982)

The candidate's PostgreSQL clients exported a custom archive and restored it into a vacant owned database. The copy contained 62 public tables and 184 migration records. The scoped account and channel/stream records matched before the copied Main started with initialization disabled. A fresh cache and data volume isolated the copy. The retained account logged in and read the copied channel. Only that database recorded the new account login timestamp. Exact cleanup covered the temporary archive, Compose roles, volumes and private image.

The scope is a same-engine copy of disposable candidate data. Original-origin upgrade, rollout, browser execution, and comparative performance remain separate. Full credit belongs to the original Dispatcharr authors and dependency contributors. [Original attribution](../ATTRIBUTION.md)
