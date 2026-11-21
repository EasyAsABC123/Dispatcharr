# 814b9403 Core backup schedule startup

A fresh Compose replay of the [d608 startup workflow](rust-m8-d608ff75-core-backup-startup.md)
verified four ordinary web generations per backend. Packaged Rust Core
ran with Selected jobs and Beat disabled. Build manifest SHA-256
`46d1ec93035cac53d7e71d79f93db30fd93f0312b37ceb43d93929db565151a2` identifies the tested package.

- Startup created the daily 03:00 UTC backup schedule with retention 3.
- The next boot moved the same task row to Tuesday 14:10 UTC with
  retention 5 and preserved other periodic rows.
- Disabling scheduling removed the task and retired the unreferenced
  cron row. The next disabled boot left the Beat marker unchanged.

The owned broker showed zero backup messages and result keys, and
neither arm created an archive. Original PluginRefresh observations
rose from two to six across the checkpoints. Rust showed six throughout.
All four Rust Main generations exited zero. The four owned Compose
roles and their process generations retired with empty project inventories.

Scheduled execution, archive import, M8 completion and comparative
performance have separate gates. [Redacted receipt](../scripts/rust-integration/m8-814b9403-core-backup-startup.json)

The original Dispatcharr and dependency authors retain full credit.
[Attribution](../ATTRIBUTION.md)
