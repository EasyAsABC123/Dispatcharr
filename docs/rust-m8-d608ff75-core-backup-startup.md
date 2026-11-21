# d608ff75 Core backup schedule startup

A fresh Compose comparison verified the backup schedule across four ordinary
web generations per backend. Native used the cache-backed packaged Core
entry point with Selected jobs and Beat off. The exact build manifest
SHA-256 is `c5fd0f753eea11c1af9263ca758250b783e404f55a1ff571dd541a6eb562f8d5`.

- A missing settings group and task row became the default daily 03:00 UTC
  schedule with retention 3.
- The next boot applied the configured Tuesday 14:10 UTC schedule with
  retention 5 and kept the existing task row.
- With scheduling disabled, the next boot removed the task and its orphaned
  cron row. Booting again left the Beat change marker unchanged.

Unrelated periodic rows stayed unchanged. The isolated broker observations
kept backup messages separate from PluginRefresh and other jobs. Neither arm
observed a queued backup message, result key, or archive in this workflow.
All four Native Main generations exited 0 and every owned Compose role retired.

The earlier stock fixture exits remain recorded. A clean stock replay supplied
the task-specific admission evidence. The tested scope covers startup
synchronization. Scheduled backup execution, full M8, and comparative
performance have separate gates.
[Redacted receipt](../scripts/rust-integration/m8-d608ff75-core-backup-startup.json)

The original Dispatcharr authors and contributors retain full credit for the application and its behavior. Dependency authors retain full credit for their work.
[Attribution](../ATTRIBUTION.md)
