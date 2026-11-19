# Integration editing, daily XC schedules and Redis startup validation

The original Dispatcharr authors and contributors retain full application,
workflow and algorithm credit.

- Native Main accepts the integration edit and Enabled toggle from the
  existing React interface. It uses the existing admin policy, preserves
  omitted settings, and leaves subscriptions, logs and delivery untouched.
  The pinned Original passed all 18 requested edit cases.
- The XC account editor supports switching between interval scheduling and
  a basic daily cron schedule. It reuses the existing task and configured
  timezone and preserves shared schedule rows.
- Main and the operator reject contradictory Redis-backed Celery TLS
  settings at startup. The error names the conflicting setting and omits
  connection details. Redis TLS transport support remains open.
- The failover fixture now checks the backup-origin portion of each
  existing connection separately. All six retained Original and Native
  captures decoded 31 clean backup frames. Earlier startup results remain
  recorded.

All 528 default Store tests, six Main configuration tests and ten operator
tests passed. The Store suite skipped 79 cases that require external
fixtures. Formatting and strict all-target Clippy passed. Exact-image
Compose checks remain open for the new application paths.

[Focused checks](../scripts/rust-integration/milestone-batch82/focused.json)
· [Retained failover decode](../scripts/rust-integration/milestone-batch82/failover-decode-retained.json)
· [Milestone ledger](rust-milestones.md)
