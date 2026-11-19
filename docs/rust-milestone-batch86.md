# Selected worker startup readiness

The original Dispatcharr authors and contributors retain full application,
workflow and algorithm credit.

Selected Main now waits for work or an observed queue state on both the Celery
and DVR lanes before marking the worker graph ready. Starting the lane task
alone does not establish readiness. A successful empty broker check while a
Celery due task is retained marks the lane working, before that task completes.
The local HTTP startup probe accepts the pending readiness response while the
worker graph starts.

The locked job-transport suite passed 284 tests, including the two new
readiness regressions. The suite ignored 113 external-fixture cases. Formatting
and strict all-target job-transport Clippy passed. Packaged readiness,
deployment activation and performance remain open.

[Focused results](../scripts/rust-integration/milestone-batch86/focused.json)
· [Milestone ledger](rust-milestones.md)
