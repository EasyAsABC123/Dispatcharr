# Selected worker startup readiness

The original Dispatcharr authors and contributors retain full application,
workflow and algorithm credit.

Selected Main now waits for work or an observed queue state on both the Celery
and DVR lanes before marking the worker graph ready. A successful empty broker check while a
Celery due task is retained marks the lane working, before that task completes.
The local HTTP startup probe accepts the pending readiness response while the
worker graph starts.

The locked job-transport suite passed 284 tests, including the two new
readiness regressions. The suite ignored 113 external-fixture cases. Formatting
and strict all-target job-transport Clippy passed. The exact `a1e2231b1` Docker
build preserved all 497 React, license and attribution inputs. The startup
fixture accepted that image and rejected Candidate85. Packaged readiness
execution, deployment activation and performance remain open.

[Focused results](../scripts/rust-integration/milestone-batch86/focused.json)
· [Docker build and image binding](../scripts/rust-integration/milestone-batch86/packaging.json)
· [Milestone ledger](rust-milestones.md)
