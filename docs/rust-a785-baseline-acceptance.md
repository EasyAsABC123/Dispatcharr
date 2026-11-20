# a785 permissions and management baseline

The `a785a2b7` production image passed 1,024 checks across 18 suites in a
fresh owned Compose project with pinned Original, PostgreSQL and Redis.
The run decoded 75 MPEG-TS video frames. Both backends returned HTTP 404
with the same response body for the malformed settings ID.

Execution, evaluation and independently verified cleanup each exited 0.
The scoped coordinator exited 2 with one passing node and 126 blocked
nodes. The prior 735 image has a separate acceptance record. The standalone
backup HTTP routes have separate [default-off evidence](rust-m8-standalone-backup-create.md).
Full M9 and comparative performance remain open.

The [fresh a785 MPEG-TS functional checks](rust-m9-a785-functional-streaming.md) passed
all three selected nodes with independently verified cleanup.

The original Dispatcharr authors and contributors retain full application and workflow credit.

[Redacted receipt](../scripts/rust-integration/milestone-batch113/a785-baseline-acceptance.json)
