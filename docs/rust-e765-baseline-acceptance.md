# e765 permissions and management baseline

The exact `e7651a6d` production image passed 1,024 checks across 18 suites
in a fresh owned Compose project with pinned Original, PostgreSQL and Redis.
The run decoded 75 MPEG-TS video frames. Both backends returned HTTP 404
with the same response body for the malformed settings ID.

Execution, evaluation and independent cleanup each exited 0. The coordinator
exited 2 with one passing node and 126 blocked nodes. Full M9 acceptance
and comparative performance remain open.

The original Dispatcharr authors and contributors retain full application and workflow credit.

The [packaged provider workflow evidence](rust-e765-provider-workflows.md) covers the separate provider checks.

[Redacted receipt](../scripts/rust-integration/milestone-batch113/e765-baseline-acceptance.json)
