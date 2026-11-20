# 976f permissions and management baseline

The exact `976f2867` production image passed 1,024 checks across 18 suites
in a fresh owned Compose project with pinned Original, PostgreSQL and Redis.
The run decoded 75 MPEG-TS video frames. Both backends returned HTTP 404
with the same response body for the malformed settings ID.

Execution, evaluation and independent cleanup each exited 0. The coordinator
exited 2 with one passing node and 126 blocked nodes. The a785 baseline has
its own receipt. [Standalone backup modes](rust-m8-standalone-backup-create.md)
were exercised separately on 976f. Full M9 acceptance and comparative
performance remain open.

The original Dispatcharr authors and contributors retain full application and workflow credit.

[Redacted receipt](../scripts/rust-integration/milestone-batch113/976f-baseline-acceptance.json)
