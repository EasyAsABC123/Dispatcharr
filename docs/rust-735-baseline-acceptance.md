# 735 permissions and management baseline

The exact `735a0299` production package passed all 1,024 recorded
checks across 18 unchanged suites in an isolated Compose project. Pinned
Original ran with disposable PostgreSQL and Redis. The run verified 75
decoded MPEG-TS video frames and the previously mismatched settings response.
Both backends returned HTTP 404 with the same generic response body for the
malformed settings ID. Execution, evaluation and independently verified
cleanup each exited zero. The coordinator exited 2 with one passing node
and 126 blocked nodes.

The [e852 observation](rust-e852-permissions-management-baseline.md) retains
the original failed response comparison. The nine C74 required passes remain
scoped to that earlier image.

Full M9 acceptance and comparative performance remain open.

The original Dispatcharr authors and contributors retain full application and workflow credit.

[Redacted receipt](../scripts/rust-integration/milestone-batch113/735-baseline-acceptance.json)
