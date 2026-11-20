# e765 permissions and management baseline

The exact `e7651a6d` production image passed 1,024 checks across 18 suites
in a fresh owned Compose project with pinned Original, PostgreSQL and Redis.
The run decoded 75 MPEG-TS video frames. Both backends returned HTTP 404
with the same response body for the malformed settings ID.

Execution, evaluation and independent cleanup each exited 0. The coordinator
exited 2 with one passing node and 126 blocked nodes. Full M9 acceptance
and comparative performance remain open.

The [e765 OutputProfile and provider modal browser checks](rust-m9-e765-output-profile-and-provider-browser.md) passed their full owner workflows.

The original Dispatcharr authors and contributors retain full application and workflow credit.

The [packaged provider workflow evidence](rust-e765-provider-workflows.md) covers the separate provider checks.

The [e765 createsuperuser and strict typed comparisons](rust-e765-createsuperuser-and-typed154.md) cover a separate coordinator run.

The [operator, guide and authentication checks](rust-e765-operator-guide-auth.md) cover the separate six-node run.

[Redacted receipt](../scripts/rust-integration/milestone-batch113/e765-baseline-acceptance.json)

The MPEG-TS runs covered shared demand, delayed restart and bounded
retention, with exact cleanup after each run. [MPEG-TS functional checks](rust-m9-e765-functional-streaming.md)

Across seven fresh coordinator scopes, this exact package has 17 distinct
passed nodes out of 127. The remaining 110 nodes still need qualification
on this package. [Combined required-node evidence](../scripts/rust-integration/milestone-batch113/e765-current-required-node-coverage.json)

The bounded streaming core passed all 33 host tests. Cargo compiled both
crates from a workspace whose 1,496 Rust files matched this package.
[Core test evidence](../scripts/rust-integration/milestone-batch113/e765-bounded-streaming-core.json)
