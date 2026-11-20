# e852 permissions and management baseline

The exact e852 package ran `package/baseline-all` in a private Docker Compose
project against pinned Original. The unchanged runner recorded 1,024 checks
across all 18 suites. Of those checks, 1,023 passed, including the ordinary
75-frame MPEG-TS decode.

One standard-role GET failed the response-body comparison at
`/api/core/settings/not-a-number/`. Both backends returned HTTP 404.
Original returned `{"detail":"Not found."}`, and the packaged Rust backend
returned `{"detail":"No CoreSettings matches the given query."}`.
The checked fixture exposed a response mismatch in the production settings
detail route.

Execution and evaluation exited 1. Cleanup exited 0, and independent
inventory confirmed that the owned containers, networks, and volumes were
gone. The overall coordinator exited 2 with this one node failed and 126
blocked. The nine required C74 passes remain scoped to the separate C74
package. Full M9 and comparative performance remain open.

The original Dispatcharr authors and contributors retain full application and workflow credit.
[Redacted failed observation](../scripts/rust-integration/milestone-batch113/e852-baseline-all.json)
