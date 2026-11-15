# Candidate9 Core, scheduling and background admission

Original Dispatcharr and dependency authors retain full application, algorithm,
protocol and license credit. React remains unchanged.

Runtime `3cd82adfb0c95bf30eb7cc537dceba21a0384464` passes formatting, compilation,
strict Clippy and the whole default workspace: **930 passed, zero failed,
751 external-fixture tests ignored** across173 result blocks. All1199 inputs
remain unchanged during execution and match Git. The [SDK evidence](../scripts/rust-integration/milestone-batch9/inventory.json)
retains the initial signature/format/type-inventory/lint failures, the earlier
928-test pass and the final pass. Ignored tests are not acceptance.

The experimental Main graph now includes actual Core rehash and recording-offset
handlers. Rehash uses SQL-backed snapshots/pages and Source1000-stream batches,
with renewable, owner-checked Redis lock tracking. Offset scheduling uses the
same owned SQL session through real revoke/booking callbacks. The earlier4096
stream/1024 account/512 future-row scan caps are removed. Wire, scratch, child
result and value-domain budgets remain explicit; unrestricted input or concurrent
mutation compatibility is not established. Date-only offset saves preserve the
original stored task key when Source skips rescheduling an already ended window.

Main now assembles one normal clocked/interval/crontab scheduler using the shared
pool and Jobs instance, cached due state, reserve/sync/reload behavior and actual
generic publications. Existing component constructors remain intact. This
calendar profile covers five retained timezone files and1970–2037; broader
calendar grammar, timezone history, routing and complete consumer coverage remain
open. The effective pinned DatabaseScheduler override installs backend cleanup
with Redis, and the existing normal cleanup consumer handles it; its actual new
producer-to-consumer parser boundary is tested.

A real scanner backlog exposed a separate Main construction bug: background Auth
had no HTTP ingress permit, but task SQL preparation required one. Background
work now acquires from the same four-slot writer budget; valid HTTP permits are
reused, foreign permits refused and exhausted capacity retried before claim.
The actual pool lease remains held until physical close instead of detaching and
allowing a replacement connection. Tests exercise the bare Main path and real
capacity, plus the exact previously queued frame and parent receipt. Full
production consumption still needs the corrected Docker cohort.

The [production build](../scripts/rust-integration/milestone-batch9-build/inventory.json)
is `sha256:946ebf97def2a3a3edf6019c622320a3c659e9e0f62db82ded8cf43db5cad860`.
All1691 archived inputs match Git:1194 Rust,495 React and two original credit
files. All114 earlier React/credit hashes and120 selected installed asset/notices
match; separate GPG/crypto/trust/license27-file checks pass. The image config uses
UID65532, `/app` and the production Rust entrypoint; its ELF hash is
`8f71f95516729c6931802ffb40ac77ab24facae9b64b63ff0a216ef36b7ba7ca`.
An actual default-entrypoint run reaches the expected missing-secret config
refusal with exit1. Every owned probe is absent afterward. This is build/startup
evidence, not application acceptance.

Both experimental flags remain off by default. New Core scaling cases, the
normal scheduler matrix and the affected recording cohort still require runtime
qualification. No new CPU/memory gain or complete M0–M9 pass is claimed.
