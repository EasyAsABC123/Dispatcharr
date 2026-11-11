# Bounded multiple active-profile expiration task

Original Dispatcharr, Django, Celery, Channels, redis-py and dependency authors retain code, algorithm and license credit. Source is pinned to `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`. This successor changes only `active_notifications.rs`, `active_resources.rs` and the constructor documentation in `default_domains.rs`. No Main, library registry, Cargo/lock, transport, worker, task namespace or default intake changes are included.

The same inert `KnownDomains::with_active_notifications` constructor now supports 2–32 eligible profiles. The multi-profile path executes the exact captured Original all-column ORM SELECT with its original join/filter and **no added ORDER BY, ID sort or changed projection**. A read-only repeatable SQL snapshot retains admission/caller ownership through count, aggregate, fetch, commit/rollback and connection close. Multi input is capped at 64 KiB aggregate row JSON representation before fetching the full rows; the schema/protocol row framing adds bounded overhead. The existing zero/one-profile projection remains unchanged in authority: unused JSON is not newly fetched or capped in that older domain. Actual Source/native single-profile unused JSON of 70 KiB still succeeds.

Existing bounds remain 128 collector notices, 1024 global dismissals, profile names at most 237 characters, account names at most 255, years 1970–9999 and four retained service admissions. More than 32 eligible profiles, oversized multi input and existing unsupported title/date/count domains refuse before ACK without row mutation. The Source can accept inputs beyond these explicit native caps; no full-family claim is made.

Each selected profile gets its own fresh clock call and separate opposite collector/upsert/event phases. Every retained active key excludes its notice from final stale cleanup. The input snapshot ends before effectful transactions; no whole-task transaction is introduced. A failure on the second profile preserves earlier committed effects and skips later profiles/final cleanup as in Source. Unknown SQL failures still remain native operational/PENDING; this does not implement Source Python FAILURE serialization.

Actual worker gates include:

- Three profiles in original physical order, then deliberate owned CLUSTER reversal: Source iteration `[2,3,4]` becomes `[4,3,2]` and native events follow it.
- Multiple accounts, second-profile upsert failure, all-warning profiles, mixed new/existing notices and the 32-profile boundary.
- The compatibility control with one eligible profile and a 70 KiB unused JSON field.
- Nine real physical controls: existing SQL/Channels/upsert/rollback/cancellation/drain/legacy refusal, plus read-only snapshot cancellation and oversized multi-row refusal. The profile-count negative is 33; it leaves the queue head/result untouched.

The final actual Source/native corpus has 111 checks across eight cases, with exact notice/dismissal rows, IDs, event contents/order and partial-commit prefixes. IDs and sequences are restored before the paired execution. Event order is never sorted after a difference. Normal result UUID/date/TTL fields are validated in their own identity/time domain. Three earlier single-profile fault cases remain Source FAILURE versus explicit native operational/PENDING, as documented in the preceding component.

The same final executable passed the prior single-profile 314-check corpus plus the current nine physical tests, and the unchanged inactive-only 134-check/nine-control regression. Its independently frozen 713-input workspace passed strict all-target Clippy and 390 default tests, with 632 external tests ignored. Root's later VOD/UUID union is a separate source map and must be regated separately. No performance result transfers from these functional gates.

Reproduce after the selected immutable Git blobs are published:

```sh
. artifacts/rust/tooling/environment.sh
python3 scripts/rust-job-contracts/active-notifications-multi/test_reconstruct.py
python3 scripts/rust-job-contracts/active-notifications-multi/reconstruct.py --repo . --output /tmp/notification-multi-source
python3 scripts/rust-job-contracts/active-notifications-multi/build_peer.py --source /tmp/notification-multi-source --target /tmp/notification-multi-target --output /tmp/notification-multi-build
python3 scripts/rust-job-contracts/active-notifications-multi/run_native.py --repo . --output artifacts/rust/notification-multi-rerun --native-binary /tmp/notification-multi-build/native-default-peer --support-root scripts/rust-core-perf/support --resources
```

The public reconstruction uses base `2fbeabcd69802ef82c7dcb2be277d622e6eba7df` plus twenty immutable Git blobs and verifies every one of the exact 713 Rust/Cargo/dataset inputs. The only runtime delta from the preceding single-profile handoff is the three selected paths; do not replace a newer canonical tree wholesale with this older reconstruction. The public builder records compiler JSON/versions/source identity and copies the executable outside the generated target. Support is the committed `scripts/rust-core-perf/support`, with no private fallback.

`source-original-3/` retains the separately executed seven-case Source-only62-check capture. The eighth single-large-JSON case is an actual Original worker counterpart in the final paired corpus, not a Factory approximation. Task-scoped dependency clock control is explicit; task/helper/ORM/Channels/result implementations remain pinned Original. Worker-local proxy managers are empty; no effects in a separate HTTP process are inferred.

Initial draft Clippy failed while the Option-to-Vec loop refactor was incomplete, and is retained privately. Early native4 qualified the seven-case corpus before compatibility review noticed the full-row byte cap should not narrow the prior single-profile projection. The final source preserves that projection and adds the actual large-JSON regression; earlier proof remains historical. No captured failures have been discarded or relabeled.

Remaining: concurrent profile/key mutation, arbitrary planner/GUC/physical-layout combinations, broader date/title/count/byte domains, profile-save parsing/signals, same-process nonempty proxy managers, Python failure metadata, other configured default families and actual scheduler/application/Main/default enablement. Source's absent profile ORDER BY is preserved rather than replaced with an invented universal order.
