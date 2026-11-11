# Settled notification P0001 terminal failures

Original Dispatcharr, Django, psycopg, Celery, Kombu, Channels and Redis authors retain application/framework code, algorithms and license credit. This is a bounded normal-library extension; no Main, default consumer, Cargo, parent transport or task-domain authority is added.

Exact source is the qualified current746 Stats/VOD/multi tree plus twelve selected job-source/QA changes, yielding748 inputs. The source map is included; the actual outside-target peer is SHA256 `9224535d195ea8a630a2ce376b4fcb626692827533c1e4057ed7507d5668b585`. Strict all-target whole-workspace Clippy and462 default tests passed (646 external tests ignored). Source713 and image48 remain historical separate inputs.

## Execution boundary

`Domains::execute_terminal` is additive and defaults to the existing execute method. Existing KnownDomains execute APIs still return their original normal/error outcomes. The explicit notification implementation records only errors from DELETE child/parent or profile INSERT/UPDATE statements at the opposite collector, profile upsert or final collector stages. Statement rollback and physical connection close must both settle before classification; close uncertainty clears the classification candidate while preserving legacy error precedence.

Only PostgreSQL ERROR severity with SQLSTATE P0001 and a supported bounded field rendering becomes a typed `django.db.utils.ProgrammingError` with one string argument. The existing Source35/70-proven primary/detail/hint/CONTEXT renderer is now a regular pure module. Unknown SQLSTATE, SQL positions, oversized fields, reads, admission/count/snapshot errors, pool/IO faults, uncertain rollback/close and unclassified errors remain operational. No coarse Sql-error-to-Python mapping exists.

The narrow notification terminal writer uses the existing physically supervised metadata command, old-SUCCESS guard, TTL3600 fixture and result publication. It rejects raised notification failures for other task kinds. Metadata IDs/date/children are owned by the worker, and argument/diagnostic/date bounds are enforced before transport. The typed default worker reports Failed only after the actual metadata write reply settles. Its poller exposes FailedTask and continues to the next known task; operational errors still produce Fault and stop intake.

The traceback is explicitly an honest native diagnostic beginning `Native Dispatcharr account expiration task diagnostic`. It does not claim Python frames executed. Exception module/class/args, status, side effects and consumer behavior are separate claims from raw traceback-text equality, which is false by design and recorded in the data.

## Actual checks on one final executable

- Source-only same-family63 witness: real first SUCCESS/null, second same UUID actually fails P0001, original raw SUCCESS and agedTTL120 remain unchanged. No synthetic success seed.
- Paired HTTP/result/same-ID87 checks +five real physical controls. Actual Source producer, native real notification SQL, actual Source AsyncResult class/args and dedicated real Source WSGI consumer readback are exercised. Known failures yield the same JSON500 primary+CONTEXT detail through Source's backup-specific status view; healthy notification null also yields the original NoneType.get JSON500. No native status route is added.
- Single-profile317 +nine physical controls; multi-profile112 +nine; inactive160 +nine, now including both actual native child/parent DELETE faults and Source exception decoding. Exact rows/events/committed prefixes remain intact.
- Physical controls hold a committed result reply past timeout/caller abort and verify retained caller/admission/failed shutdown; drop a committed result reply and retain operational uncertainty; deny backend SETEX without writing a fake terminal; reject unobserved P0002 classification; and prove a known failed task does not stop the poller before a subsequent normal known family.

Each host controller actually exited0, copied/default database audits passed, and all owned projects were removed. Same-ID native evidence records one real domain execution on the second call, AlreadySucceeded, unchanged raw success and agedTTL. No result-cache execution skip is introduced.

Retained failures: initial all-target compile found a missing exhaustive Run::Failed poller arm; a first new physical fixture used10ms below the existing25ms poll minimum and its uncleared owned trigger caused the next control to fail; final fixtures use the existing25ms minimum and independent owned-trigger cleanup. A new negative unit exposed an omitted private encoder date guard; the explicit guard was added and all final gates replayed. Failed snapshots/logs remain in artifacts; no runtime acceptance is inferred from partial passes.

## Reproduction

After parent publication makes the twelve immutable Git blobs available, reconstruct source from canonical `fda49b16d5e3aca1e2c95ce01fc2b4d558e77572` and the included complete base/overlay maps:

```sh
# Requires the tested Rust 1.90/Cargo toolchain and Docker Compose on PATH.
python3 scripts/rust-job-contracts/notification-terminal/test_reconstruct.py
python3 scripts/rust-job-contracts/notification-terminal/reconstruct.py --repo . --output /tmp/notification-terminal-source
python3 scripts/rust-job-contracts/notification-terminal/build_peer.py --source /tmp/notification-terminal-source --target /tmp/notification-terminal-target --output /tmp/notification-terminal-build
python3 scripts/rust-job-contracts/notification-terminal/http/run_native.py --repo . --output artifacts/rust/notification-terminal-http-rerun --native-binary /tmp/notification-terminal-build/native-default-peer --support-root scripts/rust-core-perf/support --resources
python3 scripts/rust-job-contracts/notification-terminal/single/run_native.py --repo . --output artifacts/rust/notification-terminal-single-rerun --native-binary /tmp/notification-terminal-build/native-default-peer --support-root scripts/rust-core-perf/support --resources
python3 scripts/rust-job-contracts/notification-terminal/multi/run_native.py --repo . --output artifacts/rust/notification-terminal-multi-rerun --native-binary /tmp/notification-terminal-build/native-default-peer --support-root scripts/rust-core-perf/support --resources
python3 scripts/rust-job-contracts/notification-terminal/inactive/run_pair.py --repo . --output artifacts/rust/notification-terminal-inactive-rerun --native-binary /tmp/notification-terminal-build/native-default-peer --support-root scripts/rust-core-perf/support --resources
```

All four interop runners are exact executed frozen tools. After quiet release and Root made the reviewed Git blobs available, all12 public closure/corruption controls passed, Git-only reconstruction matched every frozen748 input, and a fresh public rebuild passed. The rebuilt executable SHA256 is `35bd956a19fa5c6353bd8dc77f1d3e11f0557a3dbb29b6235cba8b31f4df06b1`; it differs from historical9224 and is retained as a separate proof. Actual public-path replay passed87 Source/native HTTP/result/same-ID checks and allfive physical controls, hostexit0, unchanged defaultDB and removed project. Runtime inputs are unchanged; no performance results transfer. The exact build/source/compiler/replay records are bound in the summary public_portability field. No private source or support fallback exists: committed `scripts/rust-core-perf/support` is explicit. Never replace newer canonical source wholesale with an older reconstruction.

Remaining: other SQLSTATE/position/read/connection failures, full Python traceback equivalence, worker monitoring-event parity, other unported default families, and actual application/default intake integration. No milestone, full family, public native HTTP or performance claim is made.
