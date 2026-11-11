# Scoped Redis backend cleanup expiry

Original Dispatcharr, Celery, Kombu, Django, Channels and Redis authors retain their code, algorithms and licenses. This is a normal inert Rust library component. Main/default intake and native default-scheduler enablement remain off. No full task-family, generic expiry, crash-recovery or performance claim is made.

Pinned actual Beat emits `celery.backend_cleanup` with expires43200, its periodic name, UTC deadline and canonical transport milliseconds43200000. Redis retains aged messages: the worker checks the deadline. Actual expired requests write typed REVOKED/TaskRevokedError(expired), then ACK, and never execute the body. A fresh delivery of that UUID in the same worker is revoked; a fresh worker without a state file can execute it. Existing SUCCESS bytes and aged TTL stay protected.

The consumer preserves this ordering with a bounded cleanup receipt. A real held backend reply retains unacked although metadata is already committed. Backend denial leaves PENDING/unacked until warm stop restores the redelivered message. Unknown writes never authorize ACK. Unexpired cleanup dispatches early ACK and actual noop/result concurrently: actual Source and native SUCCESS are observable while an ACK response is held beyond expiry. There is no expiry recheck after that ACK. Two existing command credits are needed for the unexpired branch; one-credit configurations refuse before POP. Sequential expired handling works with one. No caps are raised.

Receipts retain caller/family admission and bounded frame/count credit through physical commands. Existing reservation Lua and exact owner comparisons preserve foreign tuples/TTLs. Jobs shutdown accounts for receipts. Final-owner stop signals are implemented; physical caller cancellation and explicit shutdown are qualified by actual delayed reply tests. External revoke/state-file persistence, generic lost-owner/IO, full visibility sweep/cadence and all unsupported families remain separate gates.

## Evidence and its limits

The exact final executable passed63 ACK-boundary checks plus the same-executable83 expiry,134 inactive and58 original pair regressions. Fifteen distinct real Redis/SQL/Channels ownership/capacity controls passed. Whole workspace:390 default tests/623 ignored/166 suites and strict all-target Clippy; job library51 units. Local socket tests required socket-enabled execution; the prior sandbox-only failure is retained. Each final host fixture exited0, audited the default database unchanged and removed its owned project.

Source ACK evidence uses actual unmodified prefork workers, real backend pipeline replies/ACL and broker ACK replies. Native consumers use ordinary compiled protocol/domain/worker/transport modules; only peers and resource tests are cfg(test). Source AsyncResult decodes native revoked errors. No Python exception import or new HTTP projection is implemented by the native decoder. Unknown exception namespaces/reasons stay rejected; factory-seeded `children:null` remains an explicit negative rather than widening the existing result domain.

Initial early-pop expiry proof was only final-state evidence and is retained as partial. The reservation fix supersedes its ACK claim. Failed signal observer, mismatched proxy readiness and initial nullable-factory corpus attempts remain recorded. Concrete bound source/data/log hashes are in the verified summary. Some original draft wording in inherited module headers is historical; the ordinary module declarations and actual executed gates define current scope.

## Reproduce from published objects

Use Rust1.90, Docker Compose and the locked cached Cargo dependencies. The source reconstructor reads immutable Git history only, never mutable Rust or private artifact fallbacks. The published commit must contain the17 manifest Git blobs. `test_reconstruct.py` supplies seven exact-tree/corruption controls. Before publication, those same controls were run with an explicit private Git-object store; that validation setup is not a public prerequisite.

```sh
python3 scripts/rust-job-contracts/cleanup-expiry/reconstruct.py --repo "$PWD" --output artifacts/rust/cleanup-expiry-source-1
python3 scripts/rust-job-contracts/cleanup-expiry/build_peer.py --source artifacts/rust/cleanup-expiry-source-1 --target artifacts/rust/target-cleanup-expiry-1 --output artifacts/rust/cleanup-expiry-build-1
python3 scripts/rust-job-contracts/cleanup-expiry/ack/run_pair.py --repo "$PWD" --output artifacts/rust/cleanup-ack-rerun-1 --native-binary artifacts/rust/cleanup-expiry-build-1/native-default-peer --support-root scripts/rust-core-perf/support --resources
python3 scripts/rust-job-contracts/cleanup-expiry/expiry/run_pair.py --repo "$PWD" --output artifacts/rust/cleanup-expiry-rerun-1 --native-binary artifacts/rust/cleanup-expiry-build-1/native-default-peer --support-root scripts/rust-core-perf/support --resources
python3 scripts/rust-job-contracts/cleanup-expiry/regressions/inactive/run_pair.py --repo "$PWD" --output artifacts/rust/cleanup-inactive-rerun-1 --native-binary artifacts/rust/cleanup-expiry-build-1/native-default-peer --support-root scripts/rust-core-perf/support --resources
python3 scripts/rust-job-contracts/cleanup-expiry/regressions/pair/run_pair.py --repo "$PWD" --output artifacts/rust/cleanup-pair-rerun-1 --native-binary artifacts/rust/cleanup-expiry-build-1/native-default-peer --support-root scripts/rust-core-perf/support --resources
```

Each output must be fresh. The runners require the committed portable support tree explicitly, pin Original source/image and use only copied disposable databases/broker8/result9. The read-only observer comes from immutable cc31 Git history. Normal library dependencies are unchanged from2fbe704; no new Cargo edge is needed. Use a runtime-compatible build environment for the peer; build.json records its actual compiler/source/ELF. A fresh rebuild is not an assertion of byte-identical ELF or measured gains.

For whole-workspace checks, copy the five committed `scripts/rust-timeshift-contracts/python-*.json` external include fixtures from immutable2fbe into the reconstructed root, preserving their paths, before Cargo. These are compile/test inputs only and do not alter the711 Rust-root file map. Original code credit remains with the original authors.
