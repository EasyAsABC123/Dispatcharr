# Inactive-account notification component

Original Dispatcharr, Django, Celery, Kombu and Channels authors retain code, algorithm and license credit. This handoff graduates the previously fixture-only two-family default library into ordinary inert modules, and adds a bounded inactive-account expiration-notice cleanup domain. No Main/default registry or scheduler intake starts automatically.

The actual active expired profile under a disabled account is not evaluated. Stale active `xc-exp-warning-`/`xc-exp-expired-` notices are still deleted, including real NotificationDismissal rows. An ordered key SELECT precedes one collector transaction: child DELETE, then parent DELETE. SQL errors settle rollback and physical connection close. Only then are dismissal events sent using the injected existing UpdateEventSink. Actual RealtimeBus wrong-type failure is caught after the committed deletion and returns null, matching Source. Native capacity/invalid-message/unsettled errors stay operational.

Limits are 128 notices, 1024 related dismissals, 255 characters/1020 UTF-8 bytes per key, one worker admission and existing bounded Jobs command/candidate permits. Parent locks and a second related-count check prevent new FK references expanding the admitted collector. Concurrent mutation parity between key SELECT and collector, and event-order behavior on equal sort keys, remain unqualified. Fixtures use distinct explicit timestamps without post-diff sorting.

`Jobs::connect_defaults` creates explicit isolated authority. `KnownDomains::new` supports cleanup/disabled recurring; `KnownDomains::with_notifications(pool, sink)` adds this branch. `DefaultWorker::new` and `DefaultApplication::prepare/start/shutdown` expose inert explicit assembly. Other task families are refused. Drain every family before one final shared Jobs close. Only peers/resources are cfg(test). No active expiration handler, external HTTP fetch, recording, version check or full default-family claim is added.

The final binary passed134 checks across 12 actual worker cases, plus the earlier 58-check two-family regression and nine distinct actual Redis/SQL/Channels resource tests. 44 library tests, non-test library check/strict Clippy and all-target strict Clippy passed. Both final host runners exited 0; default database audits and owned cleanup passed. Earlier pair negative verifies nonzero aggregate failures cannot retain stale passed:true.

Source SQL-trigger failures produce real ProgrammingError/FAILURE. Native currently keeps these operational with no fabricated result (PENDING after earlyACK); the actual canceled-trigger rollback test proves children and parent remain after rollback and no false SUCCESS is emitted. Source-compatible native SQL failure-result encoding is an explicit remaining gate. Enabled account/profile evaluation and over-cap collections are refused before ACK, preserving FIFO; this is not Source earlyACK equivalence.

## Reproduce

The committed source-overlays.json names immutable cc31e2188bef9974e5979550424fa4b25a1f229f plus14 exact Git blobs and the complete704-file map. Reconstruction reads Git history, validates every byte and refuses extra/unowned paths; it reads no mutable Rust or ignored artifact directory. Cargo.lock adds only the cached job dev-edge to realtime (330 packages), never a new production sink implementation. Use Rust1.90/Cargo and Docker on PATH, with locked dependencies cached for offline commands. In the original local workspace, the optional tooling environment file can configure those paths; it is not a checkout prerequisite. Use a fresh output and a separate small target:

```sh
python3 scripts/rust-job-contracts/inactive-notifications/freeze_inactive.py --output artifacts/rust/inactive-rebuild-1 --repo "$PWD"
CARGO_TARGET_DIR="$PWD/artifacts/rust/target-inactive-rebuild-1" cargo clippy --manifest-path artifacts/rust/inactive-rebuild-1/rust/Cargo.toml --locked --offline -p dispatcharr-job-transport --all-targets -- -D warnings
CARGO_TARGET_DIR="$PWD/artifacts/rust/target-inactive-rebuild-1" cargo test --manifest-path artifacts/rust/inactive-rebuild-1/rust/Cargo.toml --locked --offline -p dispatcharr-job-transport --lib --no-run --message-format=json
# Copy the compiler JSON executable outside target before cleanup.
python3 scripts/rust-job-contracts/inactive-notifications/run_pair.py --repo "$PWD" --output artifacts/rust/inactive-rerun-1 --native-binary <copied-executable> --support-root scripts/rust-core-perf/support --resources
python3 scripts/rust-job-contracts/default-pair/run_pair.py --repo "$PWD" --output artifacts/rust/default-regression-1 --native-binary <same-executable> --support-root scripts/rust-core-perf/support --resources
```

Support is an explicit committed input, with no private fallback. Original source/archive/image and read-only observer are pinned by the runner and copied support; the observer is loaded from cc31 Git history. Actual task code is unchanged. Bulk fixture setup avoids unrelated save-signal publication and sets only copied rows. Raw fixture records contain private task IDs and copied rows: publish the sanitized dataset, not raw records. Source-only capture script is retained separately; it does not prove native behavior. Some original draft-header comments in the hash-bound child modules remain historical (including obsolete UNCOMPILED/fixture wording); the explicit regular module registry and actual tests define current scope.

Full source/tool maps and outside-target ELF remain under the bound artifact paths. Only the reviewed lib/transport authority/registry additions belong to this delivery. Main, attribution and automatic/default task intake remain unchanged.

Root independently verified the 134 raw assertions across12 cases, prior58-check regression, nine distinct physical tests and actual Docker cleanup. The exact704-input whole workspace passes383 default tests and strict all-target Clippy;617 fixture-dependent tests remain explicitly skipped in that run. Seven reconstruction/corruption controls pass. The published builder uses immutable Git blobs from this delivery history, without a private overlay directory. These checks do not enable Main or establish new resource gains.
