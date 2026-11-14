# Candidate6 experimental worker assembly

Original Dispatcharr authors retain all application and algorithm credit. Celery,
Kombu, django-celery-beat, SQLx, rustls and other dependency authors retain their
code, protocol and license credit. The React frontend is unchanged.

Runtime `b7dafc1a586c6f1c5a466349ca5afa0f54639b14` passes the complete Rust
format, compilation, strict workspace lint and default test batch: **908 passed,
zero failed, 751 fixture-dependent tests ignored**, across 1,179 recorded inputs.
All four commands exit zero and every input remains unchanged during execution.
The [raw evidence and inventory](../scripts/rust-integration/milestone-batch6/inventory.json)
retain failed diagnostic attempts as well as the final result. The same shared
Cargo target was reused; no additional target directory was created.

The new Main assembly prepares the selected fourteen task families plus startup
recovery. Existing HTTP/file adapters retain their real services while a shared
consumer dispatches broker heads to the appropriate family. Recording consumption
supports twenty admitted tasks; ordinary work has six shared admissions, but some
family services still allow only one task at a time. These limits do not establish
full Source concurrency parity.

The experimental assembly includes normal DVR media, actual startup recovery,
clocked recording publication/scheduling, live statistics, scanner/M3U workers,
and a shared received-revocation set with real Redis pidbox subscription. Startup
waits for a served Main HTTP response and actual control subscription. Shutdown
stops intake before draining workers and keeps shared services available for
finalization. In this experimental path, an incomplete drain or deadline returns
an error; process termination may then close outstanding resources.

Both flags remain **off by default**:

- `RUST_EXPERIMENTAL_SELECTED_JOBS=1` enables this selected worker graph.
- `RUST_EXPERIMENTAL_SELECTED_BEAT=1` additionally enables the clocked-only
  scheduler, requires selected workers, and must run on exactly one instance.

This is not the complete Source scheduler: interval/crontab scheduling, the full
Source task inventory, wider supported domains, several external/restart frame
adoption paths, and normal Recording HTTP controls remain incomplete. Redis TLS,
control reconnect and targeted/terminate control operations remain unqualified.
The DVR path now uses the actual configured plugin directory, including Source's
directory creation behavior; populated plugin/hook execution is still unsupported.
Legacy captured constructors retain their existing behavior.

Two narrow backports retain the locked SQLx 0.8.6 packages and their original MIT
and Apache licenses. The verify-ca verifier recognizes rustls's contextual
hostname error while retaining chain/signature/time validation. PostgreSQL allow
keeps plaintext preference and retries a fresh encrypted connection after a TCP
startup/authentication database error, at each physical connection boundary.
Other allow I/O/EOF/address branches and prefer fallback remain unqualified.

A workspace regression package compiles the **exact complete vendored SQLx core
library** in its original crate scope. Its feature/dependency set uses existing
locked packages; unavailable optional backends and self-dev-dependencies are not
introduced. Narrow pre-existing upstream style lint allowances apply only to this
regression package. Real WebPKI checks pass for the contextual hostname mismatch,
wrong CA, expired/not-yet-valid certificate and bad signature. The
[upstream file index](../scripts/rust-integration/milestone-batch6/upstream-sqlx-files.json)
binds retained dependency bytes; only two upstream implementation files differ.

This SDK pass does not qualify production Main/Docker workflows or a performance
gain. The grouped Compose database and selected-worker runs are the next gates.
SQLite remains unsupported. All full migration milestones remain open; the
[packaged candidate5 results](rust-milestones.md) retain their own source scope.
