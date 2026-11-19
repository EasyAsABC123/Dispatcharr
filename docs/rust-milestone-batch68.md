# Candidate68 XC account lifecycle

Original Dispatcharr and dependency authors retain credit for the application,
models, algorithms and protocols. React continues to use the existing account
API.

Candidate68 implements bounded JSON XC account creation through the normal
Rust job workers. It reserves worker capacity before saving the account, then
builds the create response before inline setup updates the account. Active
accounts can publish the real profile refresh task and load groups and optional
VOD categories during creation. The category-only delete path removes the account
relationships and preserves global category rows, including orphaned categories.

The worker releases its slot after a confirmed provider error and cleanup.
If broker admission proves that nothing was published, inline Groups can
continue. An uncertain publication retains physical ownership.

[SDK116](../scripts/rust-integration/milestone-batch68/SDK116-terminal.json)
passed formatting, workspace compilation, strict Clippy and workspace tests
on the exact fifteen-file revision 5d6b82cd18b1dffb637a34330f46f8c8a4c0b64e.
The workspace recorded 1,240 passes, zero failures, and 802 ignored tests.
All 1,469 bound inputs stayed unchanged. The earlier draft flagged one lint
and its HTTP tests could not bind loopback in the sandbox. The final checks
used the corrected code and host loopback.

The [Docker release build](../scripts/rust-integration/milestone-batch68/build-execution.json)
exited 0 in 322.633 seconds. The image source matches the SDK116 Rust snapshot,
and the frontend, license and attribution inputs match Candidate67 byte for byte.

All six [package probe families](../scripts/rust-integration/milestone-batch68/package/inventory.json)
passed on each installed image. The linked inventory covers the corrected
image. Each independent cleanup check confirmed all 55 recorded probe
containers were absent.

The [original-backend reference](../scripts/rust-integration/milestone-batch67/xc-source11-checkpoint.json)
covers twelve HTTP calls across eleven cases. The Native test uses that
reference for account creation, group selection, first refresh and deletion.
The [first Native run](../scripts/rust-integration/milestone-batch68/native-xc11-attempt1.json)
exited 1 after four of twelve HTTP calls, with four containers and three
captured process generations confirmed absent after cleanup. The first active
create returned 201 and published the expected profile task. The profile worker
claimed it, then failed four verified TLS handshakes. It used a client factory
that ignored the configured provider CA. The same CA had worked for inline
Groups. Commit 2b3fba29 switches profile and account-info fetches to the same
Requests-compatible trust factory, preserving certificate verification. The
[SDK117 checks](../scripts/rust-integration/milestone-batch68/SDK117-terminal.json)
passed all four gates on the corrected snapshot, including 1,240 workspace
tests. The [corrected Docker build](../scripts/rust-integration/milestone-batch68/build-trust-repair-execution.json)
exited 0 in 326.482 seconds with exactly one source file changed from the
first build.

The [corrected Native run](../scripts/rust-integration/milestone-batch68/native-xc11-attempt2.json)
exited 0 and completed all twelve HTTP calls. Four containers and three
captured process generations were confirmed absent. The first active create
recorded three successful provider HTTP calls with zero TLS errors. The
comparison matched request and status projections, response field order,
provider exchanges, task publication lineage, broker state, and media-origin
state for all twelve calls. All five tasks completed successfully with matching
result values, including the first refresh's profile child. Redis journal
review verified the Native refresh result's staged write, rename, expiry, and
publication after the first observer missed the staged write.

First refresh added one stream and left existing channels intact. Deleting the
live-only account removed its stream. Deleting the VOD account removed three
relationships and preserved all three category rows. The literal comparison
still differs on headers, clocks, schedule IDs, cache observations and
WebSocket details. Known WebSocket differences include progress elapsed-time
precision and error-event key order. Runtime capture and task success are
verified. Full byte-level parity remains open.

The [repeated viewing measurements](rust-candidate67-viewing-comparison.md)
apply to Candidate67. Broader milestones and Candidate68 performance
acceptance remain open.
