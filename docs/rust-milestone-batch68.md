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

The repeated viewing measurements apply to Candidate67. Candidate68 runtime
parity and broader milestones remain open. The Native retry is running against
the corrected image.
