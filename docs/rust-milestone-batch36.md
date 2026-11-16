# Rust milestone batch 36: bounded DVR capability admission

SDK47 passes all four toolchain gates and installs nine exact frozen Rust paths. The change permits unrelated Main work to start with a populated plugin directory while refusing unsupported DVR work before queue claim or producer effects. This is a limited stable, owned namespace capability boundary; Candidate36 Docker and application acceptance remain pending.

Original Dispatcharr, plugin and dependency authors retain full application, library and algorithm credit. This migration claims none of their original work.

## Implementation and limits

The DVR service retains a directory descriptor and checks configured canonical target, device/inode identity and emptiness around enumeration. Each actual admission checks again. This does not provide atomic protection against external rename or mutation during an operation. The exposed capability string records the last observation; it is not a current filesystem lease or a whole-M8 readiness result.

Recording and Core-offset task guards precede schema/SQL adoption and broker claim, including revoked and missing-row recording heads. Recording HTTP create/update and CoreSettings DVR-offset writes check after validation but before save, revoke, cache invalidation or publication; bridge publication methods also check. CoreSettings reuses the existing weak recording-jobs port to avoid a new ownership cycle. Unknown filesystem/port failures retain operational/503 classification rather than declaring known unsupported capability.

Unsupported startup recovery is explicitly deferred before its lock and publication. Two actual startup roots remain expected in this limited domain; no recovery SUCCESS is fabricated. Source three-root startup parity, automatic retry when plugins disappear, arbitrary plugin model/SQL writes and full M8/DVR plugin parity remain open. Existing file guards remain.

## Exact verification

The [input manifest](../scripts/rust-integration/milestone-batch36/inputs.json) binds 1,414 files, including 1,408 Rust-tree files. [Four completed checks](../scripts/rust-integration/milestone-batch36/checks.json) record actual exit 0 for format, workspace check, strict all-target Clippy and workspace tests. The test log contains 1,115 passed, zero failed and 783 ignored across 180 result blocks. These are toolchain results; they do not establish application parity or performance gains.

The [nine-path overlay map](../scripts/rust-integration/milestone-batch36/components/overlay-map.json) has SHA256 `60abdae6a574f807f97a07b5b10da8ad42e9ef1964b4f6061ae0f19c07e2dcce`. Its [independent static review](../scripts/rust-integration/milestone-batch36/components/independent-review.json) has SHA256 `02896e5eefa110dac7002fcfd1efff3a9a35180185c88ed5d4f18c80d9fe30ec`. The [installation receipt](../scripts/rust-integration/milestone-batch36/components/SDK47-installed.json) records canonical equality to the frozen checked source. Earlier private join/review receipts are retained literally; their uncompiled status describes their earlier preparation time.

SDK46 namespace packaging evidence remains in [batch 35](rust-milestone-batch35.md). Native37 reached validated real Host startup and physical nonforced exit 0 with both pipe EOFs, settled callbacks and no descendants. Its whole Guide run still exited 1 on the local audit baseline assertion. SDK47 does not relabel that failed cell or establish M8 startup acceptance.

## Required next acceptance batch

- Build Candidate36 and run its complete installed-package controls against the exact new image.
- Run actual owned Main populated-domain startup with two observed roots, explicit recovery deferral and no recovery lock/root/result; retain Source three-root distinction.
- Exercise real normal, revoked and missing-row recording heads, recovery heads and offset heads. Observe exact queued bytes, no claim/ACK, preserved prior backend values and absence of SQL/media/event/provider/revoke effects through physical shutdown.
- Exercise valid authenticated recording POST/PUT/PATCH and Core DVR-offset writes, proving refusal before SQL, Beat, cache, broker or revoke effects. Retain auth/validation ordering and stop/DELETE cleanup behavior.
- Run empty-directory regressions, directory replacement, plugin addition/removal and configured symlink retarget controls; distinguish operational errors from known unsupported capability.
- Resolve the existing Guide audit-baseline failure, then qualify the complete application milestones and matched CPU/memory/stream workloads. No performance gains are established by this packet.

The [packet index](../scripts/rust-integration/milestone-batch36/index.json) hashes the public evidence. Root owns review, commit and publication.
