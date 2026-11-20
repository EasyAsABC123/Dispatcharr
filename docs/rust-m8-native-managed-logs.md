# Native managed startup logs

The original Dispatcharr authors and contributors retain full application,
collector and React credit. [Attribution](../ATTRIBUTION.md)

The Native app stays PID 1 under UID 65532. Its managed writer captures
the normal console streams while forwarding their original bytes. A bounded
2 MiB queue and separate file writer keep storage work off the server path.
The writer uses the existing role name and collector settings. Main retains
its existing termination and exit status handling.

The first exact Native package created a fresh managed log containing its
real readiness record. The stock admin list reported an active collector
and the correct file metadata. The tail returned the complete prefix of
the same file through its exact cursor. The console showed the original
readiness message and SIGTERM exited zero. Pinned Original separately
verified the same list and tail behavior on its own startup log. The fixture
compares each backend against its own file and output.
[Original active-log receipt](../scripts/rust-integration/milestone-batch92/stock-original-active-logs.json)

Independent review found two file handling gaps in that first Native
candidate. Its successor checks the configured cap before appending a batch
and stops file writes when the required rotation fails. A confirmed same-role
standby drops its file backlog before later taking ownership. The new Rust
controls verify both cases.

The fresh startup scope, rotation failure invariant and standby invariant are
recorded separately. Persistence toggles, rotation under load,
replica behavior, resource measurements and full M8 remain open.

The corrected shared image repeated the fresh-boot check. All seven Compose
phases exited zero. It verified the managed file, authenticated list and
tail, exact console readiness line and clean SIGTERM, with all four owned
roles retired. The test used the ordinary startup configuration. The
separately staged Version startup option remained disabled.
[Native verification receipt](../scripts/rust-integration/milestone-batch113/native-managed-logs.json)

## Stored logs on a read-only mount

The exact corrected `c74ea312bdc7` package passed all 29 System Logs cases
against pinned Original. Both backends mounted the same pre-populated log
directory read-only. The eight listing, 13 cursor tail and eight download
cases matched in status, response body and headers, including allowed
and denied roles, two Unicode attachment encodings and the 24 MiB tail
view cap. Each of the 58 requests left its fixture snapshot unchanged.
The listing returned the expected inactive collector state on the read-only
mount. The 18 foundation checks also passed.

The controller exited 0 with both EOFs and its process group settled.
Teardown independently verified the absence of all four owned containers
and their original host processes. Project container, network and volume
inventories were empty. Active rotation, persistence toggles and full M8
acceptance retain their separate gates.
[Read-only System Logs receipt](../scripts/rust-integration/milestone-batch113/read-only-system-logs.json)

## Active collector retention setting

Pinned Original and the exact `c74ea312bdc7` package both applied the stock
System Settings PUT while the collector was running. An anonymous PUT
returned 401 and left the setting and all three test archives unchanged.
An administrator then changed retention from five files to two while
preserving the other settings. Both backends wrote the expected collector
configuration and pruned archives two and three. Archive one retained its
inode and byte hash. The live file retained its inode, and its admin list
and cursor tail matched its actual contents after the change.

All five Original and seven Native phases exited 0 with both EOFs and
settled groups. Teardown verified one Original and four Native owned
containers and their original processes were absent. The earlier
Original51 APIClient capture established config-write behavior. This pair
also observed the running collectors apply the change without restarting.
Size-triggered rotation and full M8 remain open.
[Active retention receipt](../scripts/rust-integration/milestone-batch113/active-log-retention.json)

## File persistence disabled across restart

Pinned Original and the exact `c74ea312bdc7` package handled the stock
System Settings PUT changing file persistence to false. After restarting,
the admin GET returned the saved setting and System Logs still reported
an active collector. Each process emitted its ordinary startup record to
the console. Neither target record appeared in the live file. The earlier
enabled-period log prefix remained in the first archive, and the live
file was stable across six writer intervals. Original retained 1,328 bytes
from earlier in the disabled boot. The Native retry observed zero.

The Original controller exited 0 with all ten phases settled. The first
Native controller exited 1 when Compose restarted the already completed
initializer, which exited 1. All four owned resources retired. The fresh
Native retry started only the verified app container, passed all twelve
phases, and verified exact teardown of all four owned containers and host
processes. Full M8 remains open.
[Disabled file persistence receipt](../scripts/rust-integration/milestone-batch113/disabled-log-persistence.json)

## File persistence re-enabled across restart

Pinned Original and C74 applied the disabled settings to a running
collector, witnessed by archival pruning. The administrator then changed
only file persistence back to true using the stock System Settings PUT.
After one ordinary restart, both returned the enabled setting and retained
the previous live prefix in archive one. The normal new boot record reached
the console and the managed file, and the authenticated tail matched the
exact byte prefix at its cursor on the new live inode.

All 13 tracked Original physical phases exited 0 with settled output and
exact cleanup. Its controller exited 1 when the terminal JSON serializer
received a regular expression match from a passing check. Native had yet
to run. The Native-only retry used the independently verified Original
evidence, exited 0 in all 12 phases, and confirmed absence of all four
owned containers and observed process generations. The scoped file and
console effects matched. Size-triggered rotation, live replica behavior,
performance and full M8 remain open.
[Re-enabled file persistence receipt](../scripts/rust-integration/milestone-batch113/reenabled-log-persistence.json)

## Size-triggered rotation

Original appends the batch, then rotates once the live file exceeds the
configured cap. The batch that crosses the cap therefore moves into
archive one. C74 rotates before an append that would exceed the cap, so
the triggering batch goes into the new live file. The two collectors place
the boundary batch in different files.
[Original collector](../dispatcharr/log_collector.py) · [Native writer](../rust/crates/app/src/managed_logs.rs)

Both implementations archive a prior nonempty live log during startup
independently of the size cap. The persistence restart checks cover that
boot behavior.
[Original entrypoint](../docker/entrypoint.sh) · [Native file acquisition](../rust/crates/app/src/managed_logs.rs)

Pinned Original and Rust package `735a0299` passed a fresh paired
size-triggered check. Each running collector owned a live file seeded
to exactly 1 MiB before rotation, with no first archive. On Original,
ordinary authenticated tail and download requests for an absent archive
both returned HTTP 404 and produced distinct Django warning records.
The authenticated log list returned HTTP 200 and showed exactly the live
file and archive one. On Rust, the normal schema-skip startup record
crossed the cap, and the ordinary readiness record followed after the
fixture SQL wait released.

The first Original-only attempt returned HTTP 200 for the version
request and failed the crossing-file check. Stock non-debug Original
disables successful uWSGI request records. The controller exited 1,
verified exact cleanup, and left Native unstarted. The fresh retry used
normal Django warnings with the same stock Original configuration.

Both collectors moved the seeded inode and preserved its prefix. Each
wrote the crossing record beyond the cap in archive one and placed the
following record in the replacement live file. Both controllers exited 0.
Teardown verified the disappearance of the Original container, all
four Native containers and their observed process generations. Neither
project retained a container, network or volume.

Rust moves the live inode before writing the crossing batch to that
inode. A concurrent reader can briefly observe the replacement file
before the archived append completes. This check establishes final
file placement at the configured boundary. Concurrent reader timing,
failed I/O, broader retention and performance retain separate checks.

[Paired size-rotation receipt](../scripts/rust-integration/milestone-batch113/managed-log-size-rotation-735.json)
