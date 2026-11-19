# Rust parity checkpoint

The original Dispatcharr authors and dependency contributors retain full
credit. React, the original license and attribution remain part of the build.

| Milestone | Change and focused evidence |
| --- | --- |
| M4 · Timeshift | An established archive session becomes transferable after its worker releases the provider lease. One Redis operation checks the descriptor and rotates the fenced pool, client and metadata tokens. A late cleanup from the old owner leaves the new owner intact. Twelve real Redis pool tests and two cancellation tests passed. The explicit-session two-worker run matched the Source media and provider reservations. |
| M5 and M6 · XC accounts | A group-only PATCH can change membership on inactive XC accounts and accounts with VOD enabled. The existing account-scoped write changes the selected membership. Six Rust account-validator tests and eight pinned Original controls passed. The paired Main PATCH and GET fixture is ready. |
| M6 · XC refresh | A settled malformed account-info reply counts as one profile failure and the next profile still runs. The pinned Source controls verified partial failure, recovery and fallback user-agent use. Ten focused Rust account tests passed. The real Main task and its Profiles child still need paired verification. |
| M7 · Guide | Custom dummy programmes without a time or date pattern use the Source timezone for the local date while retaining the UTC programme interval. Five pinned Python cases and the Rust controls agree. Localized time and date pattern schedules remain outside this check. |
| M8 · Operations | Repeated explicit `--user` flags use the final value. The targeted CLI parser tests passed. The Source CLI cases passed on a copied database. Packaged Candidate70 matched all 12 signed-filename assertions, including downloads using three tokens issued by Rust. |
| M8 and M9 · SQLite | The retained SQLite database stops at Channels0037. All six revisions of Channels0038 examined have PostgreSQL-specific SQL. Isolated SQLite checks also found two incompatible VOD0003 statements. The complete SQLite Source fixture and datastore acceptance remain open. [Datastore evidence](rust-sqlite-compatibility.md) |

[Focused checks](../scripts/rust-integration/milestone-batch71/focused.json)
record the checked Rust paths, test counts and Source timezone observations.

## Packaged fixed-seed EPG on Candidate70

With `PYTHONHASHSEED=42`, all 88 HTTP cases matched, including 16 cases
with generated IDs. All 750 helper comparisons matched, including 130 with
generated IDs. The request admission and response-cap checks returned the
expected 503 responses. The two Rust containers shut down with exit 0 and
without an out-of-memory kill.

Two earlier attempts ended before application requests because the tools
snapshot called Git archive from below the repository root and extracted an
empty tree. The successful run archived from the repository root and verified
that the archive contained the Source project file before starting Compose.
Independent Docker inventories found no containers, volumes or networks left
for any of the three projects.
[Packaged evidence](../scripts/rust-integration/milestone-batch71/candidate70-fixed-seed-epg.json)

## Backup and operator follow-up

Candidate70 matched all twelve signed-filename responses, including
valid, missing and invalid tokens for UTF-8, punctuation and literal percent
filenames. The Rust package issued each token used by the successful test
downloads.

The original CLI reference used three cases on a copied database. It exited
0 for final-target selection, 0 when the final value was empty, and 2 for a
malformed final option. The first fixture attempt expected the startup
notification sequence to stay constant. Source created the developer notice
during startup. The corrected fixture checked the inserted row and its exact
sequence, then completed all three cases. Packaged Candidate71 matched all three cases, including the captured SQL,
Redis and ordered Channels effects. Independent inventories verified cleanup.
[Runtime receipts](../scripts/rust-integration/milestone-batch71/operator-source-backup-candidate70.json)

## Candidate71 packaged archive and operator checks

The exact Candidate71 image built with the same React, license and attribution
inputs as Candidate70. In the explicit-session archive case, two separate
Rust Main listeners each returned HTTP 200 with 8,192 bytes identical to the
pinned Python capture. Both requests used the same stored primary profile.
The provider count changed from one to zero after the first body closed,
returned to one on the second worker and ended at zero. The origin reported
two connections, with up to two handlers overlapping, and no active handler
after retirement. The independent cleanup check confirmed the owned
containers, network and volumes were absent.

[Build receipt](../scripts/rust-integration/milestone-batch71/candidate-build.json)
· [Archive result](../scripts/rust-integration/milestone-batch71/native-cross-worker-idle.json)
· [Packaged operator result](../scripts/rust-integration/milestone-batch71/operator-native.json)

## M0 retained Core coverage

Twenty-nine retained Original HTTP observations now map to seven Core GET
operations. The register has 188 partial Source assignments and 610
unassigned HTTP references. Two regenerations matched byte for byte. The
portable suite passed 364 checks, with one skipped, and the scoped artifact
custody check passed separately.
[Register verification](../scripts/rust-integration/milestone-batch71/m0-offline-controls.json)

These checks advance the [complete parity plan](rust-complete-parity-plan.md).
Full milestone acceptance and new performance measurements remain open.
