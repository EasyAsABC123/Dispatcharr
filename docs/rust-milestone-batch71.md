# Rust parity checkpoint

The original Dispatcharr authors and dependency contributors retain full
credit. React, the original license and attribution remain part of the build.

| Milestone | Change and focused evidence |
| --- | --- |
| M4 · Timeshift | An established archive session becomes transferable after its worker releases the provider lease. One Redis operation checks the descriptor and rotates the fenced pool, client and metadata tokens. A late cleanup from the old owner leaves the new owner intact. Twelve real Redis pool tests and two cancellation tests passed. The packaged two-worker playback comparison still needs to run. |
| M5 and M6 · XC accounts | A group-only PATCH can change membership on inactive XC accounts and accounts with VOD enabled. The existing account-scoped write changes the selected membership. Six Rust account-validator tests and eight pinned Original controls passed. The paired Main PATCH and GET fixture is ready. |
| M6 · XC refresh | A settled malformed account-info reply counts as one profile failure and the next profile still runs. The pinned Source controls verified partial failure, recovery and fallback user-agent use. Ten focused Rust account tests passed. The real Main task and its Profiles child still need paired verification. |
| M7 · Guide | Custom dummy programmes without a time or date pattern use the Source timezone for the local date while retaining the UTC programme interval. Five pinned Python cases and the Rust controls agree. Localized time and date pattern schedules remain outside this check. |
| M8 · Operations | Repeated explicit `--user` flags use the final value. The targeted CLI parser tests passed. The paired fixtures cover repeated arguments and signed downloads of three encoded filenames through the packaged application. |
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

These checks advance the [complete parity plan](rust-complete-parity-plan.md).
Full milestone acceptance and new performance measurements remain open.
