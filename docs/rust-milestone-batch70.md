# Parallel Rust migration checkpoint

The original Dispatcharr and dependency authors retain full credit. This
checkpoint changes the Rust backend and its test fixtures.

| Milestone | Change | Focused verification |
| --- | --- | --- |
| M3 · HLS and profiles | A typed output profile waits for its shared transform to open before returning a capability. A failed startup returns the Source error. Cross-worker readiness checks the active lease and resets on a fresh owner claim. | The real Redis HLS and output-store regressions passed. The packaged positive transform, same-profile sharing and fault cases remain part of acceptance. |
| M5 and M7 · Management and guide | Current-programme POST accepts ordinary bounded multipart forms with the existing selector rules. XC EPG generated IDs support fixed nonzero Python hash seeds. | Six pinned Django/DRF multipart comparisons and two Rust decoder tests passed. Four fixed hash seeds have checked-in vectors. A separate comparison of the Rust function with fresh local CPython processes matched 40 seeds across 42 strings. Packaged guide and XC requests remain open. |
| M8 · Operations | The native `reset_user_network` operator accepts attached `--user=VALUE` syntax. An explicit empty value retains the all-users behavior. | The pinned Original argument parser accepts the tested values. The Native parser regression exercises attached values and its existing byte limit. The packaged invocation still needs an owned datastore and task-effects check. |
| M8 · Backups | Signed downloads verify the same once-decoded filename that token issuance and file lookup use. | The focused verifier passed for Unicode, punctuation and a literal percent sequence. The paired HTTP fixture now has 12 additional cases for issuance, download and authorization outcomes. |
| M8 and M9 · Datastore | The pinned Python all-in-one entrypoint reached HTTP readiness with PostgreSQL despite receiving `DB_ENGINE=sqlite` at PID 1. The independent direct SQLite CLI run still failed at Channels0038. | The all-in-one run observed six uWSGI process generations and 184 applied migrations in PostgreSQL. The isolated container was removed. [SQLite compatibility evidence](rust-sqlite-compatibility.md) |

The [focused verification receipt](../scripts/rust-integration/milestone-batch70/focused.json) records the exact test scopes.

The existing output-profile fixture now has a focused two-viewer HLS mode.
It checks matching segments, one upstream connection, one FFmpeg transform,
and continued playback after one viewer leaves. Local observer controls and
the scoped packaged run passed.

The work above preserves the release datastore gate. SQLite requires a
legitimate complete Source fixture or a supported scope decision. This batch
has no new performance measurements or milestone closures. The [complete parity
plan](rust-complete-parity-plan.md) tracks the remaining Compose, React, paired
performance, soak, and copied-data rollback work.

## Candidate69 XC follow-up

The Docker-built Candidate69 ran all twelve expected HTTP calls through Main
and its selected task workers. The service exited 0. The independent teardown
check confirmed four containers and three host process generations were
absent. The first launch failed before HTTP because the launcher selected an
existing Candidate68 output directory. The retry passed its binding path to
its own runner explicitly.

The three previously failing controls now pass. A configured user agent avoids
two unrelated cache writes, both error events use the Source field order, and
refresh progress reports elapsed time beyond hundredths. All twelve HTTP
statuses match the retained Original. All five task results match after
verifying the complete staged Rust write and its child publication. The
literal direct-write observer still fails because it expects the Original's
single Redis write.

The whole-fixture comparison retains differences in headers, response bodies,
SQL, cache inventory and event data. Through-Main fallback user-agent and
multi-profile recovery are pending. [XC execution and comparison receipt](../scripts/rust-integration/milestone-batch70/xc-candidate69.json)

The follow-up HTTP review matched all twelve header-value multisets after
case normalization and excluding transport `Connection` and `Date`. Six
bodies matched byte for byte. The other six differed only on the listed
timestamps. The review preserves the whole-fixture comparison and does not
qualify its SQL, cache or event differences.
[HTTP-only review](../scripts/rust-integration/milestone-batch70/xc-candidate69-http-review.json)

## Cross-worker archive Source behavior

Two separate workers returned HTTP 200 and 8,192 usable bytes for the same
idle archive session. The pool busy value and provider reservation each
followed 1, 0, 1, 0 and remained at zero after retirement. The origin reported
up to two overlapping physical handlers and zero after cleanup. That defines
the Source behavior for this case. Rust cross-worker adoption needs a fenced
lease handoff to preserve the stored profile and prevent stale cleanup from
affecting the new owner.
[Source archive receipt](../scripts/rust-integration/milestone-batch70/source-archive-idle.json)

## Candidate70 build

Docker built the published Rust checkpoint from its exact Git revision. The
React, license and attribution inputs matched Candidate69 byte for byte.
[Build receipt](../scripts/rust-integration/milestone-batch70/candidate-build.json)

## Packaged HLS sharing

Two simultaneous viewers through Rust Main received distinct capabilities
for the same channel and output profile. They fetched two identical
consecutive segments while the origin saw one connection and the application
ran one FFmpeg process. After stopping the first viewer, the second fetched
a later segment from the same owner, connection and process. The two
captures each decoded 74 video frames with audio present.

The first attempt exited before playback because the startup observer could
not read its mounted directory. The retry corrected the directory permissions
and used the same image and test files. It exited 0. All five owned
containers across the two attempts were independently confirmed absent.
This covers one same-channel, same-profile workload.
[Compose result](../scripts/rust-integration/milestone-batch70/profiled-hls-two-viewers.json)

## M0 retained guide coverage

Six guide GET operations now reference 101 existing Python framework
observations. Three HEAD observations retain their separate method context.
The register has 181 partial assignments and 617 unassigned references.
Two generations matched byte for byte. The portable suite passed 363 checks,
with one skipped, and the retained artifact custody check passed separately.
[Offline receipt](../scripts/rust-integration/milestone-batch70/m0-offline-controls.json)
