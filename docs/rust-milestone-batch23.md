# Profile demand keeps a remote follower alive

Original Dispatcharr and dependency authors retain full application, media,
algorithm and licensing credit. React remains unchanged.

[Candidate22](rust-milestone-batch22.md) fails its real two-replica profile test:
the remote stream ends before its first profile frame. When temporary raw
subscription setup ends, the follower has no ordinary raw viewers. Its idle
cleanup then stops local clients, including the newly admitted profile client.

This correction retains bounded internal profile demand for the existing reader
and delivered frames. It reuses the reader permit and adds no ordinary client,
user slot, provider reservation, media queue or extra process. Idle retirement
checks ordinary raw viewers and internal profile demand. Final release uses the
existing fenced cleanup. Diagnostics and regression assertions require exactly
three actual clients for one ordinary viewer plus two profile viewers, one
provider/upstream/process, held-frame lifetime and selective follower retirement.

All four SDK gates pass: formatting, compilation, strict Clippy and1,034 tests,
zero failures,764 ignored across176 result blocks. The
[evidence](../scripts/rust-integration/milestone-batch23/inventory.json) binds
1,302 inputs and three corrected source paths. The [actual Redis control batch](../scripts/rust-integration/milestone-batch23/physical-output/report.json)
passes all eight tests: six output-store controls, the two-replica/held-frame
profile lifetime case, and the forced-stop race. The SDK16 host controllers run
in the unchanged Candidate22 dependency environment; they do not execute
Candidate23 Main. Exact3 client records, one upstream/provider/process and
selective retirement pass. All four containers, the owned Compose project and
original Redis generation settle.

Docker build and all [package checks](../scripts/rust-integration/milestone-batch23/package/)
pass for `7e0dd3dd51ae040467d9ea44465843d029e17775`, image
`sha256:867acb382e4251c2e13ecd9b6fc06ad7b408d02e62e34c9fd95f74fa75ee4996`.
All1,302 selected inputs and the1,793-file archive match committed Git.
The [actual Source-first Main batch](../scripts/rust-integration/milestone-batch23/functional/report.json)
exits1 after1,141.329 seconds. Native passes all six normal output-profile cells.
Source passes three compact-numbering cells and six HTTP/model callback cells,
including held-lock and parent-transaction rollback behavior. Its auto-sync batch
passes four of five cells; the failed never-policy assertion incorrectly expects
removed provider channels to acquire the new names. The original failed row is
preserved and the next driver corrects that expectation. Source regex passes one
of five cells; four assertions remain unresolved. Native's auto parent reaches
SUCCESS, but its three TVG descendants do not finish; compact/API/regex remain
blocked after readiness faults. Both VLC workflows fail; the retained private
Source media log identifies the fixture's closed HTTP proxy as the blocker.
The next driver supplies a bounded owned forward proxy for that case.

All seven actual containers, both owned Compose projects, original process
generations and controller groups settle. Whole selected Main acceptance and
performance acceptance remain open.
No full milestone or application performance gain is accepted.
