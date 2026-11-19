# Viewer delivery and focused client compatibility

The original Dispatcharr authors and contributors retain full application,
workflow and algorithm credit. The selected workflows continue to use the
original React application.

Candidate100 changes fMP4 delivery so a slow viewer can resume from a
retained fragment after the idle interval. It also ports the ordinary VOD
stats route, Python-compatible HDHomeRun stored output-profile IDs, disabled
recurring-rule detail operations, and canonical JSON channel bulk rename.
The recurring implementation fences enabled-rule rebuilds. The rename uses
the pinned regex dependency and a bounded request and row budget.

The exact candidate revision is `6a015a0bc753b3688e099e57ec5b9cf2b64936ad`.
Formatting, strict all-target Clippy, 551 Store tests, 41 Main tests and three
HDHomeRun/fMP4 tests passed. The corresponding crates reported 79, one and
eight external-fixture tests ignored, respectively. The exact local Docker
image built successfully and retained the hashes of all 497 React, license
and attribution paths compared with Candidate94.

The paired Compose run matched 15 recurring-DVR HTTP observations on each
backend. It matched deletion of the intended future recording, task and
clock, preserved unrelated and past recordings, and verified Beat change
markers for PATCH, PUT and DELETE. Enabled-rule rebuilds remain open.

The VOD stats route matched the Original admin, viewer and anonymous
responses (200, 403 and 401). Existing explicit-profile playback still
returned the same 64 KiB Range response. The same capture retained two
Candidate100 XC route differences for the follow-on candidate.

The Native two-replica TS controls matched the seven retained Original
observations. The selected remote viewer reached EOF, and its peer received
511,736 additional media bytes through the same upstream connection and
provider slot. This verifies the selected controls and shared-viewer
behavior. Complete status fields and the wider M3 matrix remain open.

All three runs verified the retirement of their owned Compose resources.
Initial fMP4 and bulk-rename probes exited 1 in the test harness. The two
Candidate94 password diagnostic probes also exited 1 before the scheduled
login. Their teardown is recorded and their behavior checks remain open.
These scoped functional runs establish no new performance gains.

[Compose evidence](../scripts/rust-integration/milestone-batch100/compose-first.json)

[Original and Candidate98 VOD evidence](../scripts/rust-integration/exact-vod-original-c98/receipt.json)
· [Milestone ledger](rust-milestones.md)
