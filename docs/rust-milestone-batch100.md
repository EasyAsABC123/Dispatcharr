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

HDHomeRun matched all 135 paired application checks, including the restored
default-profile integer cases. The lineup-generated stream delivered
1,165,788 media bytes without a TS synchronization error. FFmpeg decoded 74
frames with audio and video, and the packaged process exited cleanly.

The exact fMP4 test peer passed the slow-viewer resume check against an
owned Redis service. It resumed retained fragments after the idle deadline,
while the fast peer continued over Native HTTP. The run verified both
resource identities were retired and test Redis was empty. Its initial
launcher exit 1 occurred before starting the test.

The successful runs verified owned Compose teardown. The initial bulk-rename
probes exited 1 in the test harness. The corrected bulk-rename
probe passed six Original and seven Native checks before it exposed a
behavior difference. The Original returned 200 for the overlength result
where Candidate100 returned 500. The Original SQL readback for that response
remains pending.

The two Candidate94 password diagnostic probes exited 1 before the
scheduled login. Their teardown is recorded and their behavior checks
remain open. These scoped runs establish no new performance gains.

[Compose evidence](../scripts/rust-integration/milestone-batch100/compose-first.json)

[Original and Candidate98 VOD evidence](../scripts/rust-integration/exact-vod-original-c98/receipt.json)
· [Milestone ledger](rust-milestones.md)

## Original PostgreSQL archive restored through Native Main

The Original's authenticated API admitted the backup job with HTTP 202.
Its Celery worker completed successfully, and the download returned HTTP
200 with the format-2 PostgreSQL archive. Native accepted the same archive
and completed the restore. Its restored SQL fingerprint matched the
Original control.

The same Native Main process then confirmed login with the restored
credential, returned HTTP 401 for the replaced credential, and served
the restored settings and channel. Both projects and their owned process
generations retired. This covers the selected PostgreSQL archive workflow.

[Cross-backend restore receipt](../scripts/rust-integration/milestone-batch100/postgres-cross-restore.json)
