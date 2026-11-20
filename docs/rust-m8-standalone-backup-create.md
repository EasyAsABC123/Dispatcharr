# Explicit standalone manual PostgreSQL backup

The original Dispatcharr, Celery and PostgreSQL authors retain full credit.

Ordinary Rust Main can run manual PostgreSQL backup create and task status
when `RUST_BACKUP_CREATE_OWNER=rust` is configured with explicit
`CELERY_BROKER_URL` and `CELERY_RESULT_BACKEND`. Assign that namespace to
a single manual worker, separate from other producers and schedulers.
This configuration starts no scheduler or pidbox control receiver. The
default keeps its existing backup behavior.

The exact `a785a2b7` package and pinned Original ran in separate owned
Compose projects. Authenticated create returned HTTP 202. Both signed
task status responses matched an actual Redis SUCCESS result, and both
authenticated downloads returned HTTP 200. Each archive matched the
format-2 ZIP member and metadata contract, began its database member with
the PostgreSQL custom-dump signature, and matched the worker's recorded
length. The representative seeded SQL snapshot and independent control
stayed unchanged in both projects.

The Original Celery worker and Rust Main retired normally after completion.
The controller exited 0 and independently verified retirement of both
Original Compose generations and all four Native generations. The projects
had no remaining containers, networks or volumes.

Two earlier runs of the same image completed the archive operations and
exited 1 when Native Main stopped. The clean run used Django's management
shell for its SQL inspection so application startup hooks were skipped.
The Native broker's backup queue was empty before and after the snapshot.
The earlier queue contents were unobserved.

The 18 focused host tests included configuration with the option absent
and with a competing worker. They verified pre-claim capacity retry,
create-only transport authority and preservation of worker failure during
shutdown retries. Strict all-target Clippy exited 0. The paired Compose
run used the option enabled and verified a normal stop after completion.
A shutdown deadline with work still active, restore, scheduled jobs,
cancellation, full M8 and performance remain open.

A separate Compose run on the same image omitted the owner flag from the
actual Rust Main environment. Authenticated archive listing and schedule
reads returned HTTP 200, with an empty listing and a disabled schedule.
Create and authenticated task status returned HTTP 501. Anonymous task
status returned HTTP 401. The broker queue and result database were empty
before and after these calls, and the representative SQL snapshot and
independent control matched. Rust Main exited 0 and all four owned Compose
generations retired.

[Redacted default-off receipt](../scripts/rust-integration/milestone-batch113/standalone-backup-default-off-a785.json)

[Redacted paired receipt](../scripts/rust-integration/milestone-batch113/standalone-backup-create-a785.json)

## Restore-owner successor regressions

The exact `976f2867` package passed the same create-only workflow and a
separate default-off Compose run. The Restore owner flag was absent in both
Native configurations. The create-only run matched Original HTTP 202/200/200,
actual Redis SUCCESS, format-2 archive structure and normal process exit.
The default-off run returned HTTP 200 for listing and schedule reads, 501
for create and authenticated task status, and 401 for anonymous task status.
Its broker queue and result database were empty, and Rust Main exited 0.
All three owned Compose projects retired their exact resources.

[Redacted successor regression receipt](../scripts/rust-integration/milestone-batch113/976f-backup-mode-regressions.json)

## Rows inside the downloaded archives

A fresh pair on `a785a2b7` verified the actual downloaded archives in
disposable PostgreSQL 17 databases. Each archive contained the seeded
administrator and password-hash digest, channel ID and UUID with its logo
reference, the linked logo, and the settings object. Each scratch database
was removed and its independent control marker stayed unchanged.

The Native backup queue was empty before and after the archive inspection
and the final management-shell SQL snapshot. Both application roles
stopped normally. The controller exited 0 with verified retirement of
all six owned Compose generations and empty inventories.

The checked archive scope covers the four seeded records. Application
restore, scheduling, full M8 acceptance and comparative performance
remain open.

[Fresh archive row receipt](../scripts/rust-integration/milestone-batch113/standalone-backup-archive-rows-a785.json)

## Manual archive clock after live settings changes

A fresh pair on the same `a785a2b7` image saved UTC, created and downloaded
an archive, then saved Pacific/Honolulu and created and downloaded a second
archive. Original and Rust both accepted each authenticated System Settings
save and returned the exact updated values on readback. Both preserved the
other settings. All four creates returned HTTP 202. The signed status and
download requests returned HTTP 200, and the status matched the worker's
Redis SUCCESS result in every case.

For each download, the filename decoded in the saved timezone and the UTC
timestamp in the archive metadata fell within the independently observed
create and download interval. The two creates per backend had distinct
task IDs and filenames. The representative PostgreSQL snapshot and
independent control stayed unchanged.

Original's separate startup observation found two queued plugin startup
jobs before its worker started. Its queue was sampled empty before the
clock workflow. Both backends had empty queue samples around each save and
create. Rust Main used the dedicated Manual worker and its queue was also
sampled empty around the final management-shell snapshot.

Both roles exited normally. The controller exited 0 and verified the
retirement of two Original generations and four Native generations. The
evidence covers these two timezone policies for manual create. Scheduling,
application restore, full M8 acceptance, and comparative performance
remain open.

[Redacted clock-policy receipt](../scripts/rust-integration/milestone-batch113/standalone-backup-clock-a785.json)

## Normal shutdown during an active manual backup

A fresh run on the same `a785a2b7` package issued an authenticated manual
create to Rust Main. It returned HTTP 202 and a signed task reference. An
observer in the owned container sent one ordinary SIGTERM to Main. The
same packaged PostgreSQL dump process generation was active immediately
before and after the signal.

Rust Main exited 0 without an OOM kill. Redis retained the typed SUCCESS
result for the issued task. The 39,221-byte archive read from the stopped
container matched the recorded size and the PostgreSQL format-2 ZIP
contract. The representative SQL records and independent control stayed
unchanged. The owned broker queue was sampled empty around the final SQL
snapshot.

Stock Original ran its regular authenticated create, signed status, and
download in a separate Compose project, producing a 39,402-byte
PostgreSQL format-2 archive. Both phases stopped normally. The controller
exited 0 and verified retirement of all six owned Compose generations
with empty inventories.

This run verifies one normal shutdown during accepted backup work.
Shutdown deadline expiration, scheduling, restore, cancellation,
full M8 acceptance, and comparative performance remain open.

[Redacted active-backup shutdown receipt](../scripts/rust-integration/milestone-batch113/standalone-backup-active-shutdown-a785.json)

## Selected-mode restore of an Original archive

A fresh run on `a785a2b7` verified a current-schema PostgreSQL restore
with the Selected worker enabled, its scheduler disabled, and the
standalone backup owner flag absent. Two sequential internal Compose
projects each had their own PostgreSQL 17 instance and control database.

Original created a 39,423-byte format-2 archive through its authenticated
API and worker. The Native target had different seeded data. Rust Main
accepted the archive upload with HTTP 201 and returned the exact Original
bytes on download. The upload left the target records unchanged. An
authenticated restore then returned HTTP 202. The signed status
matched the restore worker's Redis SUCCESS result.

After restore, the target matched Original's representative user ID and
password hash, channel ID and UUID, logo relationship, and settings. Its
separate database control stayed unchanged. The same Rust Main accepted
the restored password, rejected the replaced password with HTTP 401,
and returned the restored settings and channel. The SQL observer ran
through the management shell with application startup hooks skipped.

Both roles stopped normally. The controller exited 0 with all six owned
Compose generations retired and empty inventories. This qualification
covers the selected current-schema route for the seeded fixture.
Standalone restore was unsupported on a785. Enabled-plugin and older-schema
restore, media files, full M8 and comparative performance remain open.

[Redacted Selected restore receipt](../scripts/rust-integration/milestone-batch113/selected-backup-restore-a785.json)

## Explicit standalone restore of an Original archive

The `976f2867` package restored an Original current-schema PostgreSQL 17
backup in a fresh internal Compose project. Rust Main ran with
`RUST_BACKUP_RESTORE_OWNER=rust`, the standalone create owner absent,
Selected jobs and Beat disabled, and dedicated Redis broker and result
namespaces. Authenticated create returned HTTP 501 and left both
namespaces empty. The backup schedule read returned a disabled schedule.

The pinned Original API and worker produced a 39,434-byte format-2 ZIP.
Rust Main accepted the upload with HTTP 201 and returned byte-identical
archive data on download. Upload preserved the target records. The
restore request returned HTTP 202, and its signed status matched the
typed Redis SUCCESS result for that exact task ID.

Restored PostgreSQL observations matched Original's representative user
ID and password hash, channel ID and UUID, logo reference, and settings.
The separate control database stayed unchanged. The same Main accepted
the restored password, rejected the replaced password with HTTP 401 and
served the restored settings and channel.

The restore and readbacks finished before shutdown. Both roles exited 0.
The controller verified retirement of all six owned Compose generations
and empty inventories. This check covers the explicit current-schema
scope. Crash atomicity, enabled plugins,
older schema graphs, media files, full M8 and comparative performance
remain open.

[Redacted standalone restore receipt](../scripts/rust-integration/milestone-batch113/standalone-restore-only-976f.json)

## Selected-mode regression on 976f

A fresh copied PostgreSQL 17 Restore ran against packaged 976f2867 with
selected Rust Jobs enabled, selected Beat disabled, and both standalone
backup owner settings absent. Original created a 39,397-byte current-schema
archive. Rust returned the same uploaded bytes on download, preserved the
seeded target data during upload, and then returned HTTP 202 for restore.
Signed status agreed with the actual Redis SUCCESS result. The restored
representative rows matched Original, the separate control database stayed
unchanged, and the same Main served the restored login, settings and channel.

The controller exited 0 and retired all six owned Compose generations.
The a785 receipt remains tied to its earlier package. Standalone create and
standalone restore have their own owner settings and qualification.

[Redacted 976f selected-mode receipt](../scripts/rust-integration/m8-selected-backup-restore-976f.json)

## Normal shutdown during a claimed standalone restore

A fresh `976f2867` run uploaded Original's 39,411-byte PostgreSQL archive to
its disposable restore target and downloaded the exact bytes. Upload
preserved the target records. The authenticated restore returned HTTP 202
with a signed task reference. The same packaged `pg_restore` writer
generation was active immediately before and after one ordinary SIGTERM
to the owned Rust Main.

Main exited 0 without an OOM kill. Redis still held exactly one typed
SUCCESS result for the issued task, and the owned queue was empty.
Post-shutdown SQL matched Original's representative user, channel, logo
and settings records. The independent database control stayed unchanged.
The controller exited 0 and verified retirement of two Original and
four Native Compose generations with empty inventories.

The first attempt exited 1 before issuing restore. Its watcher expected
`/tmp` while the pinned Compose configuration used `/data`. Archive
handoff, unchanged target SQL and retirement passed in that attempt.
The corrected run verified `/data` and repeated both phases on the same
package with the same overlap and result requirements.

The evidence covers one normal active restore. Shutdown deadline
expiration, forced termination, crash atomicity, full M8, and comparative
performance remain open.

[Redacted active standalone restore receipt](../scripts/rust-integration/milestone-batch113/standalone-restore-active-shutdown-976f.json)
