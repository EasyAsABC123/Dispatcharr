# Standard provider deactivation on default Rust Main

Pinned Original and packaged Rust `7ccbb7a20e9a` each returned HTTP 200 on an
administrator's initial read, `{"is_active":false}` PATCH and readback.
The typed reply changed the active flag and status to disabled for the
owned standard provider. The linked four-hour task became disabled,
kept its ID and count, cleared its last-run time and advanced the task
and Beat update timestamps.

The two related profile expiration notices and their two prior user
dismissal rows were deleted. The real Channels updates group delivered
one matching dismissal event for each deleted key. The comparison
used the eventual payloads. Profile, stream, channel, link and control
notification rows were preserved, and readback added no SQL changes.

Original ran stock Celery prefork with Beat stopped. Rust Main ran
with Selected jobs, Selected Beat, EPG refresh and Version tasks off.
Both arms recorded zero new queue admissions or stored task results.
Rust Main exited 0, and both arms retired their owned containers and
observed process generations.

This scoped pair leaves concurrent edits, live playback, running Beat,
full M6 and performance open.
[Redacted deactivation receipt](../scripts/rust-job-contracts/verified-ordinary-provider-deactivation.json)

Original Dispatcharr, Celery, Django Celery Beat and Channels contributors retain application and protocol credit.
[Dispatcharr upstream](https://github.com/Dispatcharr/Dispatcharr)
