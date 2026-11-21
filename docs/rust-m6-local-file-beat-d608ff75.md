# Scheduled refresh of a local M3U account

Pinned Original and packaged Rust `d608ff758c4a937961439f4389d01040f560122d` each ran a normal
one-hour schedule for an owned standard M3U account. The account save
signal enabled its stored schedule, and Beat published one task to
the ordinary worker. The task had an account ID, no parent, and the
account's periodic task name. Native used the explicitly enabled
Selected worker and Beat configuration.

Both roles returned `SUCCESS` with `Dispatched jobs complete.` Each
imported one stream from the identical plain local file, preserved the
playlist bytes, and created two enabled group memberships. One group
came from the playlist, and the application added Default Group. The
durable schedule count advanced once. The account status and refresh
event reflected the imported stream, and the queue and account lock
settled. Both workers and Original Beat exited with literal 0. The
fixture verified retirement of all six Original and five Native roles.

The first independent Original attempt failed before worker startup
because the disposable read-only source mount lacked a media mount point.
The retry passed physically, and its first reducer expected one new
group membership. The qualified receipt checks both actual groups and
preserves both earlier exit values and the teardown evidence.

[Redacted paired receipt](../scripts/rust-integration/m6-local-file-beat-d608ff75.json). M6, default worker deployment,
and performance qualification remain open.

The original Dispatcharr authors and contributors retain full credit.
