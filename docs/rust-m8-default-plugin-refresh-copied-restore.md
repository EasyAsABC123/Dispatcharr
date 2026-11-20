# Startup plugin refresh and copied restore

The non-diagnostic Rust package at `4f9e3916fddfaf35a2f84c075cf11cd5eda2d3fe`
passed the copied PostgreSQL restore test with startup plugin refresh
enabled, Selected jobs active, and Beat off. The exact build manifest
has SHA-256 `da9b42e61ed4dab91510f30df638f4ace610ebd3623ba5f8462caa35379c22cd`.

On the isolated repository fixture, the Native resolver witness returned
a temporary resolution error and Original reported `gaierror`. Two
stock startup tasks stored `ValueError` on the repository and completed
with `SUCCESS` and a null result. The packaged Rust startup task
matched that status and result. The fetch timestamps advanced, cached
manifest and other rows stayed intact, and the queue and acknowledgments
drained.

The opted-in trusted taskless restore returned HTTP 202. Its signed
completion and stored result agreed. One guarded PostgreSQL helper ran
and retired. The same Rust Main and plugin host served the restored
data. Main exited 0 and both Compose projects cleaned up every owned role.

The earlier diagnostic images retain exit 1. Their fixed traces observed
the platform DNS branch and a retained task owner. The isolated
resolver witness identifies the temporary category for the new fixture.
This result covers this copied restore and shutdown workflow. Full M8
and performance remain open.
[Earlier diagnostic runs](rust-m8-default-plugin-refresh-shutdown-diagnostic.md)
[Redacted receipt](../scripts/rust-integration/m8-default-plugin-refresh-copied-restore-4f9e3916.json)

The original Dispatcharr authors and all dependency contributors retain full credit.
[Attribution](../ATTRIBUTION.md)
