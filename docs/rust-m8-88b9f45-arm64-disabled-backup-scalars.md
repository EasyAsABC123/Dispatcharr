# ARM64 disabled backup schedule settings roundtrip

Two fresh, isolated Lappy Compose projects used stock Original Daphne and Rust Main from ordinary ARM64 application `88b9f45`. Both had owned Redis configured and ran without a scheduler or worker. Each began with the same saved disabled schedule and used an owned administrator.

Both backends returned the same five typed results for the initial GET, ordinary PUT, edited GET, restore PUT, and final GET. The edits changed the weekly time from 03:15 to 14:10, the day from 2 to 4, and retention from 3 to 5. The restore returned all three values to their initial settings. The saved row's disabled status and unknown typed property were preserved, along with the scoped independent setting, migrations, and Beat rows. The exact cleanup and both literal exit codes passed.

The backup directory and scoped broker and task-result stores remained empty. The application received a disabled schedule in both updates. This result covers the settings roundtrip under the observed roles. Enabling a schedule with the ordinary Native owner absent remains a separate gap. The [machine-readable result](../scripts/rust-integration/m8-88b9f45-arm64-disabled-backup-scalars.json) binds the exact ARM64 image and both receipt hashes.
