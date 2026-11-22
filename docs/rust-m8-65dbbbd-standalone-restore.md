# Standalone copied-data restore on 65dbbbd

Original Dispatcharr authors and contributors retain full application credit. See [upstream attribution](../ATTRIBUTION.md).

The Original worker created a genuine 39,378-byte PostgreSQL 17 archive in an owned Compose project. A separate Rust project uploaded and downloaded the archive and verified its SHA-256. Its standalone restore worker completed the signed task with `SUCCESS`. SQL inspection confirmed the expected rows and the unchanged control database. The same Rust Main process served restored login, settings and channel reads. Both projects passed exact cleanup.

This fixture covers standalone restore of a generated archive into copied data. The full operations and release gates remain open. [Machine-readable evidence](../scripts/rust-integration/m8-65dbbbd-standalone-restore.json)
