# Stock manual channel creation with a dummy guide

The original Dispatcharr authors and contributors retain full application,
React, and workflow credit. The package preserved all 497 frozen React and
attribution inputs.

Both backends accepted stock channel creation with an existing dummy guide.
The paired fixture matched all eight cases per backend, including selected
profile membership and ordered streams, an empty stream list with all
profiles, invalid-guide validation, the missing-profile response after
save, and four role denials. The measured PostgreSQL rows, sequences,
cache and broker state matched in the selected scope. The controller and
both application runs exited 0 with exact process and resource retirement.

The application image also served the paired historical SQLite migration
and rollback rehearsal. Each check uses its own fixtures and receipts.
[Manual channel creation receipt](../scripts/rust-integration/milestone-batch111/manual-dummy-guide.json)
