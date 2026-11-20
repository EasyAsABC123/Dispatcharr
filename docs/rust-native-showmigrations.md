# Native migration inspection

Original Dispatcharr and Django authors retain full credit.

The fully migrated copy ran the shipped Rust operator from application `c6061adbe06bec08f3fc3e8a6de819a585f633d4`. The list showed 32 applied and 0 pending. The plan showed 184 applied and 0 pending. Both commands exited 0 and matched Original stdout byte for byte. The expected notification and schedule writes occurred, with the migration ledger and default database unchanged during inspection. The isolated fixture retired cleanly.

[Redacted all-applied receipt](../scripts/rust-operator-contracts/showmigrations/verified-native-all-applied-1.json)

The independently stock-prepared pending copy ran the shipped Rust operator from application `c6061adbe06bec08f3fc3e8a6de819a585f633d4`. The list showed 31 applied and 1 pending. The plan showed 183 applied and 1 pending. Both commands exited 0 and matched Original stdout byte for byte. The expected notification and schedule writes occurred, with the migration ledger and default database unchanged during inspection. The isolated fixture retired cleanly.

The fixture used the stock Original migration planner and executor to make only `core.0032_limit_default_interleave_delta` pending before testing the Rust commands.

[Redacted pending receipt](../scripts/rust-operator-contracts/showmigrations/verified-native-pending-1.json)

This evidence covers the two operator commands and recorded PostgreSQL profiles. Broader operator parity, migrations and performance remain open.
