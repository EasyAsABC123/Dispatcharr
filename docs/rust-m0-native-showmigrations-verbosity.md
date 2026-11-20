# Native migration inventory with applied timestamps

The original Dispatcharr and Django authors retain full credit.

The packaged Rust operator at exact revision `0c429b6418ded230e99d2e19658a139064dfca51` ran the same two verbosity-2 inventory commands as pinned Original. Both commands exited 0. The full inventory matched 184 applied timestamps and the Accounts list matched eight. Complete stdout matched Original ordering and format using the times from the Native fixture's own PostgreSQL ledger.

Both invocations preserved the copied ledger and the ordinary default database. The notification and backup schedule startup SQL matched the Original effects. The two operator containers retired, and the fixture verified both Compose service containers and host generations absent and empty container, network and volume inventories. Full M0 and performance measurement remain open.

[Redacted receipt](../scripts/rust-operator-contracts/showmigrations/verified-0c429-native-verbosity-2-list.json)
