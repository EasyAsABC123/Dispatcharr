# Original migration inventory with applied timestamps

The original Dispatcharr and Django authors retain full credit.

Pinned Original Dispatcharr ran its Django 6.0.9 migration inventory with `--verbosity=2` for all applications and with `--list --verbosity 2` for Accounts. Both commands exited 0. Complete stdout matched the migration graph and the applied timestamps on all 184 global rows and all eight Accounts rows. Each timestamp reflected the copied PostgreSQL recorder to whole UTC seconds.

Both invocations preserved the migration ledger and produced the previously verified notification and backup schedule startup writes. The fixture verified the default database unchanged and retired its exact Compose containers and host generations. Native verbose list and verbose plan are outside this Source result.

[Redacted receipt](../scripts/rust-operator-contracts/showmigrations/verified-original-verbosity-2-list.json)
