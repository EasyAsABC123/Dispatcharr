# Packaged migration plan with dependencies on ca752176

The exact Rust operator ran `showmigrations --plan --verbosity=2` and the
ordinary plain plan on disposable copied PostgreSQL. Each command exited
0 and matched complete pinned Original stdout. The verbose plan retained
all 184 rows and 203 direct dependencies in Original order.

The pair verified the normal startup SQL, unchanged migration ledger and
default database, and retirement of both operator containers and the owned
Compose project. Original Dispatcharr and Django contributors retain full
credit. [Redacted receipt](../scripts/rust-operator-contracts/showmigrations/verified-native-verbosity-2-plan-ca752176.json)
