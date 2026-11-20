# Original migration plan with dependencies

Pinned Original ran `showmigrations --plan --verbosity=2` using Django
6.0.9 on a disposable PostgreSQL copy. The command exited 0. Its complete
stdout matched the graph’s 184 applied rows, including 203 direct
dependencies in sorted order. Of those dependencies, 29 crossed app
boundaries. The projection without dependency suffixes matched the
previously verified plain plan byte for byte.

The fixture verified normal operator startup SQL, preserved the migration
ledger and backup settings, confirmed the default database was unchanged,
and retired both exact Compose roles and process generations. The original
Dispatcharr and Django contributors retain full credit.
[Redacted Original receipt](../scripts/rust-operator-contracts/showmigrations/verified-original-verbosity-2-plan.json)
