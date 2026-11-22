# Original copied SQLite bootstrap reference

The pinned Original application ran the existing direct web entrypoint
on a disposable historical SQLite copy with the two documented dialect
adaptations. The fixture reached 184 migrations and left its historical
master unchanged. The workload and independent reducer both exited 0.
All owned resources retired.

| Request group | In each generation |
| --- | ---: |
| Channel Profiles | 7 |
| Core catalogs | 24 |
| EPG sources and data | 16 |
| Server Groups | 8 |
| Administrator Users | 8 |
| M3U Accounts | 8 |
| Public configured setup | 1 |
| **Total** | **72** |

The fixture repeated all 72 calls after reopening the listener and copied
database, and reused the administrator bearer. It captured applicable GET
and HEAD status, method and content headers, body lengths and digests
for administrator, standard, streamer and anonymous requests. An
independent SQL census confirmed Users and profile relationships. The
M3U Accounts checks verified the typed nested rows and role-specific
visibility. Repeated reads preserved the login clock and audit state.

The matching Native feature-on run, browser bootstrap behavior and broader
SQLite parity remain open.

[Redacted Original request receipt](../scripts/rust-integration/m8-original-copied-sqlite-bootstrap-72.json) ·
[SQLite dialect feasibility](rust-m8-sqlite-schema-feasibility.md)

Original Dispatcharr and dependency authors retain full credit.
