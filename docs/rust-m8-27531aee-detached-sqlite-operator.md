# M8 detached historical SQLite copy upgrade on AMD64

The opt-in Rust operator built from application `27531aeeea69f53ebb37f33c4069bda0fd2efa8f` upgraded an owned Dispatcharr v0.11.2 SQLite copy from 132 to 184 migration records. The separate output file had mode 0600 and 62 application tables. The verifier matched the expected migration names, table names, named columns, indexes and foreign keys. `core_coresettings` was the sole table with different stored DDL text. SQLite integrity and foreign key checks passed.

The upgrade preserved the fixture user's ID, username, stored password field and privilege/status flags, and the fixture M3U account's ID, username and password field. These were compared in memory from read-only immutable connections. The receipt contains only row counts. It establishes stored-field equality for those two owned rows. The fixture did not exercise login or provider authentication.

The ordinary package and opt-in sibling had byte-identical Main binaries. The ordinary build used `DISPATCHARR_BUILD_SQLITE_OPERATOR=0` and `DISPATCHARR_BUILD_COPIED_SQLITE_RUNTIME=0`. The sibling enabled `DISPATCHARR_BUILD_SQLITE_OPERATOR=1` and kept `DISPATCHARR_BUILD_COPIED_SQLITE_RUNTIME=0`. Both were built from source archive `5ddebde22eb743d67597b9629db25ae740f05c49ebe8e8868e39d611bfa2bd86`. The sibling's image ID was `sha256:5c3341b8b9d988d057b97d608fefb95ca43f30627fa6e6be823a437116101fbe`.

| Physical case | Literal exit | Observed result |
| --- | ---: | --- |
| Ordinary help | 0 | The detached upgrade command is absent |
| Opt-in help | 0 | The detached upgrade command is present |
| Historical copy upgrade | 0 | A new output passed migration, schema, integrity and preservation checks |
| Unlisted migration history | 1 | Input refused without leaving an output or staging file |
| Late duplicate row | 1 | Upgrade refused without leaving an output or staging file |

All five operator cases ran without networking as UID/GID 65532. The three upgrade inputs were mode 0444 and mounted read-only. Each input's before/after digest matched, and the historical master stayed unchanged. Each case removed its exact owned container and recorded empty project-label container, network and volume inventories. The successful fixture output was removed after its checks. Results were reduced from literal zero host, campaign, preflight, build, run and proof receipts. The raw physical receipt SHA-256 is `a0e89633f64be212fde78ce1cf98dd699e56369c40251f86e1cbf4de3b3906bf`.

Ordinary AIO uses PostgreSQL. The ordinary Rust package rejects explicit `DB_ENGINE=sqlite`, as checked in its exact archived source. The separately built copied-data web option requires `DISPATCHARR_BUILD_COPIED_SQLITE_RUNTIME=1`, `--sqlite-copied-data-runtime`, `DB_ENGINE=sqlite` and an explicit private `RUST_SQLITE_COPIED_DATA_ROOT`. That runtime, production SQLite startup, in-place upgrades, live database and WAL ownership, SQLite backup or restore, historical SQLite-to-PostgreSQL conversion, and performance are outside this physical receipt. M8 remains open.

Full credit belongs to the original Dispatcharr authors and contributors, Django, SQLite and all dependency authors. LICENSE, ATTRIBUTION.md and SQLite dependency notices were matched against their pinned digests.
