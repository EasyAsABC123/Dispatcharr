# Native restore refusal parity at cf077146

The ordinary AMD64 Compose workflow verified both safe restore refusal results byte-for-byte against the earlier Original observations. A valid archive missing `metadata.json` returned `Invalid backup: missing metadata.json`. An archive marked SQLite in the PostgreSQL fixture reported both database types in the result. Both refusals preserved the scoped database state.

The same run exercised five real worker jobs, signed backup download and result retrieval, both exact empty HTTP 204 deletions, a positive same-schema restore and naturally due UTC retention of two of five backups. The independent template database stayed unchanged. Preflight, the physical run, evaluation and exact teardown passed.

The two-file change matches the two observed refusal messages. Full M8 and formal root acceptance remain open. Original Dispatcharr authors and contributors retain full project and React UI credit.

[Scoped Compose receipt](../scripts/rust-integration/m8-cf077146-native-restore-refusal-parity.json)
