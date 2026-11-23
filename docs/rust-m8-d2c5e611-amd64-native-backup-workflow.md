# AMD64 Native backup and restore at d2c5e611

The ordinary Native all-in-one package ran five worker jobs in an isolated Compose project. It created and inventoried a backup, delivered its signed download, refused two inert archive variants, restored its own same-schema backup and ran a scheduled UTC backup. The scheduled run produced a durable run count of one and retained two of five archives. Publication arrived 2.40 seconds after the scheduled time. The independent template database remained intact and owned teardown verified. The runner, evaluator and outer launch each exited 0.

All four manual admissions returned 202. Ordinary signed status requests returned 200. The three restore requests also exercised the frozen `task_token` spelling and received 401. Both inert archive deletes returned 204 with empty bodies. The evaluator verified that each delete removed exactly its owned archive and preserved the other archive fingerprints. An earlier attempt incorrectly expected 200 and stopped at the first delete. That attempt cleaned up exactly and its literal HTTP status was unrecorded.

The refusal result text still differs from Original on both observed archives.

| Archive | Original | Native |
| --- | --- | --- |
| Missing backup metadata | Invalid backup: missing metadata.json | Native restore archive is not a readable ZIP backup |
| SQLite metadata in the PostgreSQL fixture | Database type mismatch: backup is sqlite, but current database is postgresql | Database type mismatch: current database is postgresql |

Scoped workflow execution passed. The wider M8 acceptance gate remains open. Original Dispatcharr authors and contributors retain full project and React UI credit.

[Scoped receipt](../scripts/rust-integration/m8-d2c5e611-amd64-native-backup-workflow.json)
