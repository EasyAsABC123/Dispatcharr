# Original and Native same-schema PostgreSQL restore on ARM64

The original Dispatcharr authors and contributors retain full credit for the application. See [Attribution](../ATTRIBUTION.md).

Original and [Native](rust-m8-f960875-arm64-standalone-restore.md) completed separate ordinary HTTP submissions and task execution using fresh, owned PostgreSQL targets on Lappy. Each restored an independently generated current-schema synthetic archive. The underlying archive bytes came from their own executions.

| Check | Original | Native |
| --- | --- | --- |
| Archive bytes | 36,680 | 36,652 |
| Signed terminal and corresponding Redis result | Completed | Completed |
| Login with restored credential | 200 | 200 |
| Login with replaced credential | 401 | 401 |
| Representative restored SQL, migrations, archive and independent control | Passed | Passed |
| Exact retired containers, networks and volumes | 7, 1, 2 | 6, 1, 2 |

Original used the stock Celery worker and normal task API. Its fixture held two pre-existing startup lock keys in the disposable cache to isolate the restore. Native used the explicitly selected standalone restore owner. Source worker and API shutdown, Native shutdown, and both probes returned 0. Both controllers verified exact retirement and empty project inventories.

The earlier Original run passed the restore probe but expected generic Celery log messages that stock application logging suppresses. The final run used Original's task-ID-matched start, migration and completion log records. The earlier controller exit 1 remains in the [machine-readable evidence](../scripts/rust-integration/m8-f960875-arm64-same-schema-restore-pair.json). Older schemas, enabled plugins, broader database contents, production restore and comparative restore performance remain outside this scope.
