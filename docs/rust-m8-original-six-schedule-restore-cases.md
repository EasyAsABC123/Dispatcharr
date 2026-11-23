# Original backup schedule and missing-file restore

Six declared requests passed on stock Original `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` through an authenticated administrator. The isolated Compose fixture kept PostgreSQL and Redis local. The controller, evaluator, fixture and outer owner all exited zero. [Sanitized receipt and source identities](../scripts/rust-integration/m8-original-six-schedule-restore-cases.json)

| Case | HTTP | Observed effect |
| --- | --- | --- |
| 1 | 200 | Initial daily schedule enabled at 03:00 with retention three. |
| 4 | 400 | Unsupported frequency left the settings, scheduler, cache and broker state unchanged. |
| 5 | 400 | Invalid time shape left those same fields unchanged. |
| 6 | 200 | Original accepted numeric `99:99`, saved the value, retained the periodic task ID and replaced its cron with hour and minute both `99`. |
| 7 | 200 | Disable saved 03:30 and removed the scheduled backup row and its now-unreferenced cron. |
| 8 | 404 | Missing owned archive caused no task/result or scoped state changes. |

Unrelated scoped settings, cache entries and broker/results remained unchanged across all six calls. Teardown independently verified absence of two exact containers and their host process generations, the private network by name and ID, and an empty project inventory. The fixture used tmpfs and owned no Docker volumes.

The first attempt exited 1 at fixture startup before the six calls. The successful retry repaired the controller's network identity check and preserved the failed receipt. These are Original web observations. The physical Beat/worker cases, retained archive restore, remaining 16-case Source authority and Native M8 parity remain open.

Dispatcharr’s original authors and contributors retain full credit for the application and React UI.
