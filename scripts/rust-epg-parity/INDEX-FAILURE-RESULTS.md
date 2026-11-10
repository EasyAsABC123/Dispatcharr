# Original programme-index worker failure results

Original Dispatcharr authors retain application and algorithm credit. Celery, Kombu, Django and redis-py retain their framework and transport credit.

Frozen136 executes the actual original task in a prefork worker with copied PostgreSQL, owned files and isolated broker/result Redis databases. It passes42 observations; the project is removed and the original database manifest is unchanged. The original task, lock helpers and Celery trace code are unchanged. Two controlled dependency adapters point only the cached lock client at an owned refused loopback endpoint or deliberately invalid authentication. The release-only adapter allows acquisition/index writing before its delete fails. A copied-database SQL trigger matches one owned source only.

| Case | Execution/backend outcome | Index/lock effect |
| --- | --- | --- |
| Missing source or file | SUCCESS, null result | No index, no remaining lock |
| Malformed XML | SUCCESS, null result | Raw scanner indexes the opening programme tag |
| Invalid UTF-8 channel | SUCCESS, null result | Replacement character and HTML entity decoding retained |
| Empty file | SUCCESS, null result | Empty channel index stored |
| Directory path | FAILURE, builtins.IsADirectoryError | No index, lock released |
| Matching-source SQL fault | FAILURE, django.db.utils.ProgrammingError | No index, lock released |
| Lock-client refused connection/auth | FAILURE, redis.exceptions.ConnectionError/AuthenticationError | No index or acquired lock |
| Lock release refused connection | FAILURE, redis.exceptions.ConnectionError | Index stored; original lock remains until fixture-only cleanup/expiry |
| Same UUID succeeds, then directory error | Later execution FAILURE; backend remains prior SUCCESS | Prior result bytes/date/TTL preserved; existing index retained |

Each observed initial result has keys `status`, `result`, `traceback`, `children`, `date_done`, `task_id`; `parent_id` is absent and children are empty. Success tracebacks are null. Failure result values preserve `exc_type`, `exc_message` and `exc_module`; complete original Python tracebacks are captured. The result TTL is3600 seconds under the captured explicit fixture configuration. Endpoint-specific messages and absolute fixture paths are observations, not reusable literals for arbitrary deployments.

Exact synthetic result bytes, task IDs, dates and tracebacks are in `source-index-failure-results-136.json`. The concise case/side-effect/tool-hash manifest is `verified-index-failure-original-136.json`. The frozen runnable tool closure is listed in `artifacts/rust/index-failure-stageable-136.json`.

Attempt135 is retained: its queue observer assumed a message could not be consumed while the worker process was stopped, but a pending Redis BRPOP can pop server-side.136 uses normal actual `task.delay` and postrun/result observations without that assumption. No native exception writer is accepted by this corpus. Parented/ETA failures, result-backend outage/restart ambiguity and broad native result-writing parity remain separate gates.
