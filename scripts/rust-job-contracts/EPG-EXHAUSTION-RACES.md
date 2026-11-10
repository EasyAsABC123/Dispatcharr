# Original exhaustion races and return semantics

Original Dispatcharr authors retain full application/algorithm credit and AGPL licensing; all framework authors retain their respective credit and licenses.

`verified-original-epg-exhaustion-races-1.json` binds55 checks and six actual Original worker executions in an owned copied database. No application/task/helper is replaced. The default database remains unchanged and the project is removed. This is Source-only evidence; historical native UPDATE0/NotOwner pending-result controls are not retroactively parity passes.

Each case begins with a real file-busy or parser-busy retry480 task. The controller holds a PostgreSQL no-key row lock and witnesses the worker's actual blocked status UPDATE. The stored index is already invalidated and the real refresh lock is acquired before intervention. An owned statement-level transition-table observer records the UPDATE's actual affected count, and an owned Channels group membership receives the real progress message.

| Intervention at blocked UPDATE | Actual affected rows | Progress messages | Actual Original result |
| --- | --- | --- | --- |
| Django collector deletes owned source/index, then commits | 0 | 1 | SUCCESS/null |
| Replace only owned refresh key with a foreign token, then unblock | 1 | 1 | SUCCESS/null |
| Delete only owned refresh key, then unblock | 1 | 1 | SUCCESS/null |

Both reasons are captured for every intervention. Source's unchanged `release_task_lock` calls DEL unconditionally; the replacement key is absent afterward. The helper has no explicit return. The actual worker result is null, not proof that a lease was still owned or that a replacement was preserved. Exact release/renewer source hashes and private implementation capture are retained.

All cases complete in less than60 seconds, before the unchanged renewer's120-second interval. No renewal-tick or unbounded-concurrency behavior follows. The row-deletion case proves that UPDATE0 still publishes through the real Source Channels layer. Result metadata, TTL, source/index absence or error state, query hashes and intervention details are captured.

The native follow-up must preserve foreign ownership and avoid new stale writes; Source's deletion of a replacement key does not authorize deleting a foreign key. Only the physically settled return semantics are being reconsidered. A future typed release disposition must distinguish acknowledged NotOwner from successful Released and from unknown/unsettled failure. No broad Failed promotion, scheduler, default consumer, public202 or performance claim is made here.
