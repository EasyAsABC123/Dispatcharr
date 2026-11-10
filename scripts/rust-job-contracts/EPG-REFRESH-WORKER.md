# Original EPG refresh worker reference

Original Dispatcharr authors and contributors retain all application/algorithm
credit and AGPL licensing. Celery, Kombu, Django, PostgreSQL and Redis authors
retain their framework credit and licenses.

[The real Original worker gate](verified-original-epg-refresh-worker-2.json)
passes50 checks over seven cases in Docker Compose with a copied database and
owned broker8/result9. The Original producer, task implementation and prefork
worker run unchanged; signal observers record their messages and outcomes.

Inactive and dummy sources return SUCCESS/null and preserve the previous index.
A busy refresh lock also returns null, preserving the other owner's lock/index.
A missing source returns a cleanup string and deletes its named orphan schedule
and sole-owned interval. These results must not be inferred from index-task null
semantics.

A busy file lock or active per-channel parser count first invalidates the index,
then publishes a new refresh child with preserved force=True, incremented
_file_defer_retry, retries header0 and parent/root lineage. Actual ETA observations
confirm a15-second countdown, rather than the index task's5seconds. Exhaustion at
the captured maximum480 sets the source error status, returns SUCCESS/null and
publishes no child. The declared task hard limit is14400seconds; this capture does
not wait four hours or prove hard-limit behavior.

Fixture workers stop before deferred children are due; warm restoration and the
captured envelopes retain those publications. No unrelated family executes.
Owned rows/keys are cleaned, the default fixture database is unchanged and all
projects/volumes are removed. The first attempt's incorrect observer field name
is retained as a failed fixture attempt; the corrected observer reads the actual
last_message field.

Only explicit force=True keyword requests are covered here. Default/positional
forms, a successful local import, index/SeriesRules child publication, wrapper
failure cleanup, remote/compressed imports, a native worker, mixed-family intake,
calendar scheduling, application wiring and performance remain separate gates.
Existing FileJobRunner service tests do not establish this task transport.

```sh
python3 scripts/rust-job-contracts/run_epg_refresh_worker.py \
  --output artifacts/rust/epg-refresh-worker-local
```
