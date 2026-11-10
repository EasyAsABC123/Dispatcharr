# Native index terminal results

Original Dispatcharr authors and contributors retain all application and algorithm
credit and AGPL licensing. Celery, Kombu, Django, psycopg, PostgreSQL, redis-py,
Redis and SQLx authors retain their framework credit and licenses.

The immediate and scheduled index workers accept an explicit `Terminal` from an
executor. Existing executors inherit the normal null-success behavior. A raised
terminal value passes through the existing atomic result writer: the same result
key, expiry, pubsub payload, lineage, child tuples and existing-SUCCESS guard.
The guard suppresses only result replacement; the handler still executes.

`Failure::new` bounds arguments and diagnostics and restricts exception identity
to observed forms. Operational errors, admission rejection, cancellation and
uncertain I/O do not automatically become Python exceptions. `Completed` means
the terminal result was processed; callers must inspect the result status to
distinguish SUCCESS from FAILURE.

The detailed IndexService report preserves operation, commit and lease-release
observations independently. The test-only classified adapter maps the observed
directory errno21 and bounded P0001/no-position SQL failures only after confirming
NotWritten and physical lease release. Tolerant malformed XML, lossy UTF-8 and
empty files preserve their actual partial/empty indexes and normal null results.
Other error families and uncertain cleanup remain unsupported operational errors.

The `traceback` field contains a clearly marked native diagnostic. It does not
recreate executed Python frames. Original AsyncResult accepts and reconstructs
the tested exception identities and typed arguments, but literal traceback
equality is an explicit compatibility difference.

The runner uses Docker Compose, a copied database and owned Redis DB8/9. It freezes
compiler inputs and tooling, records hashes and retains failed attempts. Run with
the pinned toolchain environment loaded:

```sh
python3 scripts/rust-job-contracts/run_index_terminal.py \
  --classified --output artifacts/rust/index-terminal-local
```

The controller exercises actual Original publication and result decoding through
both native worker paths. Separate physical tests withhold the real Redis reply
after confirming server-side FAILURE publication, abort the waiter and verify
that caller/admission ownership and shutdown remain held until the reply arrives.
A wrong-type backend key must remain intact and produce a transport error.

[The frozen terminal result gate](verified-index-terminal-results-9.json) records
the actual comparisons and physical tests. The independently reviewed combined
workspace and normal-worker regressions are bound by
[the checkpoint manifest](verified-index-terminal-workspace-1.json).

The classified adapter and PostgreSQL formatter remain test-only; there is no
application worker attachment or default queue consumer in this change. Visibility
polling, mixed-family intake, general cache/producer errors, combined-failure
precedence, recovery, public202, scheduling and whole-application acceptance remain
separate gates. No performance gain is inferred from these functional tests.
