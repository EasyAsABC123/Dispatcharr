# Direct cached permission classification

Original Dispatcharr authors and contributors retain full application and
algorithm credit and AGPL licensing. Redis/redis-py, Celery/Kombu and framework
and dependency authors retain their original credit and licenses.

The test-only native adapter now reproduces the observed direct SET/key-denial
terminal payload through actual Original publication and result consumption.
[The classified gate](verified-index-set-permission-classified-1.json) passed
93 checks: seven Source profiles, seven native raw-report comparisons and six
classified immediate/scheduled worker cases. Four classifier units and strict
all-target Clippy passed against the same immutable 540-file Rust snapshot.
This does not wire an application worker, scheduler or HTTP 202 endpoint.

The six worker cases cover SET denied, SET denied before a directory can be
opened, and denied key access, each in immediate and scheduled execution.
Original's `AsyncResult.get(propagate=False)` reconstructed exact
`redis.exceptions.NoPermissionError` module, class and typed arguments from
native metadata. Failure state, empty children, matching task ID, absent parent,
UTC date, result TTL, no index/lock and physical caller/job/lease/service/transport
drain were checked. The traceback field contains an honest native diagnostic;
Python frames or exact traceback equivalence are not claimed.

Classification requires `AcquireFileLock`, `NotWritten`, explicit bounded
server `NOPERM`, exact observed SET/key detail, no IO cause or truncated fields,
and physically settled acquisition cleanup. A SET denial occurs before the
Original protected body and finally release. The native owner-checked cleanup
is an additional safety operation, not a Source finally exception overriding SET.
The adapter does not infer a denial from an arbitrary string or fabricate an
errno, host or authentication failure.

[The raw native gate](verified-index-cache-acl-raw-native-1.json) separately
passed 69 checks before mapping. Its three direct SET/key cases matched Source
arguments exactly, with no index or lock. The
[seven-profile Source extension](verified-original-index-cache-acl-2.json)
passed 52 checks; a quick GET-denied task succeeds because the renewal GET is
delayed. That establishes no long-running renewal behavior.

Four capability differences remain explicit: plain DEL denial, plain-command
success without EVAL, Lua DEL denial with script context, and GET denial during
native Lua release. Original can complete with only connection/GET/SET/DEL/EXPIRE
permissions. Native owner fencing uses Lua GET/DEL and requires additional
capabilities. A Lua `ResponseError` is not rewritten into Original's plain DEL
`NoPermissionError`, and script context is not stripped. No unfenced deletion is
used to hide the difference. Unknown release, CAS-not-owner, timeout, ambiguous
reply, network and startup failures remain operational.

The successful tool closure is recorded in the classified summary; its frozen
runner docstring predates a wording-only correction. All executable fixture
bytes are retained and hash-bound. Source and native reports preserve synthetic
ACL usernames where required by exact error text, never passwords or credential
URLs. Baseline cached-connection restart/recovery evidence is independent of
this permission payload gate. See [the Source ACL inventory](INDEX-CACHE-ACL.md)
and [finally-release precedence](INDEX-DOUBLE-FAILURE.md).
