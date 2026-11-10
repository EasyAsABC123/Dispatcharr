# Original double-failure and release ACL precedence

Original Dispatcharr authors retain full application/algorithm credit and AGPL
licensing. Celery, Redis/redis-py, PostgreSQL and Django authors retain their
framework credit and licenses.

[verified-original-index-double-failure-2.json](verified-original-index-double-failure-2.json)
binds the exact five-case observation dataset, task/observer/controller tools and
failed setup attempt.46 checks pass:17 existing worker references and29 new
precedence checks. The copied database had26 nonempty tables; the default database
manifest remained unchanged. Three owned ACL users were removed and the fixture
project/volumes were removed.

Only the cached Redis client's delete dependency changed. Actual Original acquire,
index builder, task `finally`, lock renewer and Celery tracing ran unchanged. A
bound, unlistened loopback socket prevented endpoint reuse. ACL clients had access
to only their owned lock key and could read it, while actual DEL was denied.

| Execution | Release | Terminal backend exception | Index | Lock |
| --- | --- | --- | --- | --- |
| Directory open | Refused connection | redis.exceptions.ConnectionError | Absent | locked,TTL300 |
| SQL trigger P0001 | Refused connection | redis.exceptions.ConnectionError | Absent | locked,TTL300 |
| Normal Written | DEL NOPERM | redis.exceptions.NoPermissionError | Committed | locked,TTL300 |
| Directory open | DEL NOPERM | redis.exceptions.NoPermissionError | Absent | locked,TTL300 |
| SQL trigger P0001 | DEL NOPERM | redis.exceptions.NoPermissionError | Absent | locked,TTL300 |

Release failure wins the terminal exception. The original Directory or
ProgrammingError remains in traceback context. A later release failure does not
roll back a committed index. NOPERM is not authentication failure: its argument
includes the authenticated ACL username and the denied `del` command.

The first attempt stopped at a missing helper import before task cases ran. It is
retained with unchanged default-database audit and completed cleanup. Fresh2 fixes
only the controller import path.

No native class or message mapping follows automatically. Native release uses
owner-fenced Lua, unlike Original plainDEL; script/command ACL behavior needs its
own comparison. The separate cached-client restart gate also exposes native
reconnect differences. Native endpoint/error arguments must be observed rather
than invented from a coarse unavailable state. Until proved, unknown release
families, CAS-not-owner, physical timeout and ambiguity remain operational.
