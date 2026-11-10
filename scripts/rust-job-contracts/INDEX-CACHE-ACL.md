# Original cached Redis ACL contract

Original Dispatcharr authors retain full application/algorithm credit and AGPL
licensing. Redis/redis-py, Celery, PostgreSQL and Django authors retain framework
credit and licenses.

[verified-original-index-cache-acl-1.json](verified-original-index-cache-acl-1.json)
binds six actual task cases and two controlled script probes.48 checks pass
(17 existing worker references and31 ACL checks). Six owned per-key users were
removed. The default database manifest stayed unchanged and the copied-database
fixture project/volumes were removed. There were no failed attempts.

| Cached user policy | Original result | Index | File lock |
| --- | --- | --- | --- |
| SET denied | NoPermissionError naming `set` | Absent | Absent |
| SET denied, directory input | Same acquire failure; no directory open | Absent | Absent |
| DEL denied | NoPermissionError naming `del` | Committed | locked |
| Key pattern denies lock | NoPermissionError: No permissions to access a key | Absent | Absent |
| Plain connection/GET/SET/DEL/EXPIRE, no EVAL | SUCCESS/null | Committed | Released |
| EVAL allowed, plainDEL denied | NoPermissionError naming `del` | Committed | locked |

The denied-command argument includes the authenticated ACL username. It is not
AuthenticationError. Only safe host/port/database/username context is retained;
no password or credential-bearing URL is published.

A controlled LuaDEL probe with EVAL permitted but DEL denied produced
`redis.exceptions.ResponseError` with script/user-script/SHA context, while the
Original task's plainDEL produced `NoPermissionError`. Its probe value remained.
The plain-command user also demonstrated actual denied EVAL while the Original
index task succeeded. Consequently native owner-fenced Lua cannot be assumed to
match the Original command-permission surface or error argument.

No native permission mapping or generic release exception follows. Keep raw
codes/details and physical stage/commit/release evidence. Do not fall back to an
unsafe unfenced deletion or synthesize plainDEL arguments by stripping script
context. Bounded reconnect and an actual same-ACL native comparison are separate
gates. Unknown permissions, CAS ownership loss, timeout and ambiguous replies
remain operational until a supported policy is explicitly verified.
