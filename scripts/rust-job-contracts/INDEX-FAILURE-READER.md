# Bounded failure metadata reader

Original Dispatcharr authors and contributors retain all application/algorithm
credit and AGPL licensing. Celery, Kombu, Redis/redis-py, Django and framework
authors retain their credit and licenses.

The generic result decoder previously accepted only builtin ValueError/KeyError.
An actual captured index failure therefore returned UnsupportedException even
though the Original AsyncResult accepted the native writer's record. The retained
before-test exposes that rejection; [the later reader gate](verified-index-failure-reader-1.json)
binds the corrected code, corpus and actual Redis checks.

The reader recognizes the observed Directory, ProgrammingError, cached connection/
authentication/permission, startup AttributeError and child-publication
OperationalError descriptors. Directory requires exact integer21 and the captured
message. The other new forms require one string of at most8KiB and the exact
module/class pair. Existing builtin ValueError/KeyError behavior and global
identity, size, nesting and tagged-value guards remain in place.

These are data descriptors. No module is imported or exception constructor run.
The full result object and opaque traceback are preserved. Reading a supplied
ProgrammingError or NoPermissionError does not classify an arbitrary native SQL,
network or Lua error into that exception. Python exception display and HTTP task
status projection remain separate gates.

Nineteen captured Original/native records are checked against real owned Redis,
including descriptor arguments, date, diagnostic and unchanged key bytes/expiry.
Foreign modules, wrong errno and excessive strings are rejected; an absent key
remains pending. The99-check fixture uses no PostgreSQL: the disposable database
name is only the existing fixture constructor's marker. Its Redis container has
no external network and is removed afterward. No application worker is attached
and no performance gain is inferred.
