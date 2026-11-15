# New Source HTTP producer authority — prepared, not dispatched

Original Dispatcharr, Django, Celery, uWSGI/gevent and dependency authors retain
all credit and licenses. This is ONE bounded ordinary Source-only cohort after
Root releases its current heavy lease. No applications, Cargo or image build
have run for this proposal. All Source9 task/failure captures are reused; no task
worker/Beat/media consumer is started, and none of those task bodies is invoked.

The existing Source9 host bootstrap, pinned Original image/archive, PG17 schema,
Redis, role_owner, qualified web-role uWSGI configuration and absolute HTTP socket
owner are reused. The exact production WSGI application serves real TCP requests
under uWSGI/gevent with one warm worker. The AIO's attached worker/Beat/Redis/ASGI
daemons are intentionally excluded; the existing storage instance is used and
task frames remain unconsumed. This is web-role authority, not an entire AIO
startup/performance qualification. Natural web startup frames/notifications remain
literal, separately recorded from per-request family frames.

One disposable DB/network-none Compose project holds the warm server. Real users,
API keys and JWTs are created through original models/token code; an ephemeral
signing key and all credential values stay in memory. Only role/subject metadata
and credential hashes are retained. No login/API auth bypass or Source function
replacement is used. All four actual routes cover admin/standard/anonymous/
inactive/bad JWT/API-key schemes/mixed-header precedence, method/OPTIONS,
permission-before-JSON, empty/nonlist/malformed/duplicate input, global/user UI
network denial and ignored spoof headers with trusted proxies disabled.

Extra cells capture source-ID logo count/publication, mutually exclusive selector
ordering, invalid source-ID publication BEFORE HTTP500, bulk profile scalar/list/
sentinel normalization, fractional/-1 starts and4097-ID publication. These can
expose real Native producer/coercion/capacity gaps. They must remain literal;
Source behavior is not altered to match the existing Rust bounded profile.
Every request persists raw status/headers/body before JSON parsing, physical
socket/timer retirement, exact queue bytes/body/metadata, real live web PID/
generation/PGID/hostname, reply/delivery UUID types, and unchanged task business
tables. No backend results and absence of a task worker verify publication-only
scope. This does not remeasure Source9 normal/failure task execution.

The host uses the existing coordinator lock/schedule/command implementation and
persists one ordinary lease. Its inherited600-second functional and90-second
cleanup bounds remain. Web master/children and the role-owner process get actual
normal waits/pipes/generation retirement before container destruction; all project
resources/CIDs/host CLI groups/default DB62/sequence59/Redis7 are independently
checked. Parent scheduling approval is required BEFORE --dispatch; currently the
proposal is artifact-only. Complete tools/schema/pg-init/archive/coordinator input
hashes are bound before any application action, correcting the narrower Source9
historical helper-binding scope without replaying its task cases.

`tools/cases.py` is a pure plan generator. `qualify.py` reads a future actual
output; neither starts an application. The existing Native callback must be
extended with these actual HTTP/frame counterparts AFTER measurement, keeping
newly exposed malformed/capacity/source-ID gaps visible rather than claiming
parity from publication alone.
