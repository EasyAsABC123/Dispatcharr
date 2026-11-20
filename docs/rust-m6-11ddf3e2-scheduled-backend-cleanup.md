# Selected scheduled Redis backend cleanup on 11ddf3e2

Original's stock scheduler and packaged Rust Selected worker and Beat
each ran one due Redis backend cleanup at the 04:00 UTC schedule. Both
recorded one durable run, a successful stored null result and empty
queues. The expiry setting was 43,200 seconds. The prior Version
result kept its bytes and aged normally. Other schedule rows stayed
unchanged.

The copied fixture again observed one startup Version request and one
event per backend. The paired controller exited 0 and verified the
retirement of six Original and five Native role generations. Full M6,
default worker graph verification and performance remain open.
[Redacted receipt](../scripts/rust-integration/m6-11ddf3e2-scheduled-backend-cleanup.json)

The original Dispatcharr, Django, Celery and dependency contributors retain full credit.
[Attribution](../ATTRIBUTION.md)
