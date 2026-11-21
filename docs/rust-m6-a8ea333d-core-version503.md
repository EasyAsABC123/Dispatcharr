# Scheduled Version HTTP 503 on the opt-in Core owner

The paired run used exact Rust application `a8ea333d922f9834afc561493fc6b104f5e2b5d3` and pinned Original Dispatcharr `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`. Original used its worker and DatabaseScheduler. Native used the opt-in shared Core receiver and scheduler and reached Core readiness. Selected workers, Selected Beat and the standalone Version startup publisher were disabled.

One saved daily Version root per backend called the owned version origin once and received HTTP 503. The task settled as `SUCCESS/null`, advanced the durable daily run count once, and produced zero Version SQL notices and zero Version events. The existing Redis startup guard retained its expiry deadline, and worker readiness published no extra Version task. Both queues and in-flight claims drained. The controller and independent reducer returned 0, with exact teardown of six Original and five Native roles.

The earlier [Selected Version 503 receipt](rust-m6-5d9e558b-version503.md) remains separate. This receipt covers the saved HTTP 503 workflow on that exact Core image. The four current HTTP 200 cases, full default worker graph, M6 milestone, and performance have separate gates. [Redacted proof](../scripts/rust-integration/m6-a8ea333d-core-version503.json)

Original Dispatcharr, Django, Celery and django-celery-beat authors retain credit.
