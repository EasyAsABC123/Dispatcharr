# Grouped Core scheduler preview on 66b02c4a

The existing `66b02c4a` ordinary image passed one shared opt-in Core preview
per backend. Original and Rust each published one saved daily Version root,
made one HTTP 503 origin request, and recorded `SUCCESS/null`. Both preserved
the startup guard deadline and produced zero Version notices or events.

Each scheduler then published one saved UTC backend cleanup root with
43,200 seconds of expiry and returned `SUCCESS/null`. The previously
recorded Version result bytes stayed identical and the TTL continued aging.
Both counts and clocks persisted through normal stop, with no late frames.
All six Original and five Native owned roles and their exact process
generations retired.

The first physical arm had exact cleanup. Original completed the Version
work, then the observer waited for a durable count before issuing the next
normal schedule-change marker. Stock Beat flushes its dirty entries before
reloading. The corrected observer checked persistence after that marker and
kept the original result, clock, expiry, and cleanup assertions. Both
physical and independent proof exits were zero on the same image.

This scope used explicit Core opt-in and covers two settled saved tasks and
normal stop. The default scheduler and worker graph, held-tick lifecycle
cases, and comparative application performance remain open.

[Execution evidence](../scripts/rust-integration/m6-66b02c4a-core-cleanup-version-preview.json)

Original Dispatcharr, Django, Celery and django-celery-beat authors retain
full credit for the application, models, algorithms and protocols.
