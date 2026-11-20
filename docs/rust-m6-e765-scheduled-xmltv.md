# e765 scheduled XMLTV and guide descendants

The original Dispatcharr, Celery, and django-celery-beat authors retain full credit.

The exact `e7651a6d` selected Main and pinned Original ran separately in owned
Compose fixtures. Each scheduler published one due XMLTV Refresh task and
persisted one schedule run. Each returned the same successful terminal result
and settled its Programme Index and Series descendants. Both wrote the same
programme and index, fetched the owned XMLTV payload once, and drained their
owned queues. Both executions exited 0 with independent cleanup verification.

This covers one naturally due XMLTV workload and its real descendants. Full
scheduler and M6 acceptance, stock activation, and performance remain open.

[Redacted receipt](../scripts/rust-integration/milestone-batch113/e765-m6-scheduled-xmltv.json)
