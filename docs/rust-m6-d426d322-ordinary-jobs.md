# M6 local gzip Refresh All and daily backend cleanup on d426d322

The exact packaged Rust revision `d426d322b27a197aaaaa057ef73dfb53c516c452` passed two ordinary job workflows in isolated Compose projects. Both physical controllers and both independent reducers exited 0.

- **Local M3U Refresh All.** The production Rust HTTP route returned 202 and used the explicitly configured Original Celery owner. Selected Rust Jobs and Beat remained off. One worker completed the aggregate and both account tasks. The two-member gzip and plain control produced two imported streams, two Default Group memberships and the expected URL-only hashes. The input hashes and settings-cache behavior matched the fixture expectations. The Original consumer and all owned roles retired.
- **Daily Celery backend cleanup.** Original worker and Beat and Rust Main with Selected Jobs and Beat each completed startup Recovery and Version, then handled one normally due `celery.backend_cleanup` task from the stock 04:00 UTC schedule. Both observed one durable run-count increment and the stock 43,200-second expiry. The existing result bytes remained intact while their TTL aged. Other schedule rows stayed unchanged, queues were empty, and independent cleanup verified six Original and five Native role generations.

These are two explicit worker configurations on the same image. The full default job graph, complete M6 acceptance and comparative performance remain open.

[Machine-readable receipt](../scripts/rust-integration/m6-d426d322-ordinary-jobs.json)

Original Dispatcharr, Celery, Django, django-celery-beat and dependency authors retain full credit.
