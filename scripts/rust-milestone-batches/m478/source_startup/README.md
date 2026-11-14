# Original normal startup/cache authority

The Original Dispatcharr authors retain full application credit; see
[ATTRIBUTION.md](../../../../ATTRIBUTION.md).

One new, bounded Original WSGI context captured startup and four real HTTP
requests. Original Python inputs (639 files) were unchanged. This uses the
standard `dispatcharr.wsgi.application` under wsgiref, actual process role
`django`, and actual initialization branch `should_skip_initialization=False`.
It is not a uWSGI/media-runtime or full M8 acceptance result.

The disposable Compose fixture uses the pinned Original image, no network,
no published ports, a cloned database, owned Redis, no Celery worker/beat,
and the application's explicit plugin-discovery optout. The cloned settings
and users come from the retained Source16 seed without executing its APIs.
Application startup otherwise executes unchanged, including backup scheduler
and local developer-notification initialization.

Actual evidence shows normal startup populates the DB1 system_settings group
before the first HTTP request. An actual settings PATCH invalidates the group
and stamps the process-local display-timezone debounce. The immediate GET
leaves it absent; the GET after an observed 5.100067 seconds repopulates it.
All four responses were HTTP 200. Default 62 tables/59 sequences were unchanged.
The controller, 15 scoped checks, and independent cleanup passed. No performance
claim is made.

This explains the earlier Native41 extra cache key: the retained management/API
Source process had just executed the setter, while production Main was fresh.
That earlier strict exit 2 remains literal. Source and Native serialized pickle
and Redis DUMP bytes differ despite equal typed UTC values; exact storage-byte
parity is not claimed. Do not remove Main's correct request-start callback to
make that earlier comparison pass.

Safe observations are in `evidence/source-normal-startup-summary.json`.
Raw private SQL/auth/body observations stay in
`artifacts/rust/m478-source-normal-wsgi-cache-1/`; their hashes are published.
The scripts retain the executed helper dependencies and require explicit
`--execute`. Dispatch only through the global fixture coordinator; the default
`run.py` invocation is read-only. Original authorship and licenses are preserved.
