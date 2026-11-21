# Saved plugin repository refresh on the shared Core worker

On `2c939204e070954e7de86b662f362237b5f7f9ae`, an enabled saved six-hour
`plugin-repo-refresh-task` fired once in each Compose project. The Original and
Rust captures linked the job to a stored `SUCCESS` result with a null value,
one scheduler tick and an empty queue. Repository rows and unrelated scheduler
rows stayed unchanged in the no-enabled-repositories fixture.

With one enabled repository, each backend made exactly one HTTP 200 request to
the owned manifest origin. Both persisted the expected manifest, preserved the
repository identity and unrelated rows, and tied the same result and tick to
that request.

The Rust arm used `RUST_CORE_SCHEDULE_OWNER=rust` and
`RUST_CORE_PLUGIN_REFRESH_OWNER=rust`, with one scheduler and one queue receiver.
The owned startup journal showed no task publication before the saved job was
armed. Both projects had exact process and container retirement.

Other trust and error paths for repository fetches, plugin execution, startup
ETA publication, the complete default worker graph and comparative performance
need separate evidence.
[Exact scoped receipts](../scripts/rust-integration/m6-core-saved-plugin-refresh.json)

The original Dispatcharr, Celery, Django and django-celery-beat authors retain
full credit.
