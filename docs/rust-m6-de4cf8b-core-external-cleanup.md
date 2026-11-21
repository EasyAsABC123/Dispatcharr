# Default Rust Main with the Original scheduled task owner

The packaged Rust application `de4cf8b0423e516adc2aaeb709f71ceff1e332ab` reached readiness
with Selected Jobs and Beat disabled. A real Original Beat had already
published one backend-cleanup task from its saved UTC schedule. The exact
pending bytes and absent result survived Rust Main readiness on the same
copied PostgreSQL and Redis services.

The restarted Original default worker consumed that task while Rust Main
remained ready. It stored a SUCCESS result with a null value. The schedule
advanced once, the other saved rows stayed fixed, and the final broker
queues were empty. Rust Main exited normally. The three owned containers
and their host process generations were independently absent after teardown.

The first physical attempt exited 1 before Rust Main started. Its fixture
treated the two legitimate journal and startup observers as service roles.
The first run also recorded exact teardown. The fresh Compose run and
independent reducer both exited 0.

The exact package manifest is `4ad88753c487193a0d7b6765e38792378090658cd4bcd2249d227983f1e663be`.
The qualified proof SHA-256 is `f93ac6fc3dedcf7657b4e5609d3f6563c2aaa06f17fe71dea465b7244a733e0b`. This scope uses the Original
scheduler and worker. Native default worker activation, whole M6 parity,
and comparative performance remain open. [Redacted receipt](../scripts/rust-integration/m6-de4cf8b-core-external-cleanup.json)

Original Dispatcharr, Celery, Django and django-celery-beat authors
and contributors retain full credit.
