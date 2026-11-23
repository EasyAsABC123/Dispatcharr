# ARM64 pre-consumer census without Django

On Lappy the exact `0c5133b` Rust package stayed created at PID 0 while a separate observer queried the disposable PostgreSQL and Redis services. The observer used the stock PostgreSQL client and a bounded RESP2 connection without loading Django, Dispatcharr or Kombu. It read the installed Kombu source to enumerate priorities.

The database query confirmed the owned database and a read-only transaction. At the held boundary the migration table was absent, as expected, and the Redis census found all eight priority lanes empty, with zero unacknowledged entries, index entries, result keys or unexpected lanes. The same Redis connection and server generation covered the census.

The observer's cgroup memory charge peaked at **24,817,664 bytes (23.67 MiB)** through disposal, below the **86.4 MiB** headroom limit under the **96 MiB** cap. Swap and memory events were zero. Ordinary fixture stop terminated the observer with exit 137 and its OOM flag clear. Cut, disposal probe, stop, down and actual run all exited zero. Teardown verified all four containers, one network and one volume absent.

The first attempt stopped before creation because the Docker volume list rejected a flag. The second stopped after detecting an application import and verified its exact teardown. The third run verifies an empty pre-consumer boundary for the held `0c5133b` candidate. Signed HTTP, active worker claims, pre-window history, result policy and Original equivalence remain open. [Aggregate evidence](../scripts/rust-integration/m9-0c5133b-arm64-no-django-held-census.json)

The original Dispatcharr authors and contributors retain full application and React credit.
