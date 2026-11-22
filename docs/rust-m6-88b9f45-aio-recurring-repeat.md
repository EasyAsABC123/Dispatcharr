# Recurring schedule repeat on ordinary Rust AIO

The ordinary Rust `88b9f45` Compose arm ran with `DISPATCHARR_ENV=aio` and all eleven owner overrides absent. It reached the packaged Core scheduler and receiver readiness gates. Original was verified from its retained physical run with the same owned recurring booking fixture. Both used isolated PostgreSQL, Redis, and a local observer.

Two natural hourly occurrences of the same saved rule returned 1 then 0. The first produced one future scheduled recording, one linked UTC one-off row, one successful local artwork task, and one Channels event. The second left the booking unchanged. The broker and result observers verified both distinct roots and zero ready or reserved messages at the end. The media was scheduled outside the dispatch window.

Preflight, physical execution, and the independent comparison exited zero. The original proof and fresh Rust proof verified exact scoped cleanup. This comparison covers the ordinary AIO recurring repeat workflow. The [machine-readable proof](../scripts/rust-integration/m6-88b9f45-aio-recurring-repeat.json) records the checks, results, and retained evidence hashes.
