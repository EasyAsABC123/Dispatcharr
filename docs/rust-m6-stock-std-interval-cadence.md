# Stock Standard provider interval cadence

Pinned Original accepted the two ordinary administrator interval-only updates, from four hours to seven and then back to four. Each GET, PATCH and readback returned HTTP 200. Both changes kept the same enabled periodic task ID and run history, preserved other provider state and advanced the Beat change marker within the request window. The shared four-hour interval remained available. Returning to four hours removed the unlinked seven-hour interval.

The stock web service and one prefork worker were running. Beat was stopped. The updates admitted zero queue tasks, wrote zero new task-result commands, and preserved the startup results. Both owned role generations retired. The live run and independent evidence reduction each exited 0.

This reference covers the ordinary interval-only edit on an existing active Standard provider with an enabled linked schedule. Rust comparison and full M6 acceptance remain open. Original Dispatcharr, Celery and Django Celery Beat contributors retain full credit.

[Redacted machine-readable evidence](../scripts/rust-integration/m6-stock-std-interval-cadence.json)
