# Core scheduler failover and HDHomeRun browser OPTIONS

The original Dispatcharr authors and contributors retain full credit for the application. See [upstream attribution](../ATTRIBUTION.md).

The ordinary Rust image `sha256:fd1a839e1fce4835039b7b079cd0929cd3c658261374cea2c7bbebf813fae3a9` was built from `9cf640701dacea07ea5f394ce7b18a6fa7c8c088`. Verification covered 2,157 exact application inputs and all 497 protected React and attribution inputs. Formatting, strict all-target Clippy on the affected crates, and the affected host suites passed. Those suites recorded 1,036 passes and 261 fixture-dependent skips. The build manifest and scoped proof hashes are in the [receipt](../scripts/rust-integration/m0-m6-9cf6407-compose.json).

## M0 fixed HDHomeRun browser OPTIONS

A fresh Native Compose run matched the retained Original observations for anonymous browser OPTIONS on `/hdhr/lineup_status.json` for the allowed and denied addresses. Both comparisons checked the pinned HTML shell, independent CSRF cookie and form token validity, status, headers, and body length. The Native run and independent cleanup passed. The result covers these two measured operations. Wider HDHomeRun roles, representations and routes still require their own evidence.

## M6 explicit Selected/Core warm failover

Two Rust Main processes used the opt-in Core scheduler with one PostgreSQL primary, session-affine database connections, working Redis and the ordinary pool size of eight. The first process held the only scheduler lock and ran one naturally due `celery.backend_cleanup` task. The peer responded as a standby with healthy receiver lanes and the expected HTTP 503 full readiness fence. After a normal stop of the settled leader, its PostgreSQL backend was gone and the same peer process acquired the scheduler role. A second naturally due cleanup ran on that peer. Both tasks reached `SUCCESS`, durable tick count progressed from zero to one to two, unrelated rows stayed unchanged, both processes exited zero and the exact Compose inventory retired. Redis socket snapshots and the actual outer broker command attributed each UUID to its publishing process. An earlier fixture attempt stopped on a shared Docker hostname and passed exact cleanup.

A separate pool-size-one run verified three real SQL-backed readiness requests while the Core lock remained on its own database session. One naturally due cleanup succeeded and both sessions retired. Interrupted publication, blocked database operations, fallback after a failed connection, and the default worker graph require independent evidence. This checkpoint makes no new performance claim.
