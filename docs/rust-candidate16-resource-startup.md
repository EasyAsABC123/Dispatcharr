# Candidate16 resource startup failures

Original Dispatcharr and dependency authors retain full credit. These are
fixture failures before measurement, not application CPU or memory results.
The [compact observations](../scripts/rust-live-perf/candidate16-startup/observations.json)
retain actual exits, elapsed times, cleanup checks and immutable raw-result
hashes for the Candidate16 image.

Attempt1 exited 1 after 82.062 seconds. The mounted uWSGI configuration had
mode0600, making it unreadable to the actual role UID65532. Its original exact
stderr was not retained. The correction copies public support files with
readable modes, preserves their bytes and verifies configuration reads inside
the guest under that UID before starting roles. Bounded private role logs are
now retained before Docker teardown.

Attempt2 exited 1 after 82.265 seconds. The UID65532 configuration read passed
with matching bytes and mode0444. Actual stderr reported `unable to load
configuration from 666`: the optional uWSGI argument was supplied as separate
`--chmod-socket` and `666` tokens. The next correction uses
`--chmod-socket=666`. Workload order, budgets, useful-media requirements and
performance thresholds remain unchanged.

Both historical strict failures and their cleanup errors remain recorded.
Independent checks found both owned Docker projects empty and observer scopes
inactive after teardown. Neither attempt completed a measurement phase, and
neither establishes a performance gain. A corrected run must execute and
validate the entire declared paired workload separately.
