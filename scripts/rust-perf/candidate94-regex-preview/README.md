# Auto Sync preview paired performance on Candidate94

Candidate94 is `fc5562a37d91ed6673e6ee9a740be4e0dd6c7c5f`. The test reused
its [exact Compose contract](../../rust-integration/milestone-batch94/acceptance.json)
and verified the same status and content for each response. Three alternating
pairs ran with 15 seconds of warmup and 60 seconds of measurement per phase.
The request manifest includes the 5,000-row find, replace, match and exclude
preview, an account-scoped preview, and a count-only preview. Each phase
completed 36 requests and scanned 60,060 rows. The full run captured 180
samples across all six phases.

The measured boundary included the active application and its subprocesses,
PostgreSQL, Redis and the idle fixture origin. The request driver and private
relay ran outside the measured cgroups. Median paired CPU usage fell 16.72%
and peak simultaneous working set fell 56.36%. Throughput was equal in all
three pairs. The full-scan p95 exceeded the allowed 1.10 ratio twice.

| Pair | Original full-scan p95 | Rust full-scan p95 | Passed all budgets |
| --- | ---: | ---: | --- |
| 1 | 56.03 ms | 51.12 ms | Yes |
| 2 | 58.54 ms | 67.66 ms | No |
| 3 | 55.03 ms | 68.08 ms | No |

The controller exited 1. The independent verifier reconciled the request
counts, response hashes, raw resource samples, latency and all nine observed
container process generations. The owned container, network and volume
inventories were empty after teardown.

[Attempt 1](attempt1.json) ended during preparation with no measured phases.
[Attempt 2](attempt2.json) confirmed Original startup had queued a new task
in this disposable fixture after initial broker preparation. [Attempt 3](attempt3.json)
checked the exact expected startup task before clearing the fixture queue
and verified the same SQL state and idle broker before every warmup.
`verify_attempt.py` validates the local raw capture when given the launcher
and run directories and the actual exit file. The [request manifest](request-manifest.json)
is unchanged across attempts.
