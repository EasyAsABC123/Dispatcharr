# Candidate102 Auto Sync resource comparison

Dispatcharr's original authors and contributing dependency authors retain
full workflow and algorithm credit. Candidate102 at
`7b23e5fa27ba618e476c1b6cde465234cf30b639` packs the bounded ordered scan
into one PostgreSQL array. The [functional results](../../../docs/rust-milestone-batch102.md)
record the exact container identity and the ordinary and wide-Unicode tests.

The [fixed request manifest](../candidate94-regex-preview/request-manifest.json)
ran in six 60-second phases after 15 seconds of warmup each. All 216
responses matched, with 360,360 total scanned rows and 180 samples. The
measurement boundary included each backend and its subprocesses, the common
PostgreSQL and Redis processes, and the idle origin.

| Measure | Original median | Rust median | Median paired change |
| --- | ---: | ---: | ---: |
| Combined CPU, percent of one logical core | 1.4412% | 1.1377% | 20.45% lower |
| Peak simultaneous working set | 391.28 MiB | 171.95 MiB | 56.05% lower |
| Completed response throughput | 151.2 bytes/s | 151.2 bytes/s | Unchanged |

| Pair | Original full-scan p95 | Rust full-scan p95 | 1.10 latency gate |
| --- | ---: | ---: | --- |
| 1 | 72.44 ms | 49.44 ms | Passed |
| 2 | 56.40 ms | 67.53 ms | Failed |
| 3 | 57.17 ms | 63.68 ms | Failed |

The launcher exited 1 after all six phases. Independent reconciliation
recomputed the work and budgets, verified retirement of nine observed process
generations, and found the owned Compose inventories empty. M7 resource
acceptance remains open because pairs 2 and 3 exceeded the fixed p95 limit.

[Independent receipt](attempt1.json) · [Prior Candidate97 comparison](../candidate97-regex-preview/README.md)

The verifier binds the exact Candidate102 build and uses the existing fixed
reducer. It accepts a local capture with `--driver`, `--functional-runner`,
`--preflight`, `--build`, `--functional-receipt`, `--attempt`, `--actual-exit`,
`--actual-log`, and `--quiet-authorization`.
