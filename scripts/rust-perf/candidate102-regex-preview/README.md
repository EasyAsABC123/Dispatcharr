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

## Request timing diagnostic

An instrumented image repeated the six phases with the same workload and
phase order. All 48 functional checks passed before measurement. The run
matched all 216 measured responses and verified cleanup. Diagnostic
full-scan p95 ratios were 0.774, 0.898, and 0.621 against the paired
Original phases.

The previously slow request in pair 2, wave 7 took 45.46 ms total with
timing enabled. Pair 3, wave 6 took 45.30 ms. The instrumented run did
not reproduce either spike, so the stage breakdown cannot establish its
cause. The original release result remains the governing result. The
same uninstrumented image repeated the existing request manifest and
budgets in the run below.

[Diagnostic receipt](timing-diagnostic.json)


## Exact release repeat

The uninstrumented Candidate102 image completed all six phases again.
All 216 responses matched with 360,360 scanned rows and 180 samples.
Median paired combined CPU was 22.61% lower, and peak simultaneous
working set was 55.67% lower. Completed throughput was unchanged.

| Pair | Original full-scan p95 | Rust full-scan p95 | 1.10 latency gate |
| --- | ---: | ---: | --- |
| 1 | 57.51 ms | 50.99 ms | Passed |
| 2 | 55.82 ms | 62.58 ms | Failed |
| 3 | 55.50 ms | 61.59 ms | Failed |

The two slow Rust requests were again in pair 2 wave 7 and pair 3
wave 6. Their delays occurred before the first response byte. The
independent verifier reconciled all measured work and verified exact
teardown. The release launcher exited 1 and M7 resource acceptance
remains open. The timing image did not isolate the cause.

[Independent repeat receipt](attempt2.json)
