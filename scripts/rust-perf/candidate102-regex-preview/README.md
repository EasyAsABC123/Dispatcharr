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

The frozen protocol checks p95 total latency and time to first byte for
each request class in every pair. Each ratio applies a 50 ms floor to both
measurements and must be at most 1.10. The CPU, peak working set and
throughput budgets are separate requirements. The full-scan class has 12
requests per phase, so its nearest-rank p95 selects the slowest request.

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

## Shifted request timing

The exact uninstrumented image completed all 216 requests under a
diagnostic schedule. The two affected Native phases started their request
waves ten seconds later while keeping five-second request cadence and the
two-second observer grid. The largest Native latencies in those phases
were 54.28 ms and 48.38 ms. They were 8.17 ms and 2.17 ms above their local
medians, below the diagnostic's 10 ms excess criterion. Both original
spikes were absent and the clock comparison is inconclusive.

The controller exited 0 for diagnostic capture and teardown. This run
used a different schedule from the release protocol. The two failed
release attempts remain the governing evidence.

[Shifted timing receipt](shifted-clock-diagnostic.json)

## Sample allocation experiment

A separate image retained result samples only while the requested sample
limit had room. It ran the same regex calls on all input rows. The 48
ordinary and 50 wide-Unicode functional checks passed before measurement.
The six measured phases again matched all 216 responses, 360,360 scanned
rows and equal throughput.

| Pair | Original full-scan p95 | Rust full-scan p95 | 1.10 latency gate |
| --- | ---: | ---: | --- |
| 1 | 64.73 ms | 47.04 ms | Passed |
| 2 | 55.59 ms | 65.64 ms | Failed |
| 3 | 55.84 ms | 65.19 ms | Failed |

Median paired combined CPU was 21.65% lower and peak working set was
55.84% lower than Original. The controller exited 1 after exact cleanup.
The independent reducer confirmed these results. The experiment leaves
Auto Sync resource acceptance open.

[Sample allocation experiment receipt](allocation-experiment.json)

## Dedicated worker concurrency experiment

The separate candidate ran the same regex operations with Python's
`concurrent=False` in its dedicated worker. The pinned environment
produced identical results and timeout errors, and the packaged image
passed the ordinary and wide-Unicode suites. The six measured phases
matched all 216 responses at equal throughput.

| Pair | Original full-scan p95 | Rust full-scan p95 |
| --- | ---: | ---: |
| 1 | 52.15 ms | 50.32 ms |
| 2 | 60.46 ms | 60.88 ms |
| 3 | 55.25 ms | 62.53 ms |

A low-priority contract test overlapped about 24.5 seconds of Original's
second measured phase. The relative CPU and memory observations in this
capture remain diagnostic. The third pair missed the latency limit and
the launcher exited 1 with verified cleanup. The same assembled image
is queued for a clean repeat under the unchanged request plan.

[Concurrency experiment receipt](concurrency-experiment-first.json)

## Quiet repeat of the dedicated worker experiment

The same assembled image repeated the unchanged workload in a reserved
window. All 216 responses matched, with 360,360 scanned rows and equal
throughput. Median paired CPU was 24.68% lower and simultaneous peak
working set was 54.95% lower than Original.

| Pair | Original full-scan p95 | Rust full-scan p95 | 1.10 latency gate |
| --- | ---: | ---: | --- |
| 1 | 67.72 ms | 46.38 ms | Passed |
| 2 | 56.46 ms | 65.27 ms | Failed |
| 3 | 55.58 ms | 60.27 ms | Passed |

The largest Rust requests in pairs 2 and 3 again occurred in zero-based
waves 7 and 6. The second pair exceeded the total-latency limit by 5.61
percentage points. The first-byte ratio also exceeded the limit in that
pair. The launcher exited 1 and exact cleanup passed. M7 resource
acceptance remains open.

[Independent quiet repeat receipt](concurrency-experiment-repeat.json)
