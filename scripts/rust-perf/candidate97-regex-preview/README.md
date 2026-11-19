# Auto Sync batching and repeat benchmark

The original Dispatcharr and dependency authors retain full workflow and
algorithm credit. Candidate97 (`fdebfb97615f9c4a8e81f29b1dacebfca91f6950`)
reduced a typical 5,000-name scan from ten cross-process frames to three
while keeping the prior worst-case input size, scan cap, request concurrency
and deadline. The exact Docker image preserved all 497 React, license
and attribution inputs. The fresh Compose contract passed all 48 checks
including 30 paired workflow checks. Formatting, three focused Store tests
and strict Store Clippy passed.

The [same request manifest](../candidate94-regex-preview/request-manifest.json)
ran across six 60-second measurement phases with 15 seconds of warmup each.
Both backends completed identical work for all 216 responses and 360,360
scanned rows. Median paired CPU and peak simultaneous working set fell
18.08% and 55.42% in the measured application, database, Redis and idle
origin boundary. Throughput matched. The full-scan p95 still exceeded the
1.10 limit in two pairs.

| Pair | Original full-scan p95 | Rust full-scan p95 | Passed all budgets |
| --- | ---: | ---: | --- |
| 1 | 62.19 ms | 47.92 ms | Yes |
| 2 | 56.49 ms | 66.12 ms | No |
| 3 | 55.64 ms | 66.71 ms | No |

The benchmark exited 1. The independent comparison recomputed all budgets
and verified teardown of all nine observed process generations, with the
owned resource inventory empty. M7 performance acceptance remains open.
[Independent repeat receipt](attempt1.json) · [Candidate94 results](../candidate94-regex-preview/README.md)

The offline verifier accepts the local captured `--driver`, `--functional-runner`,
`--preflight`, `--build`, `--functional-receipt`, `--attempt`, `--actual-exit`,
`--actual-log` and `--quiet-authorization` artifacts. Use `--help` for their
roles. It reuses the pinned Candidate94 raw comparator.
