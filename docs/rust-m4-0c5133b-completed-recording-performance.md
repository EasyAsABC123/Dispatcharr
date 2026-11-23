# M4 completed-recording performance on AMD64

The ordinary completed-recording Docker Compose comparison passed at one and
ten viewers for Rust application `0c5133b62dc9f2df0900f2228e25b75baecb6754`
and Original Dispatcharr `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`. Original
Dispatcharr, its React frontend, and its dependencies remain credited to
their respective authors under the existing attribution and license.

| Viewers | Exact requests per arm | Bytes per arm | Rust CPU reduction | Rust peak working-memory reduction | Throughput |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 40 | 67,251,260 | 42.6% | 64.0% | Equal |
| 10 | 400 | 672,512,600 | 64.1% | 58.0% | Equal |

Reductions use the median Rust-to-Original ratio across three paired trials
at each viewer count. Arm order alternated between pairs. Each arm ran on
the same quiet AMD64 host with a 15-second warmup, 60 seconds of measurement,
fixed three-second request waves, and 30 resource samples. The resource
observer included the backend and descendants, shared PostgreSQL and Redis,
and the idle synthetic origin. Docker working memory is the maximum
concurrent working set across those services. The driver and private relay
were outside that accounting.

Each viewer requested the same 3,165,955-byte full recording and a
196,608-byte closed range on every wave. All measured responses matched
the qualified 200 or 206 status, selected headers and body digest. SQL and
offline state checks passed. Each pair met the throughput, CPU, memory
and p95 gates for first byte and total time. The p95 ratio uses a 50 ms
comparison floor and an upper bound of 1.10. The receipt includes the raw
p95 measurements alongside the floored ratios. The campaign exited 0,
retired all 15 owned container generations, and verified that its Compose
project retained no containers, networks, or volumes.

This scope covers ordinary full reads and closed seeks on a completed
local recording. Growing recordings, other range forms, live timeshift,
session lifecycle cases, and full M4 performance acceptance remain open.

[Machine-readable receipt](../scripts/rust-integration/m4-0c5133b-completed-recording-performance.json)
