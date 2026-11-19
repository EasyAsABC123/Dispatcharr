# Candidate91 completed DVR playback

Original Dispatcharr authors and contributors retain full application,
workflow and algorithm credit.

Candidate91 (`695779244a9dffb477ecb885ebbb19791d3a5021`, image
`sha256:4a287f12d9d0831ccb4b89eddcedcb8a037fa02930641356daa58713373d18fa`)
and Original `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` played the same local
completed recording through the stock authenticated file route. The
[paired functional fixture](../../rust-integration/milestone-batch91/acceptance.json)
passed on the exact image before measurement. Both backends used the same
PostgreSQL and Redis data, recording bytes, and private Unix-socket relay.
The measured service group includes the application and its descendants,
common PostgreSQL/Redis and the idle origin. Relay, load driver, daemon and
kernel costs are outside those container totals.

Three pairs at each viewer count alternated Original/Native, Native/Original,
Original/Native. Each phase ran five warmup waves over 15 seconds and 20
measured waves over 60 seconds. Every viewer made a complete GET followed by one
closed seek. The [request manifest](request-manifest.json) freezes the same
method, authentication form, status, headers, byte counts and hashes for
both backends. Each one-viewer phase delivered 40 exact requests and every
ten-viewer phase delivered 400. All 2,640 measured requests and 360 resource
samples reconciled. The owned origin remained idle and recording SQL rows
and sequence state stayed unchanged in every measured phase.

| Viewers | Resource | Original median | Native median | Median paired reduction |
| ---: | --- | ---: | ---: | ---: |
| 1 | CPU as a percent of one core | 0.727% | 0.414% | 41.75% |
| 1 | Maximum simultaneous working-set estimate | 390,455,296 bytes | 136,962,048 bytes | 64.34% |
| 10 | CPU as a percent of one core | 5.654% | 2.001% | 64.62% |
| 10 | Maximum simultaneous working-set estimate | 410,472,448 bytes | 170,209,280 bytes | 58.88% |

The reduction column uses the median of three paired Native-to-Original
ratios. Each backend delivered 67,251,260 bytes per one-viewer window and
672,512,600 per ten-viewer window. At ten viewers, the medians of the
full-file p95 totals were 127.917 ms Original and 66.656 ms Native.
All six pairs stayed at or below Original CPU and simultaneous working set,
delivered at least 95% of Original throughput, and met the 1.10 p95 limit
with a 50 ms floor for both total and first byte on each operation.

The actual launcher exited 0 after 12 valid measured phases and six passing
pairs. Cleanup verified empty container, network and volume inventories,
including the six owned containers present at teardown, and retirement of
all 15 observed process generations. The preceding [Candidate88 attempt](../candidate88-dvr/README.md)
retains its actual exit 1 and six HTTP 503s from the first Native ten-viewer
warmup wave. Its measured one-viewer comparisons remain separate.

The [attempt 6 reconciliation](attempt6.json) includes the exact phase hashes,
timings, workload counters, limits, raw outcome digest and cleanup counts.
The published packet omits private request logs, fixture names, local paths,
and process or container identifiers. Given the retained private run and
binding directories, the existing verifier reproduces the public projection.

```sh
python3 -B scripts/rust-perf/candidate88-dvr/verify_attempt.py \
  --run-dir "$run_dir" --binding-dir "$fixture_dir" --actual-exit "$actual_exit" \
  | cmp - scripts/rust-perf/candidate91-dvr/attempt6.json
```

Use the actual launcher status from the retained run. The result covers
warm completed DVR file playback on this exact image. Live streaming,
active recording jobs, HLS performance and full M4 remain separate.
