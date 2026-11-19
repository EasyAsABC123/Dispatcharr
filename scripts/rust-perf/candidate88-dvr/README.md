# Candidate88 completed DVR playback

Original Dispatcharr authors and contributors retain full application and
algorithm credit.

Candidate88 (`3c5063347ca6be710894e8d753110bc344dd4df2`) and Original
(`1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`) played the same owned recording
through the stock authenticated file route in Docker Compose. The
[paired functional gate](../../rust-integration/milestone-batch88/dvr-compose.json)
qualified the exact same image pair. The offline Compose network exposed no
host ports. Both backends used a private Unix-socket relay and the same mounted
recording, PostgreSQL and Redis data. The relay and load driver are outside the
measured backend, infrastructure and idle-origin totals.

Each phase warmed for 15 seconds and sampled for 60 seconds. Each viewer made
one complete request and one closed-range request per three-second wave. The
harness validated the response headers and verified all bytes against the
3,165,955-byte recording and the same 196,608-byte range. Recording tables and
sequences stayed unchanged and the owned origin remained idle in all seven
measured phases.

| One-viewer metric | Original median | Native median | Median paired change |
| --- | ---: | ---: | ---: |
| Mean whole-stack CPU, as a percent of one core | 0.722% | 0.410% | 45.05% lower |
| Peak simultaneous Docker working-set estimate | 393,314,304 bytes | 145,559,552 bytes | 62.99% lower |
| Delivered bytes per 60-second window | 67,251,260 | 67,251,260 | Unchanged |

All three one-viewer pairs delivered exactly 40 requests per phase and passed
the frozen latency, throughput, CPU and working-set limits. Paired changes
come from the median of the three Native-to-Original ratios.

At ten viewers the first Original phase delivered all 400 measured requests.
Native's first warmup wave returned six HTTP 503 responses to full requests.
Four full requests and their four range requests completed. The controller
stopped without sampling Native at ten viewers and exited 1. Its request
recorder captured status and timing. The concurrency pattern and the
four-permit writer semaphore on the exact candidate identify the point to
investigate. The run removed all six containers present at teardown and
verified absence of all 11 observed process generations, the network and
the volume.

The two [bootstrap attempts](bootstrap-attempts.json) stopped before sampling
and passed cleanup. The [attempt 5 reconciliation](attempt5.json) preserves
the actual exit, scope, budgets, per-phase hashes and the failed Native
warmup. Full completed-DVR performance and M4 remain open.

To reproduce the independent reconciliation from a retained private run
directory, its fixture seal and the actual exit recorded by the launcher:

```sh
python3 -B scripts/rust-perf/candidate88-dvr/verify_attempt.py \
  --run-dir "$run_dir" --binding-dir "$fixture_dir" --actual-exit "$actual_exit"
```
