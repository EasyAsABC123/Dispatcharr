Candidate45 scoped live resource evidence

Original Dispatcharr authors and contributors retain full application and algorithm credit. FFmpeg, VLC and dependency authors retain credit for their work.

The actual 12-phase study completed with valid useful media in every phase. Five of six pairs meet the preset comparison budgets. **The study fails its CPU budget:** one viewer, trial 2 has Native/Source CPU ratio **0.9053066339**, above **0.90**. No threshold changed, no failed pair was discarded or rerun, and no performance gain or full milestone is accepted. The geometric mean does not override that failed pair.

Source revision, candidate image and frozen inputs are bound in summary.json and frozen-workload.json. Candidate45 is revision `7c75cb07ba08544c903b9db953d00b2309e40f1b`, image `sha256:09bebef535ebfa571db5950af063b706a613cb559cb7720dd54824384ffd788b`, validated by SDK62 and the six package families. This study has three alternating Source/Native pairs each at one and two viewers on the same shared VLC-input MPEGTS stream. The normal service graph is loaded, with Jobs/Beat and consumers idle. Each phase uses 15 seconds of warmup, a fixed 60-second measurement and bounded five-second drain. Active recordings, recovery, 10/100 viewers, GPUs and the complete migration remain outside this measured scope.

Primary accounting includes **all backend/infrastructure services plus Origin**, summed at each sample tick. External observer/orchestrator/publication containers and scopes are reported separately, with whole-fixture totals also retained. **Inside-container instrumentation remains charged to primary:** sampler.snapshot() invokes two Docker Compose exec redis-cli EVAL children per second inside infrastructure. Their CPU and Redis query processing are included without subtraction; these are not isolated application-only CPU measurements. CPU is cumulative cgroup usage difference divided by actual measurement duration. Working set is the sum of `max(0,memory.current-inactive_file)` per group, an estimate rather than RSS; peak means the maximum simultaneous measurement tick. Charged memory includes file cache. We never sum lifetime peaks.

The following are arithmetic averages of three trials' absolute primary measurements; memory columns average each trial's simultaneous peak. CPU percentage is percentage of one core.

| Viewers | Backend | CPU seconds / ~60s | CPU, one core | Working set MiB | Charged memory MiB |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 | Source | 4.12508 | 6.87491% | 899.32 | 1015.07 |
| 1 | Native | 3.71203 | 6.18650% | 262.11 | 377.83 |
| 2 | Source | 4.21872 | 7.03105% | 898.05 | 1013.80 |
| 2 | Native | 3.71786 | 6.19613% | 262.03 | 377.76 |

Across all six pairs the descriptive geometric mean Native/Source ratios are CPU **0.89050533**, working-set peak **0.29161442**, delivered bytes **0.99604593**, encoded useful bytes **0.99620600**, and first-byte p95 **0.95318863**. Actual first-byte p95 averages are approximately 7.098s Source and 6.766s Native; this is the declared percentile over one or two viewers, not a large-sample latency distribution. The preset 50ms floor is inactive for these observations. All individual ratios and per-service-scope absolute metrics, including substantial observer cost, are in summary.json.

Decoded useful video-duration spread across all phases/viewers is 0.440s, audio 0.426656s; both are below the independently indexed one-batch PTS limit 2.005333s. Complete-media qualification includes full raw and certified decode receipts and per-stream complete-unit counts; raw media remains in the local evidence archive. The sampler's `useful_work_passed=false` is intentional: sampler.py defers decoded/indexed qualification off-window to the separate passed useful-media report. It is preserved, not relabeled.

Independent review recomputed every phase metric directly from raw per-group cgroup counters, then every pair ratio and geometric mean, matching the original controller. Read-only cleanup verifies **42 exact container IDs, 12 project container/network/volume inventories, and 466 distinct captured positive host PIDs absent**. These include the initial process inventories, VLC, samplers, phase controllers and shared orchestrator. Every original phase controller exited 0 with both EOFs and settled process group. The original failed qualification pilot and its valid retained Source phase remain separate historical evidence; no pilot phase enters this measured 12-phase study.

`raw/` contains deterministic gzip copies of the byte-exact resource samples and complete-media reports, whose uncompressed SHA256 values are bound in summary.json. Full shared-ts raw reports remain local and hash-bound; first-byte timings are independently also present in published samples. Recompute without apps, Docker or media work:

```bash
python3 -B scripts/rust-integration/milestone-batch45/performance/recompute.py
```

Next steps are to attribute application versus inside-container sampling CPU, explain the CPU budget miss without changing its historical result, address broader functional/recovery domains, and qualify the remaining declared workloads before any milestone-wide resource or migration claim. The current measurements, comparison boundaries and budgets remain unchanged.
