# Candidate88 viewing evidence controls

Original Dispatcharr authors and contributors retain full application and tool credit.

The verifier uses the completed measurement JSON, the frozen workload, its
18 phase JSON receipts, the retry queue and the actual launcher log. It
checks the exact hashes, six settled phase groups and cleanup, decoded work,
and three paired resource comparisons. Keep the original evidence directory
layout when relocating these files.

```bash
run=artifacts/rust/m3-successor-viewing10-1
python3 -B scripts/rust-perf/candidate88-viewing/verify_measurement.py \
  --run "$run/candidate88-measured10" \
  --queue "$run/candidate88-retry-queue.json" \
  --launcher-log "$run/candidate88-viewing10-2.log"
```

The wrapper patch renames two metric locals in a fresh copy of the historical
launcher. Bind that copy before a future run. The measurement schedule,
limits and physical evidence are unchanged. Five offline controls exercised
the old failure, the corrected successful result, the pilot, input drift and
a failed CPU budget.

[Measurement report](../../../docs/rust-milestone-batch88.md) · [Compact evidence](../../../docs/rust-candidate88-live-performance.json)
