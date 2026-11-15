# Counter sampler lifecycle

Original Dispatcharr and dependency authors retain application credit.

The two actual controls passed: cancellation before clock start, and one actual
counter tick followed by cancellation. Both workers exited0, closed all six
counter file descriptors, settled their pipes and disappeared by PID/generation.
The counter worker stays in the same charged observer cgroup. Thresholds remain
unchanged, including the 50ms clock gate.

This validates instrumentation lifecycle only. It does not validate the full
61-tick workload, high-viewer timing, equal media work or any performance gain.
The earlier campaign's clock failure and CPU misses remain failed.

To reproduce in a fresh output directory:

```sh
systemd-run --user --scope --quiet --unit dispatcharr-resource-counter-check.scope \
  -p CPUQuota=100% -p MemoryMax=128M -p TasksMax=32 -- \
  timeout --signal=TERM --kill-after=5s 25s python3 -B \
  scripts/rust-integration/milestone-batch28/instrumentation/check_lifecycle.py \
  --sampler scripts/rust-integration/milestone-batch28/instrumentation/sampler.py \
  --output /tmp/dispatcharr-counter-fresh
```
