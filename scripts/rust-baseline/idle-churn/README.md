# Original idle/churn baseline controller

Original Dispatcharr authors and contributors retain all application/algorithm
credit and original licenses.

[The short observer contract](verified-observer-contract.json) passed18 offline
controls, ten real observer-probe samples, nine complete three-service censuses,
one authenticated2MiB reader/124-frame decode and owned cleanup. It ran on a busy
host to test the observer only. No Native service, comparative result, full
three-repetition baseline or M0 acceptance is claimed.

That earlier four-file controller/policy version is retained in commit
`671a8ff0e781d7e8bfd5fbbb882488f0f473548c`. The current denied-coverage
correction below has a separate execution proof; its full policy is unchanged.
`captured-review-seal.json` records the original review, including its historical
README hash; this public README documents the same runtime without changing it.
The proof retains failed permission, code-mount and census-completeness attempts.
Same-UID read-only subprocesses observe PostgreSQL and other owners without
adding application capabilities. PID/UID coverage and races remain explicit;
TCP tables are counted once per container and socket inodes unioned across UIDs.

```sh
python3 -m unittest discover -s scripts/rust-baseline/idle-churn \
  -p test_controls.py -v
python3 scripts/rust-baseline/idle-churn/run.py --repo . \
  --output artifacts/rust/m0-observer-smoke-NEW --observer-contract-smoke
```

Use Python3, Docker Compose, Linux cgroup v2, the pinned Original image and the
repository's committed support history. Outputs must be new directories. The
controller freezes itself and imports the sealed copy before execution. It uses
only owned synthetic PostgreSQL/Redis/origin/Source services and checks their
actual images and limits. Tokens remain in memory; census collects no command
lines or environments. Cleanup checks the exact owned project.

## Full declared Source run

Coordinate an otherwise idle host before using `--execute-original-only` instead
of the smoke flag. Competing builds, tests, fixtures, samplers and cleanup would
invalidate isolated interpretation. This command is a separate experiment,
requires a fresh output, and has not yet earned full acceptance.

The fixed policy declares three fresh repetitions. Each has20s idle before any
playback, one10s reader, cleanup/70s retention settlement/20s idle, three1-reader
and three10-reader rounds with10s holds, then cleanup/70s settlement/20s idle.
No primer, retry, backend restart or Redis reset occurs within a repetition.
Each saved prefix is exactly2MiB and must decode audio/video with at least25
frames within five output seconds. The earlier512KiB/0.5s draft failed on one
of four retained Original captures before baseline execution; that evidence was
preserved when selecting the separately Source-qualified bounded decoder.

All source policy thresholds remain explicit: minimum five complete stable
samples per20s idle, zero errors/OOM, fixed startup/rate/cleanup limits and fixed
memory/process/thread/fd/socket growth budgets. Incomplete, cleanup, retention
settlement and transition samples remain recorded and are excluded from settled
idle acceptance. The short smoke's two-complete-sample functionality check does
not replace the full idle minimum.

The full group is Source uWSGI/Daphne, shared PostgreSQL/Redis infrastructure and
deterministic origin, with inspected finite limits. This is the declared fixture
configuration, not the full default worker graph. The concurrent-import baseline
must separately account for its actual worker. Observer overhead is retained.
Whole cgroup counters/working memory and process census are different measures;
no allocator/RSS or long-running leak guarantee follows from reservation limits.
The controller always keeps `m0_closed` and baseline-acceptance fields false;
an independent parent review must qualify the resulting evidence.


[The first complete three-repetition attempt](verified-original-three-repetitions-negative-1.json) remains failed: all102 readers/18 workload phases pass, two repetitions fail fixed process/FD/socket growth limits, and one loses observer coverage after a denied proc-FD read. The latter cannot support growth conclusions. Independent owned cleanup passes. Original thresholds and failed evidence remain intact; observer correction/qualification is a separate step.

## Permission-denied observer coverage

The sealed first three-repetition Original run remains failed: repetitions1 and3
exceeded process/FD/socket growth budgets, while repetition2 lost later stable
coverage after a real proc fd-link PermissionError. Aggregate service/UID census
counts do not establish per-PID causality or a Source leak. Those results and the
original policy remain authoritative.

The separately qualified observer correction records denied processes/descriptors
and marks their merged census incomplete while continuing sampling. Denied rows
do not count toward the unchanged stable-sample minimum; persistent denial still
fails coverage. Unrelated command, parse and OS errors remain fatal. No application
capabilities or credentials change. The short Source contract smoke includes the
existing one-reader churn phase after idle/playback, and is functionality only.
It does not replace the separate full three-repetition quiet run.

`kernel_denial_control.py --output NEW_DIRECTORY` is a separate synthetic kernel
control, not a Source baseline. It makes only a bounded owned helper non-dumpable
inside a networkless64MiB/.25CPU container with all capabilities dropped. It
requires real denied samples to stay incomplete and five later stable complete
samples after helper exit. It reads the hash-checked public
`scripts/rust-guide-perf/docker_stats.py` from committed support revision
`2fbeabcd69802ef82c7dcb2be277d622e6eba7df`; no ignored private artifact is needed.

README/history updates are documentation only and remain distinct from the exact
executed runtime/controller hashes. The earlier observer and failure history above
is retained. No full M0 acceptance follows from these controls.

[The correction qualification](verified-observer-denial-contract-1.json) passes28 controls, the Source smoke and a separate public-path real-kernel denial/recovery control.
