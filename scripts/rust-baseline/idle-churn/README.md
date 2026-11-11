# Original idle/churn baseline controller

Original Dispatcharr authors and contributors retain all application/algorithm
credit and original licenses.

[The short observer contract](verified-observer-contract.json) passed18 offline
controls, ten real observer-probe samples, nine complete three-service censuses,
one authenticated2MiB reader/124-frame decode and owned cleanup. It ran on a busy
host to test the observer only. No Native service, comparative result, full
three-repetition baseline or M0 acceptance is claimed.

The four controller/policy files are byte-identical to that actual execution.
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
