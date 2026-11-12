# Explicit owned-flock release follow-up

Original Dispatcharr/Rust/Celery/Django and dependency authors retain code, algorithm and licensing credit. The prior spool-recovery770 packet remains historical and unchanged. This packet reconstructs canonical Float767 commit94c6389f8554f46967c7621bfae0870645b2cd28 plus ten exact scanner overlays into771; it does not replace newer Store/lib bytes or include Root's later stream observer. No Main/default enable or resource-performance claim.

The original Root parallel reacquire failure was untyped; its exact historical cause cannot be reconstructed from that assertion alone. A separate deterministic test confirms the mechanism: an owned child paused before exec retains an inherited CLOEXEC File descriptor, so a drop-only owner can close its original descriptor while the flock remains and typed reacquire returns Capacity. After exec, reacquire succeeds.

The normal runtime fix is a small LockedControl RAII guard, with no unsafe code. After scratch close/unlink and idle-slot sync, normal finish explicitly unlocks and propagates unlock error. Error exits also release the acquired lock through the guard. Active owners still exclude others. Released/stale Lease objects cannot write or create again. On SIGKILL, destructors do not run; inherited handles may retain a lock until they close, so no blind stale-owner cleanup is introduced.

The deterministic witness uses unsafe only inside cfg(test), with already-open descriptor read/write/close/getpid calls in pre_exec. There is no allocation/logging/mutex/panic in that closure. Parent readiness/write and cleanup waits are bounded; the owned child is released and reaped before assertions. Descriptor checks compare control dev/inode identity: raw FD numbers can be reused by concurrent tests. An initial predeclared stress attempt stopped at cohort7 on that QA mistake while runtime reacquire remained Ok; logs are retained. The corrected fresh50-cohort gate passed600 controls at8 threads, with no retry-until-green or policy change.

This does not expand bootstrap/power-loss/NFS/external-mutation/multi-application/M3U/EPG or packaged-app authority from the earlier packet. The monitor/replay summary records any remaining failed gate explicitly. See the previous spool-recovery README for all healthy-domain limits and Source259/Source217 distinctions.

Portable commands (requires Rust1.90, cached dependencies, pinned Original image and Docker Compose):

```sh
P=scripts/rust-job-contracts/scanner-logo/spool-recovery-unlock
python3 "$P/test_reconstruct.py" --repo .
python3 "$P/reconstruct.py" --repo . --output artifacts/rust/unlock-repro-source
python3 "$P/test_build_peer.py" --source artifacts/rust/unlock-repro-source
python3 "$P/test_runner.py"
python3 "$P/build_peer.py" --source artifacts/rust/unlock-repro-source --target artifacts/rust/target-unlock-repro --output artifacts/rust/unlock-repro-build
artifacts/rust/unlock-repro-build/dispatcharr_job_transport logo_scanner::spool:: --nocapture --test-threads=8
python3 "$P/run_pair.py" --repo . --support-root scripts/rust-core-perf/support --native-binary artifacts/rust/unlock-repro-build/dispatcharr_job_transport --reference scripts/rust-job-contracts/scanner-logo/helper-large/source-helper-large-4.json --task --output artifacts/rust/unlock-repro-task
python3 "$P/run_pair.py" --repo . --support-root scripts/rust-core-perf/support --native-binary artifacts/rust/unlock-repro-build/dispatcharr_job_transport --reference scripts/rust-job-contracts/scanner-logo/helper-large/source-helper-large-4.json --output artifacts/rust/unlock-repro-service
```

These new771 wrappers were actually executed:18 closure/corruption controls, exact reconstruction/freshbuild,12 scratch controls, and one portable task560 plus service544+4 cohort passed. Historical770 and all failed attempts remain unchanged.


## Retained diagnostic limitations

On the fixed original ELF, task560 and helper80+4 passed. The first service run passed the259-file effects but failed the unchanged10-second SQL-release drain control; the other three physical controls passed. That historical miss remains unresolved. A later activity-monitored service544+4 run passed but missed the brief lock in its conditional row observer, so it does not establish a retrospective cause. No host-pressure explanation is claimed as fact.

Root authorized exactly one fresh portable build/cohort with a stronger nonblocking copied-database observer. That cohort passed unchanged service544+4 and task560. The observer used103 snapshots, at most16 activity rows each, session-only50ms lock and250ms statement deadlines, and0.2-second polling. Total observed query overhead was0.07332112698350102 seconds, with zero diagnostic query errors; it recorded actual committed-row progress. This is functional diagnostic overhead, not a resource benchmark. It neither changes the native10-second budget nor explains the retained earlier miss. No additional green-seeking cohort was run.

The original controls and600 fixed parallel executions do not imply arbitrary scheduler/timing guarantees. Root's separate772 observer union and its new ELF require their own qualification; this source packet is exact771 and does not transfer those results.
