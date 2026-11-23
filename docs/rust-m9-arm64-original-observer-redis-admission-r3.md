# ARM64 Original initialization with healthy Redis

The third diagnostic ran the Original Dispatcharr setup process on Lappy under the existing 96 MiB memory cap, 86.4 MiB lifetime headroom limit, half-CPU quota, 32 PID limit and zero swap. Fresh Compose dependencies and normal migrations completed. Both allocator modes authenticated to the broker and results Redis database before setup. Redis stayed healthy through retirement.

| Observation | Default | malloc |
| --- | ---: | ---: |
| Configuration callbacks completed | 29 | 29 |
| Model callbacks completed | 29 | 29 |
| Ready callbacks completed | 3 | 3 |
| Cgroup lifetime memory peak | 96 MiB | 96 MiB |
| OOM kills | 1 | 1 |
| Observed swap | 0 | 0 |
| Setup process exit | 137 | 137 |

The saved callback trace crossed the headroom limit before entering the fourth `ready` callback, for Channels. That callback was still open when the kernel killed each process. The records establish timing and cgroup memory pressure. They do not identify which allocations caused the pressure. Initial allocator selection was observed in both modes. Both processes exited before end-of-run allocator verification.

The independent audit exited zero and verified 18 exact resource absences, successful owned stop and down, and empty final project inventories. The physical pair exited one and the observer headroom gate remains failed. The earlier [allocator diagnostic](rust-m9-arm64-natural-init96-default-and-malloc.md) remains a separate run. The reduced receipt retains the exact phase outcomes and seal without credentials or raw execution output. [Aggregate evidence](../scripts/rust-integration/m9-arm64-original-observer-redis-admission-r3.json)

The original Dispatcharr authors and contributors retain full application and React credit.
