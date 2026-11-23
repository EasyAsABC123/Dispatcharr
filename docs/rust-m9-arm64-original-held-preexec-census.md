# ARM64 Original held pre-execution census

The pinned Original ARM64 container remained in Docker's created state with PID 0 before the cut, after the cut and during teardown. The observer used the separate disposable PostgreSQL and Redis services. Its SQL transaction verified the owned database and read-only state. A single authenticated Redis DB 0 connection verified its server generation and observed all eight empty priority queues, zero unacknowledged entries and index records, and zero result or unexpected priority keys.

The same observer cgroup recorded **42,074,112 bytes (40.125 MiB)** through the post-disposal collector, below the **86.4 MiB** headroom limit under the **96 MiB** cap. Swap and memory-limit or OOM events were zero at that boundary. The independent audit verified the measured generation and preserved the later observer termination as non-OOM. The measured interval ends at the cut and its disposal readout.

The physical owner and independent audit both exited zero. Stop and down passed, and a fresh read-only check confirmed the four container IDs and reserved names, one network and one volume absent. Both project and active-engine rosters were empty. This scope verifies a held Original container and empty census. The active-worker barrier, signed application read, result policy and complete M9 remain open. The [Native held-census result](rust-m9-0c5133b-arm64-no-django-held-census.md) has its own image and observation boundaries. [Aggregate evidence](../scripts/rust-integration/m9-arm64-original-held-preexec-census.json)

The original Dispatcharr authors and contributors retain full application and React credit.
