# Actual guide task names

Original Dispatcharr and dependency authors retain application, algorithm and
licensing credit. React remains unchanged.

Main30 exposed a production parser mismatch: actual Source and Native guide
publications use `apps.epg.tasks.fetch_sd_mapped_guide_batch` and
`apps.epg.tasks.fetch_sd_guide_for_epg`. The hook parser accepted a fabricated
`apps.epg.sd_tasks` namespace, rejected the real claimed child before host setup,
and faulted the queue. Live health, queue and SQL observations preserve this
failure; later guide jobs could not reach terminal results.

The two-path correction accepts the actual names and adds a regression through
the real parser using both captured Source wire frames. It retains task/body/
identity constraints and rejects the old namespace and malformed frames.

[SDK41](../scripts/rust-integration/milestone-batch31/checks.json) passes formatting,
workspace check, strict Clippy and workspace tests: **1,099 passed, zero failed,
783 ignored**, across 180 blocks. The snapshot binds 1,408 inputs including 1,402
Rust-tree files. The failed Main30 run and cleanup remain separate evidence.

Docker, the corrected whole application workflow and performance remain
unverified here. This is not a fix claim for the earlier RegisteredPlugin
shutdown failure and closes no full milestone.
