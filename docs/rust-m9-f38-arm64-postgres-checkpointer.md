# F38 ARM64 PostgreSQL checkpointer descriptor diagnostic

The follow-up owned F38 S360 diagnostic identified the PostgreSQL checkpointer in the shared baseline. It held zero relation-file descriptors before viewing and at +210 seconds, 31 at +240, 135 at +295, 207 at +335, and 381 after formal idle. The other three observed PostgreSQL background roles kept their original file counts. Growth was visible before the +300 guide refresh. No checkpointer WAL, temporary, or outside-data descriptors grew. [Typed evidence](rust-m9-f38-arm64-postgres-checkpointer.json)

The physical campaign and cleanup passed. Formal verification exited 1 at the unchanged 64 MiB idle-memory gate. The diagnostic reducer also originally exited 1 because its per-sample role accumulator was uninitialized. A file-only replay initialized that accumulator, verified the exact captured files and original audit exits, and reproduced the full count-only projection. It started no second workload. The earlier S360 descriptor qualification and the S7200 gate remain open. Checkpointer identity and file types establish the source of descriptor growth in this diagnostic. The memory gate has separate evidence.

Original Dispatcharr, PostgreSQL, and all other dependency authors retain full credit.
