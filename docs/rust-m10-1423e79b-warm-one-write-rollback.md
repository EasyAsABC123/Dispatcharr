# Warm PostgreSQL rollback with one acknowledged write on 1423e79b

The exact packaged Rust image for `1423e79b6090af2ba47a7516ff4b29f39e9222fa` ran against owned disposable PostgreSQL copies. A single loopback front door served Original, then Rust on its copy, then the same warm Original generation. Rust acknowledged one channel rename and retired before reconciliation. Four Original observations matched before the replay. [Exact copied-state receipt](../scripts/rust-integration/m10-1423e79b-warm-one-write-rollback.json)

The same signed Original session replayed the acknowledged rename and read it back. The paired SQL comparison attributed the result to that channel's name and update timestamp. Unrelated rows, schema and sequences matched. The donor, Original control, and warm trial retired all nine observed containers, with empty owned inventories. The controller and its terminal proof each exited 0.

This copied-state rehearsal leaves the earlier strict cold observer's exit 1 intact. It provides no additional formal M9 count, and production rollout and comparative performance remain open. Full credit belongs to the original Dispatcharr authors and dependency contributors. [Formal matrix](rust-m9-1423e79b-formal-matrix.md) · [Original attribution](../ATTRIBUTION.md)
