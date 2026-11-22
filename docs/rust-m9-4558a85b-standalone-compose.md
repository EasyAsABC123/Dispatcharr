# M9 standalone Compose checks on 4558a85b

Six of 127 required workflows passed on the ordinary AMD64 package for
`4558a85bce631feddad3fed05e07aaf2b0fc58cb`. The checks used the packaged
production entrypoint and the unchanged React application.

| Compose run | Verified workflow |
| --- | --- |
| Static Chromium | Static HTML browser |
| Unified HTML | Unified HTML browser |
| Category | Category browser |
| Output Profile | Output Profile management |
| Management | Channel Logos and User Agent management |

Each run completed workflow execution, semantic evaluation, and independent
cleanup successfully. Each partial coordinator exited 2. The five immediate
wrappers and the independent combined reducer exited 0.

This checkpoint records six passes, 19 prepared workflows awaiting physical
execution, 101 blocked workflows, and one retained Original reference. The
six fresh passes apply to this image. Performance measurement and full M9
acceptance remain open.

The [machine-readable matrix](../scripts/rust-integration/m9-4558a85b-standalone-matrix.json)
records all 127 rows, the exact image identity, and the raw receipt hashes.

The original Dispatcharr authors and dependency authors retain full credit for
the application, models, algorithms, and protocols.
