# Ordinary AIO role startup on f960875 ARM64

Original Dispatcharr authors and contributors retain full application credit. See [upstream attribution](../ATTRIBUTION.md).

The exact [ordinary ARM64 package](rust-m10-f960875-native-arm64-docker-desktop.md) started with `DISPATCHARR_ENV=aio` and no ownership overrides. The internal readiness request returned 200. Both selected receivers were active, the control channel subscribed, and Core Beat reported an observed tick.

Seven persisted schedule contracts matched, including disabled Stats and the UTC cleanup schedule. The file scanner’s saved run count advanced from zero to one. Redis recorded the Version and recording-recovery startup dispatch locks. These observations establish the role, scheduling, and dispatch scope. Downstream completion, external Version retrieval, plugin work, and the longer schedules remain open.

The controller exited 0. The application, init, database, and cache containers each exited 0, and the four containers, network, and volume retired. The fixture used the published package with both SQLite opt-ins disabled. [Machine-readable evidence](../scripts/rust-integration/m10-f960875-native-arm64-aio-role.json)
