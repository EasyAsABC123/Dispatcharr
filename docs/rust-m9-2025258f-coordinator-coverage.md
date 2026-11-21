# 2025258f release-candidate Compose coverage

Eleven of 127 required coordinator nodes passed on the exact 2025258f
application image. Execution, semantic evaluation, and independent cleanup
all exited 0 for every counted node. Scoped controller invocations exited 2.
The first package launcher exited 1 before Docker started. A corrected launcher
built the same frozen revision.

- Clean install passed all 11 checks and Guide Grid passed all 43 wire comparisons.
- Live React playback advanced with the successful Stats request and 112 matching static assets.
- The 18-suite Main baseline passed at least 1,024 checks and decoded at least 75 frames.
- Safe17 and credential hash upgrade passed on the same package.
- The provider action, cached provider information, and React provider modal passed.
- The shipped createsuperuser workload and its dependent 154 typed comparisons passed.

The full release matrix, long soak, strict rollback, and matched resource
comparison remain open. [Redacted coordinator receipt](../scripts/rust-integration/m9-2025258f-coordinator-coverage.json)

The original Dispatcharr authors and dependency contributors retain full
credit. [Attribution](../ATTRIBUTION.md)
