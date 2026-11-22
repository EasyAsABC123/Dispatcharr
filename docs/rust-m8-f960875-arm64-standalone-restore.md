# Standalone Native PostgreSQL restore on ARM64 f960875

The original Dispatcharr authors and contributors retain full credit for the application. See [Attribution](../ATTRIBUTION.md).

On November 22, 2026, Lappy's native ARM64 Docker Desktop engine ran the exact ordinary `f960875` image with the standalone Native backup restore owner explicitly selected. The fixture downloaded a 36,652-byte current-schema PostgreSQL archive containing synthetic data. It submitted the restore to a fresh isolated target and kept an independently seeded control database on a separate named volume.

The signed HTTP result and its corresponding Redis record both reported completion. The restored database matched the expected SQL rows, the restored account logged in with HTTP 200, and the replaced credential returned HTTP 401. The independent control and the source archive retained their original hashes. The probe, Native process and controller all exited 0. The fixture verified exact retirement of six containers, one network and two volumes, and the corresponding project label queries returned empty inventories.

The earlier attempt reached the signed HTTP terminal state and exited 1 at its Redis observer. The retry used the Native transport's `celery-tasks:` prefix in isolated Redis database 2. It also required exactly one Celery-style result for the owned task. Application and image bytes were identical across attempts.

The [machine-readable receipt](../scripts/rust-integration/m8-f960875-arm64-standalone-restore.json) binds the results to the exact ARM64 package. The [earlier Original and Native backup pair](rust-m8-f960875-arm64-backup-inventory-download.md) covers inventory and download. A matched Original restore, ordinary AIO restore, migrations from other schema versions, plugin backups and comparative performance remain outside this standalone test scope.
