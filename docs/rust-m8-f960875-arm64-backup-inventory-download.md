# Backup inventory and download on native ARM64 f960875

Original Dispatcharr authors and contributors retain full credit. See [upstream attribution](../ATTRIBUTION.md).

Two isolated Lappy projects verified first-party administrator JWT authentication, the inventory of one owned PostgreSQL backup, exact download bytes, and an unchanged second inventory read. Original downloaded 36,545 bytes. The exact [ordinary Rust ARM64 image](rust-m10-f960875-native-arm64-docker-desktop.md) downloaded 36,575 bytes. Each matched its own separately generated stock version 2 ZIP, containing a PostgreSQL custom dump named `database.dump`. Rust's download status and Content-Length matched its seeded archive.

Both controllers and both probes exited 0. Each project verified its captured container, network, and volume retirements and empty project inventories. Earlier nonzero attempts identified Docker Desktop wait, resource absence, and header-observer assumptions. The retry preserved the actual archive and application assertions.

This scope covers backup inventory and download. Restore, background jobs, performance, and whole M8 remain open. [Machine-readable evidence](../scripts/rust-integration/m8-f960875-arm64-backup-inventory-download.json)
