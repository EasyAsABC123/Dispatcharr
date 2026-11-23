# ARM64 observer callback attribution at 256 MiB

A fresh Docker Compose diagnostic on Lappy completed Original Django setup with a 256 MiB observer cgroup, half a CPU, 32 PIDs and zero swap. The campaign and independent audit both exited 0. The trace recorded 180 events, including 29 app-config creations, 29 model imports and 29 ready callbacks. It restored the hooks and confirmed disposal of the setup process. The diagnostic used the same historical application and observer scope as the [96 MiB attribution run](rust-arm64-publication-observer-96mib-attribution.md).

The kernel recorded 119,918,592 bytes (114.36 MiB) at setup and through disposal. The separate uninstrumented 256 MiB run reached 115,249,152 bytes (109.91 MiB) at setup and 115,470,336 bytes (110.12 MiB) through disposal. Cache, boot and guide differences prevent attribution of the difference to the trace hooks alone.

The Channels ready callback completed in this run. Cgroup charge rose by 15.06 MiB between its endpoints. The summed endpoint changes across the Channels intervals reached 25.31 MiB. These measurements include any memory charged elsewhere during those intervals. They do not identify exclusive or retained allocations.

The 96 MiB configuration and its 86.4 MiB headroom requirement remain in force. This larger diagnostic establishes a complete callback trace and leaves the formal resource gate open. It made no application changes and provides no comparative Rust performance measurement.

[Audited aggregate receipt](../scripts/rust-integration/m9-arm64-observer-callback-attribution-256mib.json)
