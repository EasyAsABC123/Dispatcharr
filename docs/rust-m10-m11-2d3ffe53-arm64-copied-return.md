# M10 and M11 current ARM64 copied-data return

Lappy ran the pinned Original backend and the ordinary ARM64 Rust package at `2d3ffe53` against disposable PostgreSQL copies. The drill exercised 14 local ingress requests and exactly one acknowledged Rust channel rename. After Rust stopped, SQL showed zero remaining writer sessions. Two independent cold restores preserved all five logical checkpoints. Original read the acknowledged rename using the same signed session. The Compose owner and independent audit passed, including exact retirement of 16 containers, six volumes and one network. [Redacted physical audit](../scripts/rust-integration/m10-2d3ffe53-arm64-copied-return.json)

The offline intake verified the current package and the physical audit, and its result replayed byte-for-byte. Default-switch approval remains gated by formal M9 acceptance, the release configuration and rollback plan, canary and recovery targets, operator authorization, and monitoring and fallback-retirement evidence. The completed drill covers local same-schema copied data. [Offline decision](../scripts/rust-integration/m11-2d3ffe53-arm64-offline-intake.json)

Dispatcharr’s original authors and the authors of its dependencies retain full credit. [Attribution](../ATTRIBUTION.md)
