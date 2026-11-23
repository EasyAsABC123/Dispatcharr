# M11 ARM64 offline fallback intake on 8fc7eef0

The offline intake verified the genuine cold-copy rollback for exact ARM64 `8fc7eef0`, its package and configuration, and the restored checkpoint. The rollback had already verified 14 ingress requests, including ten signed and eight using the Original-issued session, one acknowledged Rust rename, zero reconciliation writes, and exact retirement of 16 containers, one network, and six volumes.

The exact receipt and restore-manifest equality checks passed. Six controls rejected stale evidence from `390ad8aa`, two substitutions for the receipt, restore-manifest drift, missing retention approval, and an incomplete operator packet. The intake exited 0 and made zero engine calls.

The fallback component awaits release approval and approved retention. Formal M9 acceptance, M10 operator handoff and rollout, compatibility with the proposed release configuration, and operator sign-offs and monitoring remain open. The offline intake qualifies the recorded ARM64 specimen.

Original Dispatcharr, React and dependency authors and contributors retain full credit. [Attribution](../ATTRIBUTION.md) · [Machine-readable evidence](../scripts/rust-integration/m11-8fc7eef0-arm64-real-fallback-intake.json)
