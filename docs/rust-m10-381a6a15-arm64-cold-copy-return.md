# 381a6a15 ARM64 cold logical return

The exact ordinary ARM64 package for `381a6a157d1f860527906794bb8c63859fcc8176` passed a disposable same-schema cold logical return on Lappy against Original Dispatcharr `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`. The fixture, preflight, run, proof, physical owner and independent read-only audit each exited `0`.

A Source-issued signed session remained valid through one Native rename and the subsequent cold Original read. Native changed one row’s `name` and `updated_at`, then stopped with exit `0` and no remaining writer SQL sessions. Two fresh cold copies restored from the stopped database. The audit checked ordered row and sequence state across 62 tables and 501 columns and matched five checkpoint and restore files byte for byte. It also recomputed the semantic checkpoint digest.

The same ingress recorded 14 requests, including 10 signed requests and eight with the Source-issued session. Cold Original read the acknowledged rename without an Original reconciliation write. Both cold starts recorded the same four bounded timestamp effects. Other inspected startup state and the signed post-ready read and stop remained exact within their respective comparisons.

Cleanup verified exact retirement of 16 container IDs and reserved names, one network by ID and name, six named volumes, empty scoped inventories, and removal of temporary Compose and archive files. Database contents and credentials stayed on Lappy. The physical receipt SHA-256 was `2e9f54e5c50ba28f4e726c959e65153be5c327fc4b5db6755b5c3b58a1e03232`.

This result qualifies the local same-schema return for the exact candidate and fixture. Mixed-schema downgrade, production rollout and formal M10 operator approval remain open. The original Dispatcharr and React authors and contributors retain full project credit. PostgreSQL, Django, Redis and other dependency authors retain credit for their projects.

[Machine-readable evidence](../scripts/rust-integration/m10-381a6a15-arm64-cold-copy-return.json)
