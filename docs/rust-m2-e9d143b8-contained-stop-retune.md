# Cross-worker MPEG-TS stop and retune on e9d143b8

Fresh pinned Original and the ordinary Linux AMD64 Rust package at `e9d143b8`
ran the same two-worker, same-channel fixture. Two initial viewers shared
one origin connection and one provider slot. An authenticated stop on the
other worker ended both viewers and retired the initial Redis stop marker
and client identities.

Retuning that channel established fresh ownership on the opposite worker,
with one new origin connection and one provider slot. The new owner kept
serving when its local viewer disconnected. All eight decoder paths across
the two tune generations and two backends decoded audio and video. Final
Redis and origin state retired. Both Rust worker images were verified
against the exact ordinary package manifest.

Preflight, physical execution and the strict reducer each exited 0. Five
owned Compose roles and their exact host process generations retired. The
project's container, network and volume inventories were empty. The original
fixture receipt served as workload authority. Both results here are fresh.
[Scoped receipt](../scripts/rust-integration/m2-e9d143b8-contained-stop-retune.json)

This evidence covers the contained stop and opposite-worker retune portion
of `Main/lifecycle-fault-workflow`. The full lifecycle run, M2 acceptance
and comparative performance remain open.

The original Dispatcharr and framework contributors retain full credit.
[Attribution](../ATTRIBUTION.md)
