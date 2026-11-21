# Configured recurring recording rebuild

Fresh Docker Compose runs on pinned Original and Rust `1423e79b6090af2ba47a7516ff4b29f39e9222fa` each ran the saved two-day recurring recording rebuild through Beat and the default task queue. Rust used the opt-in Selected job and scheduler mode.

Each run completed four ordinary task publications and two recording refreshes. It replaced the same future recording slot, finished linked artwork, retired the obsolete booking and unused clock, and preserved the past same-rule recording and unrelated future booking. Both queues ended empty and all owned resources retired. The prior pinned Original witness was independently reproduced before this paired run.

The exact Rust workspace passed formatting, strict Clippy, and 1,522 tests with 814 ignored. The default worker and scheduler graph and comparative performance remain open.

Original Dispatcharr authors and all dependency contributors retain full credit.

[Redacted test receipt](../scripts/rust-integration/m6-1423e79b-configured-recurring-rebuild.json)
