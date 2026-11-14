# First combined milestone integration candidate

Original Dispatcharr authors and dependency contributors retain application,
algorithm and licensing credit. React and original attribution/license files
are unchanged.

The selected candidate combines 32 Rust paths against the preceding
`b7e738595e094b186fa5b9cc1ff8342b44de440f` revision. Its exact input map contains
904 Rust files and five coupled timeshift vectors, SHA256
`e4a29d7ae5a364537a0f6f21b086cf97fa196a177d52954c66af1adcaab2bb1c`.
The [recorded checks](../scripts/rust-integration/milestone-batch1/checks.json)
and [review](../scripts/rust-integration/milestone-batch1/review.json) retain
commands, actual exits, timings and raw-log hashes.

Whole-workspace formatting, locked/offline compilation, strict all-target Clippy
and default tests all exit0. The suite reports **872 passed, zero failed and
751 ignored**, across172 result blocks. Ignored tests remain unexecuted external
fixture coverage. One shared Cargo target was reused in place. Existing VOD
work-in-progress was preserved and excluded from this clean candidate. An earlier
combined snapshot failed compilation on missing shutdown-field plumbing; that
failed attempt remains recorded, with the corrected candidate checked afresh.

Main now composes the bounded unsigned PluginRepo API with the same AuthService
admission context. The existing enabled EPG graph also prepares, starts and drains
the per-channel programme worker instead of leaving those natural FIFO frames
unsupported. The new registered job adapters retain explicit missing-family and
lineage restrictions. This does not complete configured Series or the full
default job graph.

The [health adapter](rust-proxy-health-consumer.md) copies three settings values
and follows measured Base-first inheritance and cold TS cache expiry. It remains
behind the documented migration switch while exceptional settings and actual
live consumer behavior await application acceptance. Core warm reads reuse their
existing token; cache timestamps are sampled before loading, matching Source.

The milestone batch registry groups shared package workflows and required owner
cells. New Source observations cover cold/expired settings, per-channel EPG and
version checks, and plugin transport/signature behavior; each keeps its own
capture, cleanup and failure limits. A component or Source observation does not
establish production Main acceptance.

The [immutable Docker build and installed-file checks](rust-milestone-batch1-build.md)
now pass. Next: bind the package/workflow runners to that image, run the functional matrix and
repair failed/dependent workflows. Complete functional acceptance precedes paired
whole-service performance, soak and rollback. No new CPU/memory gain or completed
M0–M9 milestone is claimed here. Historical image57 measurements retain their
original scope.
