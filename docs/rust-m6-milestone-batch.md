# M6 integrated job migration

Original Dispatcharr and dependency authors retain all application, algorithm and
license credit. These changes preserve the React interface and develop the Rust
backend implementation; they do not establish completed migration or new
performance gains.

The coordinator now exports 47 required M6 nodes: 46 behavior cells and one
combined prerequisite for previously unmeasured Source TVG/version behavior.
The matrix covers ingest/API descendants, default scheduled families, physical
failure/restart and a later full-service performance phase. Context ordering is
explicit in `depends_on`; owners do not create their own concurrency pools.
Missing implementations remain blocked. The HTTP202 and read-only FIFO probes
produce partial observations, never a passed complete workflow.

The per-channel TVG protocol, local parser, atomic programme swap, retained SQL
and cache operations, normal statistics port, explicit API-origin Refresh child
capability, family dispatcher and adapters are now registered Rust modules.
Main prepares, starts and drains the TVG application under the existing
`RUST_EPG_REFRESH_ENABLED=1` migration option, disabled by default. Natural
Channel-signal frames are processed by this worker, and unsupported FIFO heads
remain untouched. The existing Main empty-Series consumer still needs to be
replaced by the complete configured/parented workflow. Registration of the
fourteen-family dispatcher does not mean Main runs that graph.

Diagnostic checks passed for the workspace compilation and the Job library:
176 passed and 108 ignored. The first library attempt retained two sandbox OS
permission failures; the same full library suite passed with OS access. The
first all-target workspace strict check hit protected existing VOD drafts; the
relevant-crate attempt exposed two health-test type errors, subsequently
reported and repaired by that owner. The [final frozen common candidate](rust-milestone-batch1.md)
subsequently passed whole-workspace formatting, compilation, strict all-target
Clippy and872 default tests (751 ignored). Full milestone acceptance remains
separate from these SDK checks.

The grouped missing Source work is retained in
[`m6-missing-source`](../scripts/rust-job-contracts/m6-missing-source/manifest.json):

- Epoch 1 remains host exit 1. Six actual TVG tasks completed: initial mapped
  import, repeat, missing EPG, dummy source and unmapped EPG returned their
  observed normal results; a real insert trigger produced `ProgrammingError`
  with SQLSTATE `P0001` and preserved prior programme rows. The Original
  `AsyncResult`, raw backend metadata, task signals, SQL and before/after state
  are retained. A later version publication failed because the management
  producer had not imported `core.tasks`; no version request ran in this epoch.
- Epoch 2 ran only the four unmeasured version cases after normal producer task
  registration. Host and guest both exited 0. Four real HTTP200 dependencies
  produced normal SUCCESS/null results. No-update cleared the obsolete notice;
  update created the administrator-only `version-0.34.0` notice; repeat and
  malformed version data preserved that notice. The six TVG cases were reused,
  without application replay.

Both epochs preserved the full default SQL/schema/sequence and DB7 cache
observations, passed all eight cleanup steps, settled 27 CLI identities and one
container identity each, and normally stopped their Original worker with its
known process generations absent. The guest verified eleven pinned Original
module hashes before model/task execution. Observer tools were not frozen at
launch; current canonical recipe hashes must not be presented as observed
execution bindings. This limitation is preserved in the evidence manifest.

These Source observations establish reference behavior. Native paired execution,
configured descendant composition, source-refresh deferral, full default
scheduler graph, supported restart/recovery, broader provider imports and the
remaining matrix cells still need completion. The ambiguous DVR result owner
still requires owned process exit; this is not long-running recovery.
Performance must compare equivalent useful work across the complete service
boundary after functional acceptance. Historical live-stream gains do not apply
to this job candidate.
