# Candidate56: bounded DVR terminal transport diagnostics

Original Dispatcharr and dependency authors retain all application, protocol
and algorithm credit. React and the original license remain unchanged.

Commit `29b5f4ba6224c688e0d00f6b74ce465f1fd6d8ff` adds optional diagnostics
at the DVR result writer's actual command boundary. Logs use fixed stages,
error kinds, task UUIDs and elapsed time, with at most 64 records per process.
They distinguish a logical deadline from disconnect, I/O, decoding,
authentication and server errors, including a late reply/error after the
observer times out. Request payloads, credentials and error text are excluded.
Existing timeout, retry and ownership behavior is preserved. A returned Redis
invocation does not establish that a remote mutation applied or all owners
have drained.

[SDK90](../scripts/rust-integration/milestone-batch56/SDK/index.json) passes
formatting, workspace check, strict Clippy and workspace tests: **1,200 passed,
zero failed and 786 ignored**, across 180 result blocks. Its two new controls
exercise retained ownership after logical timeout and fixed error classification.
All 1,452 delivered Rust-tree inputs match the tested snapshot and published Git
commit; the SDK has 1,458 inputs total. Vendored original manifests remain.

The [Docker build](../scripts/rust-integration/milestone-batch56/build/index.json)
passes in **314.814 seconds**, producing image
`sha256:4c0855d94dbe9b2b7625066416f5bd7f89bfa115fd43b51662cefca5e2190cd4`
and source-tree SHA256
`9c7735dfa52d6d9c6404b569410b9c7bf994b493bf3146f65da693a3705ab13d`.
[All six package families](../scripts/rust-integration/milestone-batch56/package/inventory.json)
pass in **26.228 seconds**, with 55 exact containers independently absent.
The inherited archive report's Candidate54 ELF label actually compares the
published Candidate55 ELF; its [label note](../scripts/rust-integration/milestone-batch56/package/report-label-note.json)
preserves that distinction without rewriting the raw report.

The focused automatic-recording diagnostic is running with the diagnostics flag
explicitly enabled. It reuses unchanged Candidate55 Python fixture support;
the current image and all 1,452 Rust inputs are bound to SDK90. The earlier
[Candidate55 diagnostic](rust-milestone-batch55.md) reproduced a missing long
recording parent result and a DVR transport fault, while its child succeeded.
Candidate56 adds observation of the cause; no repair or application acceptance
is claimed yet. The prior PostgreSQL resolver gate remains component evidence
for unchanged resolver code, not a new SDK90 PostgreSQL execution.

Account, profile creation/deletion and EPG mutation implementations are separate
drafts based on the newly captured Source lifecycle. They are not included in
this image. Whole migration milestones M0–M9 remain open; no new application
CPU or memory gain is established by these build and package times.
