# Shared Redirect assignments and plugin runtime engineering

Original Dispatcharr and dependency authors retain full application, media,
algorithm and licensing credit. The React frontend remains unchanged.

This candidate resolves Redirect requests from the actual Source channel/stream
aliases, distinguishes borrowed assignments from owned reservations, and uses
current database credentials without allocating another provider slot for a
borrowed assignment. Ordinary native proxy owners publish those aliases from
their existing live lease. Failover and retirement use the existing ownership
ledger; VOD and preview identities cannot masquerade as ordinary channels.
The existing raw owner, counter and connection budgets remain authoritative.
Foreign Source aliases have no epoch; stronger ABA guarantees are not claimed.
Inverse Redirect-to-proxy and wider preview behavior remain open.

The same batch compiles concrete plugin runtime infrastructure: lazy owned
interpreter actors, same-session SQL compatibility, cache/events ports, loaded
callable identity, bounded shared results, causal writer ownership and real
nonextending shutdown deadlines. It does not activate enabled plugins in Main.
The generic worker, actions, host packaging and transaction/child-process
acceptance remain active engineering work. A module registration is not a
working enabled-plugin deployment.

All [four SDK gates](../scripts/rust-integration/milestone-batch25/inventory.json)
pass: formatting, workspace compilation, strict Clippy and workspace tests.
The batch binds1,350 inputs (1,344 Rust) and43 changed paths. There are1,061
passing tests, zero failures and775 ignored tests across177 result blocks.
All seven new Redirect Redis controls pass in the joined actual physical batch. Earlier
attempts exposed Result return typing, a missing owned deadline method and
strict lint issues; their failures remain recorded before the passing revision.

The committed `6612ec14757bdfb7f31a3e546ed9a6cbd8a7ad7d` builds as
`sha256:3a07c5ac8ccf415e4bd0b016bbdc7d364234d11617e518ad74dc1eae61319d22`.
All [package checks](../scripts/rust-integration/milestone-batch25/package/)
pass. All1,841 archived files match Git (1,344 Rust,495 React,two credits).
Original author credits, dependency notices, UID65532, production entrypoint,
crypto/PostgreSQL/regex dependencies and paired useful-core VLC remux pass.

The [joined physical batch](../scripts/rust-integration/milestone-batch25/physical-output/report.json)
passes all eighteen controls in one owned Compose namespace: output-store7,
profile lifetime/ownership2, observed stop1, fMP4 generation1 and Redirect7.
The host SDK controllers and exact ELF dependencies are explicitly identified;
this is component acceptance inside the candidate environment, not Main execution.
All six containers, the project and original Redis generation settle. The
qualified multi-keyframe fMP4 fixture is unchanged.

The [Source-first Main batch](../scripts/rust-integration/milestone-batch25/functional/report.json)
exits1 after1,309.397 seconds with all six Native contexts passing. Both backends
pass SD9, XMLTV4, live VLC sharing, auto5, compact3/API6 and all five common
regex cases. Native also passes recording-profile4/output6. Actual paired regex
names and absolute numbers match after reconstructing the same causal state.
Source's first two Redirect checks sample after their original parent finishes:
a guest settings save does not invalidate the serving process's ten-second
config cache, so both validation requests wait on the fixture's held URL.
The original false report remains unchanged; no Rust behavior change follows.

The corrected complete Redirect5 context keeps validation enabled for both
requests, holds only the first nonce request, and asserts the parent is still
pending at the snapshot. It passes on both backends with the unchanged committed
image, exits0 after181.894 seconds and settles all five containers. The original
batch settles all eleven containers. Both runs verify project/process/CLI cleanup.
The selected Candidate25 workflows are qualified across these named executions;
full milestones and resource acceptance remain open. It reexecutes Source compact/API as causal state prerequisites for the
later paired regex comparison; their prior passed leaves alone do not restore
numbering reservations. Source24 recording-profile authority is reused at its
original scope. Enabled-plugin physical proof and paired resource measurement
remain required. Historical Candidate24 results do not accept this candidate. No full
M0–M9 milestone or new application performance gain is accepted.
