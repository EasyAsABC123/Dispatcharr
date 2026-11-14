# Candidate6 committed Docker build

Original Dispatcharr and dependency authors retain all application, algorithm,
protocol and license credit. The React frontend is unchanged.

The [actual build](../scripts/rust-integration/milestone-batch6-build/build.json)
passes on runtime `b7dafc1a586c6f1c5a466349ca5afa0f54639b14`, producing image
`sha256:4a6b7ffab1bb9cbd316ae54f4ca18fcd5d476e5b7971286c49ffc82fd1446afe`.
The source-tree digest is
`a65637934a6a801909250072ddfe57699c41ea0bf1d630fdfa4b455b44163be6`.

Independent probes verify all 1,671 archived inputs byte-for-byte against Git:
1,174 Rust inputs, 495 React inputs and two original credit/license files. The
Rust inputs match the [908-test SDK batch](rust-milestone-batch6.md). The actual
production entrypoint is the Rust ELF, running as UID65532 from `/app`; its hash
is `523e528f64b034c193625d04121b7dbb35c0ee3f72835cbfcb6b83730c408cf0`.

All 114 earlier React/original-credit hashes remain unchanged. The probe also
verifies the Celery/Kombu notices and both original licenses for each retained
SQLx package, for 120 selected asset/notice hashes. Separate physical probes
verify the pinned GPG 2.4.8 runtime and 27 crypto/trust/owning-license files.
Every owned probe container is absent after completion.

One preparatory invocation exited2 before Docker or compilation because Root
had placed probe scripts in the builder's required empty output directory.
Those scripts were moved to a separate preparation directory before the actual
successful build. That failure is retained, not counted as an application run.

Raw commands, actual exits, build output and probes are indexed in the
[inventory](../scripts/rust-integration/milestone-batch6-build/inventory.json).
This is build and packaging evidence. It does not pass Main workflows, complete
migration milestones or establish a performance gain; those require their
separate grouped Compose results.
