# Native ARM64 HDHomeRun lineup across a planned restart

Lappy ran the ordinary native ARM64 package for `5fdaaba33be23b0d3d4677f2df96db05080f10d8` in a disposable Compose project with a synthetic channel. The package initializes PostgreSQL and Redis before Main reports HTTP 200 readiness. The same Main container stopped with code 0, started a new process generation, and again reported HTTP 200 readiness.

Before and after restart, `GET /hdhr/lineup.json` returned HTTP 200 and the exact same 185-byte JSON lineup. The React index returned HTTP 200 and the exact same 858-byte HTML. The fixture compared persisted SQL values and confirmed the same React index and assets across the restart. The observer ran in the ARM64 guest as UID 65532. Exact container, network and volume teardown passed, and the runner exited 0.

The [aggregate receipt](../scripts/rust-integration/m7-5fdaaba-arm64-hdhr-restart.json) identifies the image, source map and tested responses. The scope is one Rust package, one lineup and one planned restart. Paired Original comparison, broader M7 and M9 workflows and performance remain separate requirements.

Original Dispatcharr and dependency contributors retain full credit.
