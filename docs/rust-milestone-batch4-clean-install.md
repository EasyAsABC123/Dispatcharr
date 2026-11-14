# Combined candidate: unchanged React install and restart

Original Dispatcharr and dependency authors retain full application credit and
licenses. This execution uses the unchanged React UI with only the Rust backend.

The [actual integration result](../scripts/rust-integration/milestone-batch4-clean-install/integration.json)
passes all11 checks on committed runtime `9440a31973978e6c8efe144912525c59fa3f260f`:
fresh database, disabled initialization without DDL, native-only service set,
Original-compatible schema, absent Python runtime, first-admin form, actual React
creation/login, persisted admin row, browser tokens after restart, unchanged
schema/admin after restart and closed setup after creation.

Execution, cleanup and semantic evaluation exit zero, with both EOFs and process
groups settled. The isolated project is removed. Root independently checked the
raw result and execution/evaluation records; their exact bytes are included in
the [inventory](../scripts/rust-integration/milestone-batch4-clean-install/inventory.json).
The cohort took19.135seconds; that is functional runtime, not a performance result.

The outer dispatcher exits2 because other required matrix nodes were unbound.
This complete clean-install cohort passes; no full migration milestone or gain
is inferred. The affected job/settings workflows use the same
[committed Docker image](rust-milestone-batch4-build.md).
