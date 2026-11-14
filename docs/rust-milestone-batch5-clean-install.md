# Candidate5 React install and restart

Original Dispatcharr and dependency authors retain all application credit and
licenses. The React frontend is unchanged.

The [actual Compose result](../scripts/rust-integration/milestone-batch5-clean-install/integration.json)
passes all11 checks on runtime `95da37d9f0bd6dcca5f678d114bdac994de9b8de`
and the [candidate5 image](rust-milestone-batch5-build.md): fresh PostgreSQL,
default-off initialization without DDL, native-only services, compatible schema,
absent Python runtime, first-admin form, actual React creation/login, persisted
admin row, browser tokens after restart, unchanged schema/admin after restart,
and setup closed after creation.

Execution, cleanup and semantic evaluation exit0, with both EOFs and process
groups settled. The isolated project is removed. Root independently checked all
11 raw assertions and execution records. Exact bytes are indexed in the
[inventory](../scripts/rust-integration/milestone-batch5-clean-install/inventory.json).
The cohort took19.243seconds; this is functional timing, not a performance gain.

The first attempt failed at the sandbox's AF_INET bind probe before Docker or
application startup. Its failed execution and missing-result cleanup/evaluation
are retained under `pre-app-failure`; the authorized continuation passed.
The outer dispatcher exits2 because other required matrix nodes were unbound.
This complete clean-install cohort passes; full migration milestones remain open.
