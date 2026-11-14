# Current candidate: native fresh install and React restart

The Docker image `sha256:8e6a871b7a810f56bf14326d2b7ee90c0a7d7d0ba61c49900afd79af9d27315d`
passes the existing complete native fresh-install workflow through the global
milestone dispatcher. All11 assertions pass, including actual unchanged React
first-administrator creation, login, initialization and token persistence across
restart. Original Dispatcharr authors retain full credit and licenses.

The Compose fixture contains PostgreSQL17, Redis, the Rust candidate and data
initialization, with no Django/Python service. The official empty database starts
with no users; Rust initializes62 tables/184 migration records, React creates the
real administrator, and restart preserves the schema/user. All16 required browser
API paths succeed both before and after restart, with no browser exceptions.
Chrome156 and Node22 run the unchanged frontend.

[Raw workflow evidence](../scripts/rust-integration/milestone-batch1-clean-install/integration.json)
and the [dispatcher result](../scripts/rust-integration/milestone-batch1-clean-install/result.json)
retain the exact image/source binding. Execution, independent cleanup and semantic
evaluation all exit0, both streams reach EOF and owned process groups settle.
Exact project-container/network/volume inventories are empty after cleanup.

The full127-cell dispatcher exits2: this cell passes, one retained Original17
authority is reused without replay, and125 required cells remain blocked. That
global exit is preserved; this workflow does not close full parity or a milestone.
The approximately19.4-second execution is a functional-run duration, not a paired
performance measurement. No CPU/memory gain is claimed.
