# Release of a held SQL checkpoint during Core shutdown

Original Dispatcharr authors and contributors retain full application credit. See [upstream attribution](../ATTRIBUTION.md).

On ordinary `9cf6407`, the opt-in Core leader published one naturally due cleanup and then waited on the isolated test transaction during its SQL checkpoint. The test sent `SIGTERM` to that exact process and released the owned row inside the shutdown budget. The leader finished its checkpoint and exited zero. Its database session retired, the same standby acquired the role, and the next naturally due cleanup ran once. Both durable counts and the two publishing identities matched the scoped assertions. Exact cleanup passed.

The first attempt observed the expected database wait and then stopped because its observer compared two spellings of the same timestamp. The retry used normalized timestamps and passed. A dependency failure, an ambiguous broker reply, and a row held beyond the shutdown budget retain separate gates. [Machine-readable evidence](../scripts/rust-integration/m6-9cf6407-sql-release.json)
