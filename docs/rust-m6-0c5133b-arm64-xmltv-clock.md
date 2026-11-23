# M6 current native ARM64 XMLTV clock qualification

The exact ordinary `0c5133b` ARM64 package passed the direct-origin
clock and XMLTV fixture on Lappy. The signed T+5 request returned 202.
The refresh root, ProgrammeIndex child, and Series child all succeeded
with the expected lineage. The disposable PostgreSQL source, programme,
and index were verified, and the Redis queue and locks drained.

One direct reader stayed active for the full window. Five synchronized
roles each captured 61 samples. The maximum cross-role span was
10.188 ms against the 50 ms limit. The controller, XMLTV observer,
and relays exited 0. Exact Compose cleanup passed.

This qualification used zero application viewers. The separate paired
ten-viewer timing and resource comparison remains open. Original
Dispatcharr, its React frontend, and its dependencies retain their
existing attribution and licenses.

[Machine-readable receipt](../scripts/rust-integration/m6-0c5133b-arm64-xmltv-clock.json)
