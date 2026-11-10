# Stored programme read contracts

All application and serializer credit belongs to the original Dispatcharr
maintainers and contributors. The React source and original licence remain.

The [host Source/native gate](verified-program-http-8.json) passes37 actual
JSON/HEAD HTTP comparisons: list/detail, ignored query parameters, leading-zero
identifiers, missing/invalid IDs, standard/basic/inactive/anonymous access, JSON
format, unsupported XML Accept, and trusted forwarded poster origins. Separate
SQL projection compares53 source/data/programme row and collection-order cases.
The captured104 Source HTTP cases are not104 native HTTP passes.

`EpgProgramReads` uses the original programme serializer projection and
select-related join. Every successful body owns its read-capacity permit through
`Bytes::from_owner`; clones and slices retain it until the last allocation
reference disappears. JSON serialization writes into a capped buffer. SQL gates
oversized text/metadata before returning those columns to the client decoder.
A worker owns its permit through query and transaction rollback even if its
observer cancels or reaches its HTTP deadline. A local statement timeout bounds
blocked database statements; workers do not release admission on caller abort.
Shutdown closes admission and reports whether physical work and responses drain.

Four real-resource scenarios pass: retained body clone prevents read admission,
aborting a caller while PostgreSQL is blocked retains physical admission,
aggregate JSON capacity rejects a response, and a2MB stored row is rejected
before transfer. Pure tests additionally cover sliced byte ownership and JSON
escape expansion. These are bounds/ownership checks, not throughput or memory
performance measurements.

The new app route has configurable limits:

| Environment variable | Default | Allowed |
| --- | --- | --- |
| `RUST_MAX_EPG_READ_RESPONSES` |4|1–16|
| `RUST_EPG_READ_RESPONSE_BYTES` |16777216|128–134217728|
| `RUST_EPG_READ_ROW_BYTES` |1048576|128–4194304|
| `RUST_EPG_READ_TIMEOUT_MS` |2000|100–30000|

Capacity exhaustion and payload/deadline limits return503. Requests above these
bounds are deliberate resource-limit behavior, not captured Source parity.
Large populated guides, playback during guide reads/refresh, delayed database
network replies and failure/shutdown recovery need their own Compose workloads.
No full M5 or application performance acceptance follows from this gate.

Browsable HTML remains an explicit501 guard. The complete Accept/format grammar,
OPTIONS metadata, programme writes/poster images, guide grid/current/search and
unchanged React guide workflows remain separate implementation/test gates.
`probe_programs.py` replays the37-case JSON/HEAD family against a packaged URL,
including the HEAD representation length. `control_reads.py --candidate-image
IMAGE --build-manifest BUILD_JSON --output DIR` provisions original PostgreSQL,
Redis and the immutable candidate through Compose and validates build provenance.
Add `--native-http` for the separate frozen host and physical-resource tests.

The first host run is retained as `m5-epg-program-http-7`: the resource fixture
looked for the relation name after PostgreSQL had truncated the long query text
in `pg_stat_activity`. It now identifies the known query prefix. Retry8 passed
with unchanged runtime bytes and stricter exact503 assertions.
