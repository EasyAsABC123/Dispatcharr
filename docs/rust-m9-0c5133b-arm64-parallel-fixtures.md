# M9 ARM64 parallel fixture evidence

On 2026-11-23, Lappy ran three independent Docker Compose projects against the same packaged Dispatcharr Rust backend. A serial Original browser control passed first. The application revision was `0c5133b62dc9f2df0900f2228e25b75baecb6754`, the comparison revision was `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`, and the ARM64 package manifest SHA-256 was `750aed6d8c6912ab4e891cd1fee132838fc14a7e7cecdc724c70aeeb7e10d3fb`.

| Physical scope | Verified outcome | Runner exit |
| --- | --- | ---: |
| Original browser control, serial | Normal first-admin setup, sign-in, authenticated render, account dialog, and sign-out completed. Migration and settings checks passed. | 0 |
| Rust browser | The same six browser steps and migration and settings checks passed. | 0 |
| Rust M3U and XMLTV | Authenticated requests returned 200 for both formats. Requests without credentials returned the qualified 401 wire responses. All four cases passed. | 0 |
| Rust HDHomeRun planned restart | One lineup and the React index returned the same bytes before and after a normal stop and start. SQL state matched and the application process generation changed. | 0 |

Two physical Docker inspections observed the same three Rust application generations and separate data roles, with **608.478334 ms** between the end of the first inspection and the beginning of the second. The live projects had 10 distinct container IDs, four network IDs, and three volumes. Each retained its own Compose project, database, Redis instance, and volume. The coordinator then released the three fixtures to make their test requests. The overlap qualifies simultaneous fixture readiness on this host. The HDHomeRun scope covers one planned restart. Full M9 acceptance and comparative performance remain open.

The batch and independent audit both exited 0. The four worker ledgers verified retirement of 21 containers, and the final coordinator verified all four project inventories were empty and each named or observed resource was absent. The earlier batch `batch-0dd986fc1821` exited 1 before triple overlap, with exact cleanup. Original Compose startup and migration-role inspection both exited 0 in that attempt. The failed historical admission predicate remains unknown.

The qualifying batch ID is `batch-d28bd726d780`. Its exact receipt SHA-256 is `d149d2a75e50b74d71aaa9f86da76fe5fc8bf663576b39690c1915d7b38dbce1`. The [machine-readable audit summary](../scripts/rust-integration/m9-0c5133b-arm64-parallel-fixtures.json) records each physical receipt and the overlap sample digests.

The original Dispatcharr authors and contributors retain full credit for Dispatcharr and its existing React frontend.
