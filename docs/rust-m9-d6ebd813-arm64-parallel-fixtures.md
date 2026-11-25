# M9 ARM64 parallel fixture evidence at d6ebd813

On November 25, 2026 UTC, Lappy ran three independent Docker Compose fixtures against ordinary ARM64 Dispatcharr Rust revision `d6ebd8134ed19a7792fefce069e100ba220b29c7`. A serial Original browser control at revision `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` passed first. The ordinary package manifest SHA-256 was `b1253d761cf553b22eefe7da69ebe8d4df9e38c83974335d62c07f650fa9d090`.

| Physical scope | Verified outcome | Exit |
| --- | --- | ---: |
| Original browser control, serial | First-admin setup, sign-in, authenticated render, account dialog, and sign-out passed. Migration and settings checks passed. | 0 |
| Rust browser | The same six browser steps and migration and settings checks passed. | 0 |
| Rust M3U and XMLTV | Authenticated requests returned 200. Requests without credentials returned 401 with the exact Original response body. Fixture storage was unchanged. | 0 |
| Rust HDHomeRun planned restart | A lineup and the React index matched before and after a normal stop and start. SQL state matched and the application process generation changed. | 0 |

Two Docker inspections saw all three Rust application generations and their separate PostgreSQL and Redis roles. The browser fixture also had its own browser-origin container. The inspections verified 10 distinct running containers, four networks, and three volumes, with **608.045 ms** between the end of the first inspection and the beginning of the second. Only then did the coordinator release all three fixtures to issue their requests. This is evidence of concurrent fixture readiness and isolation on one host. Full M9 acceptance and comparative performance remain open.

The missing-credential response bytes were verified using the pinned Original M3U and XMLTV handlers and Django 6.0.9's actual JSON renderer in a bounded offline check. Both physical Rust responses matched the status and SHA-256 of `{"error": "Unauthorized"}` exactly. The physical Original browser control exercised the account UI.

Cold preflight, physical execution, the independent audit, and the owner each exited 0. The four workers verified 21 container retirements. The coordinator verified absence for each named or observed resource, empty project inventories, and an empty running-container list. An independent read-only replay produced the same audit bytes. The qualifying batch receipt SHA-256 is `4611fb9dc8a703c969c23b47bd9cb73530f9ff76e58e972a2d81bb93dcbaa3cd`. The machine-readable summary includes the ordinary package, shared release, overlap observations, and worker receipt digests. [Machine-readable evidence](../scripts/rust-integration/m9-d6ebd813-arm64-parallel-fixtures.json)

Both earlier attempts retain literal exit 1 and exact cleanup. The first stopped at the Original browser's project-name guard before any UI action or native startup. The second achieved **603.522334 ms** of overlapping native readiness, and both browser controls and HDHomeRun exited 0. Its M3U fixture expected the obsolete compact JSON digest for the missing-credential response. The final run used the verified Original digest and retained the XMLTV expectation and all other fixture checks.

The original Dispatcharr authors and contributors retain full credit for Dispatcharr and its existing React frontend.
