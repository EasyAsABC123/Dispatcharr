# BE82 ordinary signed HLS restart observation

The exact ordinary AMD64 package passed cold signed HLS. In the restart cell, one replacement mint opened one new Origin connection, with at most one active connection at a time. Fresh sequence 3 carried the discontinuity, and the output index reached 7 on the same Redis generation.

The first follow-up playlist returned HTTP 404 while index 2 and its retained segment were still present. Its descriptor had already aged beyond 57 seconds. The pinned Original and Rust implementations return a stale playlist at 45 seconds for this advertised target. The fixture retained the status and discarded the error body. Its requirement for a fresh playlist at the frozen index prevented a retained-segment request. Once the index advanced, it stopped making further playlist requests and eventually reported a timeout.

The workflow and evaluator exited 1. Exact fixture cleanup and full engine inventory checks exited 0. The [typed observation](rust-be828d4c-ordinary-hls-restart-observation.json) records both outcome classes. The next natural-expiry replay keeps the freshness rule and requires direct retained-segment readback, a fresh playlist, and both A/V decodes with the same replacement token.

Original Dispatcharr and all upstream contributors retain full authorship and license credit.
