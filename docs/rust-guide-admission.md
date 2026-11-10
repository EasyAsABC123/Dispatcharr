# Bounded guide request waiting

Original Dispatcharr authors and contributors retain all application and algorithm credit, branding and AGPL licensing.

The [image29 startup-burst comparison](../scripts/rust-guide-perf/verified-grid-functional-image29-4.json) found a visible mismatch before any resource measurement: the original returned10 successful2250-row grids, while native returned two matching responses and eight503 errors. Its immediate active-work admission refused callers before a workspace could become free. Existing stable-request tests and the Current/XMLTV performance matrix did not cover this grid burst.

The correction admits at most ten owned guide requests and lets them wait for the existing two active slots and192MiB workspace budget. Each active workspace remains96MiB. Input byte/count limits bound waiting allocations separately, conservatively below1MiB per owner. HTTP admission occurs before query decoding or header cloning. Excess owners remain explicitly rejected; the SQL/response deadline is not restarted while waiting.

A canceled or expired waiter leaves before authentication/SQL starts. Once physical work starts, its owner remains until completion and rollback even if the caller leaves. Warm shutdown rejects new owners while draining accepted waiting and active work. Detached response bytes keep their memory credits until their final clone is dropped.

[Component regression2](../scripts/rust-epg-parity/verified-grid-queued-admission-2.json) proves the ten-request drain through a real PostgreSQL relation lock, exactly two active queries, unchanged workspace reservation, eleventh-owner rejection, queued cancellation/expiry, shutdown with waiting work, and final-byte ownership. It also passes22 metadata/stored cases,22 complete entities,40 HTTP cases, original HEAD observation, strict all-target Clippy and six library tests. The first burst control accidentally retained its reference response through variable shadowing, preventing two full workspaces from fitting; that test failure is retained, and an explicit reference-frame drop fixes its setup without changing runtime bytes.

[Actual packaged ten-viewer replay](../scripts/rust-guide-perf/verified-grid-functional-image30-1.json) and the [independently sealed six-pair GRID resource matrix](../scripts/rust-guide-perf/verified-grid-experiment-image30-1.json) now pass on image30. No new performance result follows from these functional controls, and accepted image28 Current/XMLTV gains remain bound to their original image and workloads.
