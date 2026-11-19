# Channel bulk rename and Auto Sync regex preview

The original Dispatcharr authors and contributors retain full application,
workflow and algorithm credit. Candidate102 preserves the React, license and
attribution files verified on Candidate100.

The exact Candidate102 source revision is
`7b23e5fa27ba618e476c1b6cde465234cf30b639`. Its Docker image passed the
packaged input checks. Formatting, strict Store Clippy, four focused Store
tests and the compatibility adapter's Python syntax check passed.

The paired channel bulk-rename run matched all seven Original and Native
checks. Both returned HTTP 200 in the overlength case and stored the exact
512-character names. The operation kept the unrelated row unchanged.
Candidate100's earlier HTTP 500 remains recorded with its own image.

The Auto Sync scan keeps the separate count and 5,000-row limit, and packs
the ordered scan into one PostgreSQL array. Twenty PostgreSQL comparisons
matched across custom and generic plans. The ordinary 48-case Compose pair
and the 50-case pair with wide-Unicode checks passed. The wide case exercises
5,001 names of 512 four-byte code points, the bounded scan, and exact
application results. SQL order between identical update timestamps remains
unspecified by the Original query.

The six measured phases completed equivalent work. Median paired CPU fell
20.45% and peak simultaneous working set fell 56.05%. Throughput matched.
Two full-scan trials exceeded the fixed p95 budget, so the benchmark
exited 1 and M7 performance acceptance remains open.

[Performance report](../scripts/rust-perf/candidate102-regex-preview/README.md)

[Packaged functional evidence](../scripts/rust-integration/milestone-batch102/functional.json) ·
[Candidate100 replay](rust-milestone-batch100.md)
