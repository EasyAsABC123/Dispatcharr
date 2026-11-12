# Scanner-owned Refresh child migration

Original Dispatcharr and dependency authors retain full application, algorithm
and licensing credit.

The Rust library can now consume the bounded immediate Refresh children of a
successful scanner parent. Its explicit constructor pins a parent UUID and up to
four source IDs; successful parent metadata must list the child before claim.
Other families and unknown shapes remain refused. Main/default scheduling remains
disabled. M3U groups/single children and complete worker recovery remain open.

[Root integration evidence](../scripts/rust-job-contracts/scanner-logo/children-refresh/verified-root-formatted782/verified.json)
binds the merged, formatted 782-input source: strict all-target linting and 562
default tests pass, with 702 external-fixture tests explicitly ignored there.
The same code passes 150 actual checks across two EPG cases and three held-reply
cases. Original produces the scanner children; Rust executes Refresh; unchanged
Original consumes every Index/Series descendant. Events, index data, stable source
fields, typed results and lineage match the declared Source cases. The programme
case has no mapped channel, so it does not establish programme-guide ingestion.
The default database is unchanged and independent owned-project cleanup is empty.

The held Redis replies cover successful-parent lookup, actual claim, and committed
result. Caller loss and shutdown retain physical ownership until release. These
checks do not qualify every lost reply, task retry or worker restart. The original
portable reconstruction and its 27 rejection controls remain separately bound to
the historical unformatted component; the new Root source map records the merge.

The failed first portable run exposed that Original can still have a task reserved
at its prerun signal despite early-ACK policy. It remains preserved. A separate
held file-read Source witness proves ACK completion during the handler before its
result. Instant prerun reservation state is diagnostic, not a universal physical
ordering guarantee. Native physical claim controls were not weakened.

[Component reproduction and boundaries](../scripts/rust-job-contracts/scanner-logo/children-refresh/README.md)
include the immutable historical reconstruction and actual Docker runners.
These results establish functional component progress, with no new performance
claim and no full M6 acceptance.
