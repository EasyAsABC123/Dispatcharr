# Viewer delivery and focused client compatibility

The original Dispatcharr authors and contributors retain full application,
workflow and algorithm credit. The selected workflows continue to use the
original React application.

Candidate100 changes fMP4 delivery so a slow viewer can resume from a
retained fragment after the idle interval. It also ports the ordinary VOD
stats route, Python-compatible HDHomeRun stored output-profile IDs, disabled
recurring-rule detail operations, and canonical JSON channel bulk rename.
The recurring implementation fences enabled-rule rebuilds. The rename uses
the pinned regex dependency and a bounded request and row budget.

The exact candidate revision is `6a015a0bc753b3688e099e57ec5b9cf2b64936ad`.
Formatting, strict all-target Clippy, 551 Store tests, 41 Main tests and three
HDHomeRun/fMP4 tests passed. The corresponding crates reported 79, one and
eight external-fixture tests ignored, respectively. Exact package and scoped
physical verification are in progress. The completed Candidate98 comparison
below remains the Original authority for the VOD route changes.

[Original and Candidate98 VOD evidence](../scripts/rust-integration/exact-vod-original-c98/receipt.json)
· [Milestone ledger](rust-milestones.md)
