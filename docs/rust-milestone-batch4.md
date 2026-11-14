# Normal settings routes and fresh-install Series behavior

Original Dispatcharr and dependency authors retain all application, algorithm
and license credit. React is preserved unchanged.

This combined seven-file change assembles the qualified Core settings service
into Main and handles the Source no-rules Series branch. The
[929-input SDK candidate](../scripts/rust-integration/milestone-batch4/final-summary.json)
passes formatting, workspace compilation, strict Clippy and the complete default
test run: **890 passed, zero failed, 751 ignored**, across 172 result blocks.
All four commands exit zero; every input remains unchanged throughout. The
existing shared Cargo target is reused. The unrelated VOD changes are excluded.

Main constructs one settings/cache/effects service before the stream engine,
shares the original Auth writer admission, and uses that same instance for
optional dynamic health and final drain. Router composition preserves collection
GET, explicitly replaces detail GET, and merges the settings mutations without
duplicate handlers. A foreign admission budget is rejected. No additional SQL
or Redis pool is introduced.

Main's normal Series worker now applies the existing Source filtering of rules.
Missing, empty, non-list and all-nondictionary rules return
`{"scheduled":0,"details":[]}` before offset validation, SQL, artwork or events.
Existing legacy constructors retain their previous bounded behavior. A grouped
control verifies the no-effect branch and that dictionary rules still reach
configured preflight.

The [shared Docker build and physical probes](rust-milestone-batch4-build.md)
pass, as does the [complete clean-install/React regression](rust-milestone-batch4-clean-install.md).
The [affected job cohort](rust-m6-packaged-main-batch4.md) passes191 assertions:
the configured-rule chain, actual settings PATCH200 and second real import with
no rules, using packaged data paths without a Source tree mounted in Main.
The [Core99 Main cohort](rust-main-core99.md) also passes its scoped HTTP/effect
checks; missing producer transport metadata and other full settings/plugin
criteria remain open. The previous Main PATCH405
remains failed evidence on its original image. The [default worker audit](rust-default-worker-graph.md)
records the broader missing graph. No full milestone or performance gain is
accepted by these SDK results.
