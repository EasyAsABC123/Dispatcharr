# Plugin repository test support validation

The current combined Rust source passes formatting, workspace compilation,
strict Clippy across all targets, and default workspace tests: 774 passed and
741 ignored external-fixture tests across 172 result blocks. Ignored tests are
not acceptance results. [Actual waits and source binding](../scripts/rust-pluginrepo-contracts/current-test-support-qa/Root-review.json)
identify the 860 inputs: 855 Rust and five coupled files.

Five test-support paths add a fresh 23-case repository peer and four actual
metadata SQL/response-ownership probes. The original-writer observer is compiled
only for tests. The tower utility is a development dependency using the existing
locked package; no package version was added or changed. Production service
behavior is unchanged by these test additions.

Fresh Docker results are recorded separately. This workspace check does not
qualify Main integration, complete M8, or establish new performance gains.
All Dispatcharr application and algorithm credit remains with the original
authors and contributors.
