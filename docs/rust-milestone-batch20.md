# Main registry and regex drain correction

Original Dispatcharr and dependency authors retain full application, algorithm
and license credit. React is preserved.

The [preceding candidate](rust-milestone-batch19.md) passed its package checks
but failed actual Main startup: AutoChannels advertised six workers, while the
common registry admitted non-recording families only at one. This rejected the
registry before its Redis intake connected. The production validation now admits
only AutoChannels at the existing shared six-task ceiling. Other family limits,
selected-head requirements and duplicate/missing guards remain in force. No SQL
pool, task budget or semaphore capacity is increased. A regression calls the
same production guard with actual Auto handler metadata and invalid cases.

The candidate also signals the existing per-flow regex deadline before joining
worker lanes. Standalone, remote and XC import flows register weak cancellation
handles; already admitted work can progress until the actual drain deadline,
with time reserved for physical child retirement. Racing flows inherit that
deadline, and concurrent updates cannot extend an earlier deadline. Child wait,
pipe EOF and caller ownership remain attached to the existing actor. Forced or
unknown outcomes do not become successful filters or task completion.

Formatting, compilation, strict Clippy and the workspace pass: 1,025 tests,
zero failures, 761 ignored and 175 result blocks. The
[SDK record](../scripts/rust-integration/milestone-batch20/inventory.json) binds
1,293 inputs, including 1,287 Rust-tree files. Three ignored physical regex
controls require execution with the actual packaged interpreter; their presence
does not establish Main cancellation acceptance.

The next image must pass real Main startup and the combined Compose cohort:
auto-sync, large actual child metadata, normal regex, VLC and Native regressions.
Held-search diagnostics remain distinct from ordinary shutdown. Compact/manual
numbering, SD guide work, wider input/output profiles, enabled plugins and other
Source gaps are separate prepared or open work. No full milestone or application
performance gain is accepted.

The image now builds from `1cdf0c15ff4d8265409337f6b0ce29a0debe29e3` as
`sha256:cb2e36ee6ee6e76a4da8f3832e0fa1ee47f069b2619f39ec8be0e617357ab41e`.
All 1,293 SDK inputs match committed Git blobs; the 1,784-file image source
archive also matches that revision. Package checks pass as UID 65532: original
React/credit retention, crypto, PostgreSQL, all 648 Python/regex/bridge/notice
hashes, real isolated framed regex request/shutdown, and paired VLC useful core.
The [package evidence](../scripts/rust-integration/milestone-batch20/package/)
retains their actual scope. Combined Main/Compose acceptance is running; its
results and performance remain unaccepted until the batch completes.
