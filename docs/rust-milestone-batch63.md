# Candidate63 validation checkpoint

Original Dispatcharr and dependency authors retain all application, algorithm,
model and protocol credit. React and the original license are preserved.

The [validation evidence](../scripts/rust-integration/milestone-batch63/index.json)
binds code `49abb52e1420e4b41239cd45e28b1f13132950ad`. SDK107 passes formatting,
workspace compilation, strict Clippy and workspace tests: **1,232 passed,
zero failed, 802 ignored**. All 1,464 inputs, including 1,458 Rust files,
remain unchanged. Ignored physical tests do not count as runtime passes.

Fourteen explicitly executed Redis/HTTP controls pass in six groups using one
owned Compose Redis fixture. They retain the nine result/artwork reconnect
controls and four VOD handoff controls, and add an actual HTTP three-phase check:
fresh creation, absent-session recovery and another entry into the owned current
session. Redis worker metadata and the HTTP header must match independently
observed process identity. Existing provider reuse, response-body, counter and
lease assertions remain. Exact container, original host generation and project
resources are independently absent. Both retained media prefixes pass strict
first-second audio/video decoding; this is not whole-media equivalence.

The production changes join process worker identity across Redis and HTTP,
preserve provider category insertion order before sequence allocation, retain
Accept in SD Vary responses, implement the newly observed SD method/fault cases,
and preflight the bounded sealed DVR candidate read. Candidate-read readiness
keeps the same deadline and admission ownership and does not replay an uncertain
mutation. Four real TCP controls cover dropped readiness, uncertain PEEK reply
and pending physical ownership. Full automatic CUT acceptance remains pending;
the earlier cold-read cause was inferred without a captured physical error kind.

The exact-revision Docker build passes in315.621 seconds. All six runtime package
families pass: archive/assets, crypto, PostgreSQL, regex, Python compatibility and
VLC media. Independent cleanup verifies55 exact containers absent. The image is
`sha256:da1967690af90e79d4544e19468819b7b7f01e98dc2392d5fe7ae32d399b5d17`,
source tree `b43cbd5dc7520f2131b2e9130e2ec9cd521357ab76db3d6c99c32de2b1cfd03b`.
Build and validation duration are not application performance measurements.

Earlier SDK104/105 failures and the first physical14 failure remain recorded.
The latter passed all three worker-identity phases before failing its test-only
`/evidence` path assumption. An optional test evidence directory fixes that
assumption while retaining the original default; the complete SDK107 and
physical14 rerun pass. No earlier failed controller is relabeled successful.

The twenty-context Native batch is being prepared against this image, reusing
qualified Source observations. Its matching Beat startup epoch, existing VOD
recovery opt-in and bounded recent-marker observer are explicit fixture changes.
Connection acknowledgement differences remain visible in raw transport evidence;
any future application-semantic classification must retain its narrow proof and
historical provenance limits. The previous Candidate62 Native19 result remains
failed with15 contexts passed and4 failed.

**All complete M0–M9 milestones remain open.** Whole application compatibility,
remaining API/job/admin/SQLite behavior, default recovery activation, performance,
soak and rollback still require acceptance. No current CPU or memory gain is
accepted, and earlier scoped measurements are not transferred to this candidate.
