# M0/M1 disposition review: subgates qualified, formal gates remain open

This is an additive review proposal, not a milestone acceptance or a modification
of the historical crosswalk. Original Dispatcharr and dependency authors retain
credit and licensing. Root decides the ledger disposition.

## Existing requirements and actual disposition

| Existing M0 requirement | Evidence now available | Disposition |
| --- | --- | --- |
| Pinned toolchain; local and Rust-image workspace formatting, tests and lints | Historical 20-test bootstrap remains valid. Canonical repair `a65e334d82a8de51da2bc6d63af5e7cb4a3bd771` now supplies the qualified current verifier, formatting and input closure: host/container 560 default Rust tests, 701 external-fixture tests ignored, strict Clippy, formatting, 41 passing Python tests plus one optional deep skip, and 1,806 helper comparisons. | Qualified for its exact source and selected suite. The published shell has a separately disclosed one-space normalization and `sh -n` check; its new byte hash is not mislabeled as the executed archive. Later changes still need their own checks. |
| Resolve runtime URLs; snapshot schema and non-OpenAPI surface | Stable 592 routes, 798 operation work items and 250 static non-HTTP candidates; runtime extraction, passive metadata and literal public crosswalk exist. | Discovery/indexing is qualified. There are 322 unresolved method placeholders; passive observations do not invent declarations or close dynamic method behavior. |
| Contract register with source/test, method/path/event, roles, setup, assertions, owner, coverage and result | Register schema and stable identities exist. Eleven operations have partial captured-case bindings. | **Not complete as an executable contract register.** Every one of the 798 operation rows has null setup/result and empty top-level assertions. Source test candidates are module associations, not proof that a particular test covers a behavior. |
| Deterministic media and injected fault fixtures | Published TS/fMP4/HLS/seekable VOD fixtures and malformed/stalled/redirected/ranged/failure authorities; many/mixed/capacity/change/sustained references; bounded prolonged outage. | Qualified in the declared fixture/scenario scopes, not all future profile combinations. Preserve the short-fault decoder warning and outage context-report serialization exit 1. |
| Supported pinned Original tests | Full unchanged Original suite passed 3,362 tests, separately from earlier 48/526 scopes. | Qualified for its recorded pinned environment; counts are not summed. This does not assign every discovered route to a specific executable test. |
| Baseline scenarios and frozen budgets | Named warmed image48 Source cohort; idle/churn and fixed worker-inclusive three-run references; finite capacity, mixed, many, fault/change and sustained accounting; supplemental Source2 metric attachment. | Source characterization is valid in these bounded scopes. Valid Source process/FD/socket budget failures remain failures. Missing/unexercised numerical quantities stay null; not every field has a universal absolute or comparative ceiling. |
| Another developer can reproduce contracts and measurements through Compose | Public source/tool maps, guarded runners, numerical validators and recorded cleanup exist; public prerequisite eight controls and reconciliation ten controls pass in a new temporary public-only tree. | Reproduction for the qualified captures/tooling is established. It does not repair the unmapped executable-contract fields by itself. |

The contract requirement is explicit in `docs/rust-milestones.md` M0 and in
`scripts/rust-contracts/README.md`: M0 still requires collecting method/permission/
response contracts and assigning an executable test and owning milestone to every
required behavior. This review does not redefine that as an inventory count.

## Literal current register accounting

Actual operation rows: 798. Setup fixtures populated: 0. Nonempty top-level
assertion lists: 0. Nonnull operation results: 0. Role state: 787 unmapped and 11
partial captured cases. Coverage state: 771 not yet mapped, 16 scoped evidence
needing case mapping, and 11 partial captured-case mappings. Fully verified: 0.
Unresolved method placeholders: 322. These are separate dimensions, not additive
percentages. Stable IDs, methods, aliases and all historical captures remain
unchanged. The 250 non-HTTP entries include static candidates; do not equate all
of them to independently established runtime behaviors.

## Source observation limits that stay explicit

The fixed three worker-inclusive runs have valid coverage, six tasks and 44
checks per run, but process/FD/socket growth is 1/144/18 against 0/16/16 each time.
They are a characterized negative baseline, not Source budget acceptance or a
causal leak claim. Source1 had zero PSS observations and remains a setup failure.
The outage retained scenario is valid, but fixture-context final-report JSON
serialization failed with process exit 1 after an atomic report was persisted;
stored `passed=true` does not make that process successful.

Source2's 16 complete role-points and 75 PID observations, actual task/media/
physical-close/retention/default-DB/cleanup checks, and independent retained-data
numeric tests qualify its declared metric attachment. Four sequential endpoints
are not peak PSS or a continuous simultaneous census. Whole-daemon Redis counters
include observer traffic and are not application-only or link-byte accounting.
Three separate 2 MiB TS samples are not whole-stream loss measurements. Known
media executable absence does not prove unknown Python wrappers do no media work.
Observer ingress is a disclosed fixture topology change, not a retroactive change
to the fixed three-run measurements. No later Source rerun is needed just to
rewrite or reconcile these facts.

## Smallest next executable-contract task

Start with **one already captured registered operation**, such as one provider
information GET action among the eleven partial bindings. In a new additive
Source-contract namespace, bind its exact stable operation ID, captured roles,
setup/cleanup recipe, pinned Original test or runner entry point, assertions and
actual result authorities. Reuse the existing captured fixture and Source test
provenance where it really applies; do not claim a module-level test list is a
route-level assertion. Add meaningful rejection controls for an incorrect role,
method/alias, fixture or source-case binding. Run the smallest supported-Original
functional replay if that operation has no existing independently reproducible
assertion entry point. No performance window is needed for this contract work.

Then apply the same assignment process to remaining **required behaviors**, with
explicit unmapped cases left open. Reuse concrete tests from the 3,362 suite and
real captured contracts instead of demanding new endpoint executions just for
counts. Dynamic methods require actual authority, not guessed GET/HEAD/OPTIONS.
Task/event/non-HTTP behavior needs its corresponding executable authority.
This requires Source contract definitions and provenance, **not** completion of
all 798 Native implementations, complete Native role matrices, M9 soak or a fresh
core benchmark. A Source test may expose a preserved Source defect; it must not
be changed to seek green.

## Proposed ledger disposition, subject to Root review

Close the specifically qualified measurement-characterization and developer-
verification subgates. Keep formal **M0 in progress** for executable Source
contract mapping. Keep formal **M1 in progress**, despite its already-passed
technical gate, because its M0 dependency remains open. This corrects the earlier
bounded-acceptance recommendation: that recommendation omitted an explicit
contract-definition criterion and is not authority to waive it.

M1's deterministic/container/slow-reader memory/equivalent-work core gates remain
bound to `verified-core-native-functional-2.json` and the Core8 rebind proof. The
nine-pair result and public reconstruction qualify that technical gate; this
review neither repeats it nor transfers its gains to HTTP or the application.
Only after the actual M0 criteria are accepted may Root formally pass dependent
M1. Other milestone statuses are unchanged.
