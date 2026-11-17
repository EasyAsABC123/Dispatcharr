# Candidate48: ordinary Movie and Series list metadata

The provider list ingestion path previously refused nonempty description, rating,
genre, artwork and common descriptive metadata. This change supports a bounded
ordinary Movie/Series workflow through the existing refresh-vod HTTP action and
claimed worker. All application, model, task and algorithm credit remains with
the original Dispatcharr authors and contributors.

Movie and Series lists retain their different Source rules. Nonblank core fields
replace stored values; sparse fields preserve existing details. Movie director,
actors and release-date properties fill only falsey stored values, while Series
nonblank properties overwrite their corresponding keys. Duplicate identities
retain the first nonblank representative and separate provider relations.

Artwork URLs create or reuse global logos. Missing or overlong artwork clears the
association while retaining orphan logo rows. Logo creation completes in its own
short SQL session before the model batch transaction. Updates preserve existing
UUIDs and model timestamps, skip unchanged rows, and avoid an extra write after
fresh insertion. Movie relation insertion now skips known provider IDs before
inserting, matching Source sequence allocation.

The selected domain keeps the existing catalogue and provider-row bounds, ASCII
metadata, empty language and absent external IDs. It admits at most one new logo
URL per kind and batch; existing URLs can be reused. Unsupported shared metadata
is refused before new model/logo effects, while previously supported sparse
behavior remains available. Broader metadata forms, language reconciliation,
external-ID identity and shared-account metadata remain open.

| Validation | Current result |
| --- | --- |
| Source natural refresh cohort | Six ordered cases pass; actual exit 0 in 116.207 seconds |
| SDK66 | Formatting, workspace check, strict Clippy and workspace tests pass |
| Default tests | 1,164 passed, zero failed; 784 fixture-dependent tests ignored |
| Physical SQL | 44 statements and 22 wrong-bind controls pass; no business DML |
| Candidate48 image and package families | Pending |
| Combined Native seven-context cohort | Pending |
| New application performance | Pending; earlier image results retain their own scope |

The Source cases cover fresh import, identical repeat, nonblank update, sparse
refresh with omitted/501-character artwork, duplicate provider identities and
final prune. Full rows, support data, all eleven sequences, phase continuity,
HTTP, claimed task, provider TLS requests and ordered notifications are captured.
Root independently verifies both exact Source containers, both captured host
process generations and the Compose project absent.

The first Source attempt failed before any refresh root because the fixture's
provider control allowlist omitted the new case names. Its actual exit 1 and
cleanup remain recorded. The successor changes only that dispatch and durable
process-identity capture; the six provider payloads remain unchanged.

The physical peer prepares and describes the actual compiled production SQL on
the copied pinned Source schema. Read-only mode, disabled statement caching,
Ready/Close settlement and unchanged default schema checks pass. Root verifies
its exact container, eight captured host process generations and project absent.
These checks establish SQL compatibility; they do not establish application row
effects. The packaged application cohort remains required.

Evidence: [Source cohort](../scripts/rust-integration/dense-list-source1/README.md),
[SDK66](../scripts/rust-integration/milestone-batch48/SDK/summary.json), and
[physical SQL](../scripts/rust-integration/milestone-batch48/physical/README.md).
The existing six application contexts must run alongside this new seventh
context after packaging. All complete M0–M9 milestones remain open.
