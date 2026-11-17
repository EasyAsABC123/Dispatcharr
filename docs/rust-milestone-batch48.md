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
| Candidate48 image and package families | Build and all six package families pass |
| Combined Native seven-context cohort | Corrected full run passes all seven contexts; actual exit 0 in 411.508 seconds |
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

The committed source `2d25d5d77e2cb22980f1b677b33a155cd307673d` builds in
301.212 seconds as image
`sha256:a623d5ad9bb588e8cbb1cc0ff0ed5eac5bc87561788c51fb4545f6287da28cc4`.
All six package families pass. The production Main ELF changes from Candidate47;
all 1,445 archived Rust inputs match SDK66, and the React assets and original
credits remain unchanged. All 55 exact package-test containers are absent.

The first seven-context run exits 1 in 417.234 seconds. Its six prior contexts
pass, and all six new metadata phases pass their local assertions, including
30 real provider requests. The final cross-backend comparison fails on nested
logo `cache_url`: Source and Native use independently assigned localhost ports
61610 and 64347. Scheme, host, path, query, logo ID and version hash match.
Recorded port receipts and Main binding establish the differing fixture origins.
A narrow offline comparison validates both origins before mapping only this
Native response field to the Source origin; all six captures then match, with
seven deliberate URI corruption controls rejected. This diagnostic does not
change the failed runtime result.
Root verifies its nine exact containers, nine captured host process generations
and project absent.

The fresh corrected full run passes all seven contexts with actual exit 0 in
411.508 seconds. It reuses the unchanged Candidate48 image and known Source
authority. Repeat ingestion, Series, manual Comskip, shared VLC, provider-info and
advanced-Series all pass alongside the new six-phase metadata workflow. The
metadata comparison checks complete rows/support data, all eleven sequences,
HTTP/task/provider/notification/catalogue behavior and typed clock/UUID stability.
Its actual Native port 64784 is independently bound to the running Main and
mapped only in the declared logo cache URL field to Source port 61610. Paths,
queries, version hashes, provider URLs and all other metadata remain exact.
Root independently verifies nine exact containers, nine captured original host
process generations and the project absent. No Rust rebuild or Source replay
was needed for the comparator repair.

Evidence: [Source cohort](../scripts/rust-integration/dense-list-source1/README.md),
[SDK66](../scripts/rust-integration/milestone-batch48/SDK/summary.json), and
[physical SQL](../scripts/rust-integration/milestone-batch48/physical/README.md).
The [package proofs](../scripts/rust-integration/milestone-batch48/package/inventory.json)
and [failed application attempt](../scripts/rust-integration/milestone-batch48/application-attempt1/index.json)
retain their separate scopes and actual exits.
The [successful seven-context packet](../scripts/rust-integration/milestone-batch48/application/README.md)
retains the complete affected cohort. The clean public seven-context reproduction
also passes: actual exit 0 in 415.798 seconds on the same image, with no private
Source captures or Git directory. Its 31 Python payloads, all 1,445 Rust inputs
and UID65532 readability are verified. Root confirms ten exact CIDs including the
UID probe, nine captured original host process generations and the project absent;
the UID probe's initial host PID was not captured and is not inferred. The
[public execution packet](../scripts/rust-integration/milestone-batch48/public-application/index.json)
and [runner instructions](../scripts/rust-integration/catalogue-metadata-cohort/README.md)
make this bounded step reproducible. Broader provider domains and new application
performance remain separate work.
All complete M0–M9 milestones remain open.
