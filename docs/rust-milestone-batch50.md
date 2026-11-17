# Rust milestone batch50: list external-ID identity

Original Dispatcharr contributors and dependency authors retain all application,
model, task, protocol and algorithm credit. This batch extends the previously
qualified empty-language Movie/Series metadata domain with bounded external IDs.
The React UI remains unchanged.

Source passes nine natural refresh roots, each after an explicit fixture reset
or seed. The reset may change only six VOD model/relation tables and their six
sequences through ordinary allocation; unrelated accounts, support rows, cache,
provider and broker state remain exact. No manual sequence reset or known Source
business replay is used. Full rows, support data, all eleven sequences, actual
claimed tasks/backend results, provider TLS, HTTP, ordered notifications and
catalogue responses are retained.

| Validation | Current result |
| --- | --- |
| Source external-ID cohort | Nine cases pass; actual exit 0 in 186.533 seconds |
| SDK70 | Original attempt all four gates pass; retained |
| SDK71 ordering correction | All four gates pass; 1,169 tests pass |
| Default tests | 1,169 passed, zero failed; 784 fixture-dependent tests ignored |
| Physical SQL | Original and corrected attempts both pass 45 statements / 24 controls |
| Candidate50 Docker build/packages | Original and corrected images/package families pass separately |
| Combined Native nine-context cohort | Attempt1 failed; corrected full rerun running |
| New application performance | Pending |

Movie aliases prefer `tmdb_id` over `tmdb` and `imdb_id` over `imdb`. Series
prefers the opposite order. Source truthiness selects an alias before zero
cleaning: numeric `0` permits fallback, while string `'0'` blocks fallback and then
becomes absent. Opaque strings retain spaces, case and leading zeroes. The
provisional domain is printable ASCII external IDs up to 50 characters, positive
decimal JSON integers up to 50 digits, integer `0`, empty strings and null. Both
aliases must remain in that bounded domain. Booleans, floating values, negative
integers, containers, Unicode, control characters and larger IDs stay refused.
Actual model columns are nullable varchar(50); language and category tagging
remain outside this expansion.

Global identity priority is TMDB, IMDb, then name/year when both IDs are absent.
Ordered provider occurrences coalesce only within the same global identity.
Distinct provider IDs keep separate account relations. The initial keyed lookup
and the pre-insert secondary-ID recheck remain separate: a secondary fallback
can reuse a global row without applying candidate metadata to it. Existing-ID
matches preserve Source's stored-name behavior while year, metadata and relation
basic data follow their separate update rules. Sparse ID omission can move a
relation to a different no-ID global; source-assigned relation IDs and refresh
stamps remain contractual. Cross-key duplicate provider IDs and multiple initial
identity groups targeting one model remain explicitly unqualified.

The nine Source cells cover fresh priority, duplicate identity occurrences,
same-provider repetition, name/year changes under stable IDs, alias/zero
precedence, reassignment to existing targets, sparse ID omission, the secondary
recheck and conflicting secondary IDs. The last cell preserves Source's caught
batch-error behavior: attempted model inserts roll back, sequence allocation and
separately committed setup/category effects remain observable, and the root
continues according to Source. This is not a blanket exception or success waiver.
Ownership-safe cleanup from Candidate49 remains unchanged.

SDK68's workspace attempt failed when the sandbox denied local TCP bind in the
ingest HTTP tests, before Store tests ran. Formatting, check and Clippy passed;
the exit 101 and exact failure excerpt remain recorded. SDK69 subsequently passed
all four gates. Before physical execution, independent review caught two new
wrong-bind controls that still used the original INT8 type. SDK70 changes only
that peer plumbing, replaces those controls with BOOL and asserts each mutated
wire type differs from production before Parse/Describe. Its six production
payloads are byte-identical to SDK69. These historical attempts are preserved,
not rewritten as application or physical successes.

SDK70 binds 1,452 inputs, including 1,446 Rust inputs and the new external-ID
selector module. Forty-five actual production SQL statements and 24 wrong-bind
controls are prepared for the existing copied-schema executor. The physical read-only PostgreSQL gate passes separately in 14.188 seconds, with
both EOFs and settled group. It performs no business DML/tasks/media, uses no
statement cache, passes Ready/Close and leaves the default 62-table/59-sequence
schema byte-identical. Root verifies its one exact container, eight original
host process generations and project absent. All six Docker package families now pass separately. The first full affected Native nine-context run fails strict Source comparison;
ignored host tests do not satisfy that gate.

Evidence: [Source authority](../scripts/rust-integration/external-identity-source1/README.md),
[SDK70](../scripts/rust-integration/milestone-batch50/SDK/summary.json),
[physical SQL](../scripts/rust-integration/milestone-batch50/physical/README.md) and
[historical SDK outcomes](../scripts/rust-integration/milestone-batch50/SDK/diagnostic-history.json).
Source identity is the pinned Original runtime, not the unused declared
Candidate49 dependency metadata. Public nine-context reproduction, larger or
additional identity domains, cross-provider transports, performance and all
complete migration milestones remain open.


Published source `9b1e7988c36eaf21c5017b54476aa6efdd89dfbd` builds in
302.470 seconds as
`sha256:470a6c44d0de4d109149bcb581545ad4af53fa7a1ff2f59a257fd92319ef7d1a`.
All 1,446 Rust inputs match SDK70; actual Store and Main recompilation is
verified, and the Main ELF differs from Candidate49. Unchanged React assets
and original credits remain verified. All six existing package families pass;
all 55 exact package containers and scoped inventories are absent. These are
[package proofs](../scripts/rust-integration/milestone-batch50/package/inventory.json),
not application or performance acceptance. The full Source0/Native9 attempt exits 1 in 607.581 seconds, with both EOFs and
settled group. Root verifies twelve exact CIDs including UID, eleven original
host process generations and the project absent. All eight earlier contexts and
all nine new local case assertions pass, but the complete strict Source
comparison fails in the first cell. Later complete comparison phases are not
reached, so the nine-context cohort is not accepted.


The failure is a production relation-allocation bug. In the first cell the global
Movie IDs and external identities match Source, but relation PK1 points to
provider stream 9702 instead of Source's 9701. The JSON input order was lost in the
relation INSERT's filtering plan. Model IDs, relation IDs and ordered Source
expectations are not normalized away. The [failed attempt](../scripts/rust-integration/milestone-batch50/application-attempt1/index.json)
retains its literal exit 1, traceback, local/whole-workflow boundary and cleanup.
The minimal proposed correction adds WITH ORDINALITY and ORDER BY to the Movie
and Series relation INSERTs. SDK71, fresh physical checks, a new image and the
entire affected Native nine-context run must pass before this correction is
accepted. Published SDK70, physical, package and failed application evidence stay
unchanged. Application performance and complete migration milestones remain open.


The correction now passes all four SDK71 gates with 1,169 tests, zero failures
and 784 ignored tests. Only the two relation INSERT SQL constants differ from
SDK70; values, typed controls, bounds and Source authority remain unchanged.
The separate corrected physical attempt passes all 45 statements/24 wrong-bind
controls in 14.107 seconds, with both EOFs and settled group, read-only/no-cache
Ready/Close and unchanged default schema. Root verifies its one exact container,
eight original host process generations and project absent. See
[SDK71](../scripts/rust-integration/milestone-batch50/SDK71/summary.json) and
[physical attempt2](../scripts/rust-integration/milestone-batch50/physical-attempt2/README.md).
The corrected Docker image and all six package families now pass separately.
The entire Native nine-context rerun is running. Neither the host checks nor
Parse/Describe establish corrected application behavior.


Corrected source `7a613ade8b926e99658c45ab071b832525ce5df2` builds separately
in 303.641 seconds as
`sha256:be231d05f35bb58a8241789377ee21f7c81ceb7ad2e1a09c47dc0a2355a5218d`.
Its 1,446 Rust inputs match SDK71. Actual Store/Main recompilation and a Main ELF
different from the failed image are verified. All six package families pass and
55 exact package containers are absent; the corrected
[package-attempt2 proofs](../scripts/rust-integration/milestone-batch50/package-attempt2/inventory.json)
leave all earlier package records unchanged. Fresh UID177 readability passes.
The same frozen 177 helper closure and strict Source oracle are used for the
complete Source0/Native9 rerun. At this update it is RUNNING; no corrected
application, public reproduction, performance or complete milestone acceptance
is inferred from successful compilation or packaging.
