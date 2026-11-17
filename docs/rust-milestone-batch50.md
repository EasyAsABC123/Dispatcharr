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
| SDK70 | Formatting, workspace check, strict Clippy and workspace tests pass |
| Default tests | 1,169 passed, zero failed; 784 fixture-dependent tests ignored |
| Physical SQL | 45 statements / 24 wrong-bind controls pass |
| Candidate50 Docker build/packages | Pending |
| Combined Native nine-context cohort | Prepared; not run |
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
host process generations and project absent. Docker packages and the full
affected Native cohort remain pending; ignored host tests do not satisfy them.

Evidence: [Source authority](../scripts/rust-integration/external-identity-source1/README.md),
[SDK70](../scripts/rust-integration/milestone-batch50/SDK/summary.json),
[physical SQL](../scripts/rust-integration/milestone-batch50/physical/README.md) and
[historical SDK outcomes](../scripts/rust-integration/milestone-batch50/SDK/diagnostic-history.json).
Source identity is the pinned Original runtime, not the unused declared
Candidate49 dependency metadata. Public nine-context reproduction, larger or
additional identity domains, cross-provider transports, performance and all
complete migration milestones remain open.
