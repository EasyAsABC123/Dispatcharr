# Batch53 reference: combined live/VOD group settings

Original Dispatcharr and dependency authors retain all application, API, model
and algorithm credit. React remains unchanged. This reference addresses its
normal combined settings request: every live group is submitted alongside only
changed VOD categories, followed by an account-detail GET. The accepted
Candidate52 image still explicitly refuses nonempty live-group arrays.

The [Source evidence](../scripts/rust-integration/live-vod-settings-source1/index.json)
qualifies twelve manual PATCH/GET cells across two literal controllers. The first
controller exits 1 in 80.514 seconds after fully capturing the combined save;
its checker uses the wrong model table prefix. Independent offline checks
qualify that actual save without changing rows or replaying it. A second
controller exits 0 in 173.124 seconds for only the eleven remaining cells. An
explicit import of the captured combined state—fifteen selected tables and all
fifteen sequences—precedes the continuation and is separately observed. These
are one qualified retained save plus eleven new cells, never one successful
twelve-cell controller.

The corpus covers combined save/repeat, category partial-property merge and null
clear, live-property replacement and profile-ID coercion, nullable/fractional
ranges, invalid ranges/category properties, duplicate live targets and both
missing foreign keys. Exact HTTP success/error bodies, complete rows, primary
keys, clocks and sequence values remain available. The selected settings-cache
keys, broker state, complete producer journals and raw/decoded event arrays are
retained without filtering. Twelve actual completed group-add receipts bind the
empty event observations.

Twenty-six compressed SQL snapshots preserve every value except 208 nonnull
account-login fields. Twelve account GET copies redact another 24 login values:
232 credential fields total. The derived semantic projection normalizes only
initial clock/login entropy after exact raw stability checks; business values,
primary keys, all fifteen sequences and error text remain literal. Both
controllers have independent cleanup: three exact containers, two captured
original host generations and the project absent. The UID initial-generation
gap remains explicit. Actual Source is the pinned Original image; Candidate52
is only an unused dependency in these Source runs.

Observed transaction boundaries guide the private Rust draft. Category-property
and live-range validation errors consume no IDs. A duplicate live target aborts
before category writes, rolls back business rows, and consumes three live IDs.
Both missing foreign keys fail at deferred commit: two live and two category IDs
are consumed while every business write rolls back. PostgreSQL message/DETAIL/
HINT text is preserved, including newline labels and two spaces after the colon.
No fixture constraint name or invalid ID is hardcoded into the formatter.

The private combined handler retains its pool connection beyond the transaction
wrapper. A commit error must await same-connection rollback/Ready before returning
its mapped400 response or reusing the connection; failed rollback retires the
connection. This code has only been parsed. Compilation, physical rollback and
immediate-GET controls, a new image and the complete affected Native workflow
remain pending. This is Source reference qualification, not Rust/M5 closure,
performance acceptance or complete M0–M9 migration acceptance. Broader input and
role domains, profile execution, the preceding playlist update and following
playlist refresh remain separate scopes.
