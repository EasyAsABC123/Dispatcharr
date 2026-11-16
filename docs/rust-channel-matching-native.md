# Integrated Rust channel matching

Original Dispatcharr, Django, Celery, Kombu, RapidFuzz and dependency authors
retain application, algorithm and library credit. React is unchanged.

The integration adds the three natural channel matching tasks to the shared
Main default queue and connects the collection/detail match-epg HTTP actions.
It reuses the existing Auth pool, queue ownership, output cache, realtime events
and Schedules Direct child capability. Matching work starts after the actual
queue claim; CPU matching closes its SQL session first and retains physical
ownership through cancellation and shutdown. Known Source task failures settle
as failures; unknown operational settlement remains an explicit error.

[Source51](rust-channel-matching-source51.md) supplies the actual 14-case
reference and pure normalization/scoring vectors. Exact and fuzzy matching
preserve the characterized ordering, region selection, selected-channel behavior,
child grouping, cache invalidation and single-versus-bulk failure differences.
A focused regression additionally preserves lazy normalization-cache behavior
for an empty channel name on the single exact path.

## Validation before packaging

The [SDK51 receipts](../scripts/rust-integration/milestone-batch38/checks.json)
record formatting, workspace compilation, strict all-target Clippy and workspace
tests all at actual exit 0, with unchanged inputs throughout. Tests report
1,130 passed, zero failed and 784 ignored across 180 result blocks. Ignored
fixture tests are not physical or application acceptance. There are 1,424 SDK
inputs, including 1,418 Rust inputs. The earlier SDK50 strict-lint failure is
retained alongside the successful run; its other gates passed.

A separate owned, read-only PostgreSQL check prepared all 11 actual compiled
production SQL statements against the pinned migrated Source schema, verified
server bind types and row-decoder compatibility, and rejected two deliberately
wrong bind vectors. The connection reached Ready and closed; the default
database remained unchanged and owned fixture cleanup passed. This check did
not execute matching DML, tasks or media.

The fast affected-Store lint ran before SDK51 and passed in 28.912 seconds.
SDK51 gate times were 3.031 seconds formatting, 19.246 compilation, 50.551 strict
lint and 125.059 workspace tests. These are validation timings, not application
performance gains.

## Candidate38 application acceptance

Candidate38 builds committed runtime
`3ffa1bb118baca2731efc45a1ce4d5df5ffaf2e5` as image
`sha256:10ca1c528f01abfbd7bb27eb8d9f5ba13f6e51186d15de67627fbb3a85058981`.
The actual build exited 0 in 147.791 seconds. All
[six package families](../scripts/rust-integration/milestone-batch38/package/inventory.json)
pass; independent inventories verified all 55 package containers absent.
The archive's Rust bytes match SDK51, React assets remain unchanged and original
credits/dependency notices remain installed.

[Native52](../scripts/rust-integration/milestone-batch38/Native52/qualification.json)
passes the complete 14-case matching cohort at actual exit 0 in 124.030 seconds,
with both pipe EOFs and an empty owned process group. It reuses qualified
Source51 observations with no new Source execution. All 14 actual backend result
values and ordered matching-event scalar projections equal the retained Source
reference: 13 SUCCESS, one expected FAILURE and three real child tasks. These
scalar comparisons do not establish every serialized channel field or every role.

The Native setup combines unchanged seed, receiver setup and before-state
functions in their original order into one Django invocation. It does not
invoke or replace matching task bodies. The first UID preflight failed because
the frozen driver root was mode 0700; the intended root-only 0755 correction
left all payload bytes unchanged. A fresh UID preflight passed for all 161 files.
The failed attempt remains recorded; no application privilege change, business
replay or rebuild was used to fix it.

Independent cleanup verified the three fixture containers, one read-only
observer container and all project resources absent, along with three recorded
original positive host PIDs. The observer's host PID was not independently
captured. Main exited normally. Full per-event SQL physical-settlement and
broader cancellation/loss acceptance are
still separate requirements.

These are functional and validation timings, not matched-work application
performance measurements. In particular, the Source51 and Native52 elapsed
values cannot establish a gain from the setup-call fusion.

## Current Main regression

The [Main54 seven-context batch](../scripts/rust-integration/milestone-batch38/Main54/qualification.json)
ran on the same Candidate38 image with selected jobs enabled. Actual exit was
1 after 308.868 seconds, with both pipe EOFs, settled process group and cleanup.
Six contexts pass: authorization, shared VLC viewing, output profiles, process
transports, unchanged React and stream denial. The React context passes all five
visible assertions against retained Source authority. The VLC context observes
two actual viewers sharing one input, upstream and provider slot, with strict
three-second audio/video decodes and normal retirement.

Input promotion remains failed: both identical owner/follower captures decode
with exit 0 but emit H.264 missing-PPS/no-frame startup warnings, violating the
strict zero-stderr criterion. Other promotion/failover business observations
pass; those cannot override the decoder failure. The available evidence does
not identify a Rust defect or establish that the warnings are harmless.
Independent cleanup verifies nine fixture CIDs, nine recorded original positive
host PIDs and empty project inventories. The full seven-context batch and full
M2/M3/M5 acceptance remain open. A viewing-resource study must independently
qualify its exact workload/configuration and cannot inherit this batch as a pass.

The matching characterization domain is DISABLE_ML_DOWNLOADS=true, an actually
absent MiniLM model cache and empty general-plugin configuration/directory.
Actual model-enabled inference is not implemented or accepted by these results.
The environment/cache witness comes from the application configuration and
filesystem rather than a caller-supplied success flag. Unsupported domains stay
guarded. Wider roles, OPTIONS metadata, queue concurrency/loss, enabled plugins,
paired resource measurements and complete M6/full migration acceptance remain
open. No application performance gain is claimed.
