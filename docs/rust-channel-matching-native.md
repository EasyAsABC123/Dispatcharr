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

## Required application acceptance

At this source checkpoint the Candidate38 Docker build, six package checks and
complete Native14 matching cohort remain pending. The cohort reuses qualified
Source51 observations and runs all 14 natural HTTP/task/SQL/child/cache/event
cases together. Its Native setup combines the unchanged seed, receiver setup
and before-state functions in their original order into one Django invocation;
it does not invoke or replace matching task bodies.

The supported characterization domain is DISABLE_ML_DOWNLOADS=true, an actually
absent MiniLM model cache and empty general-plugin configuration/directory.
Actual model-enabled inference is not implemented or accepted by these results.
The environment/cache witness comes from the application configuration and
filesystem rather than a caller-supplied success flag. Unsupported domains stay
guarded. Wider roles, OPTIONS metadata, queue concurrency/loss, enabled plugins,
paired resource measurements and complete M6/full migration acceptance remain
open. No application performance gain is claimed.
