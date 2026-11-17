# Candidate49: shared provider metadata and last-owner pruning

Candidate48 deliberately refused new metadata updates to Movie/Series rows shared
by provider accounts. Source matches those global rows by identity and updates
their metadata regardless of the account performing the refresh. This change
removes that temporary refusal while preserving bounded ingestion, metadata
merging, logo handling and the existing account-specific cleanup implementation.
Original Dispatcharr authors and contributors retain all application, model,
task and algorithm credit.

One Source cohort passes four natural refresh roots: A imports, B attaches and
updates the same global rows, A prunes, then B prunes. B is a separate account
using the same controlled provider transport. A declared fixture step after A
creates exactly one Episode and its A-owned Episode relation; only those two
tables and their two sequences change.

B's update keeps A's relations and Episode data intact. A's prune removes A's
relations, including its Episode relation, while the global Series and Episode
remain owned through B. B's final prune removes the remaining global Movie,
Series and Episode. Orphan logo rows remain, matching Source. Other accounts'
relations and timestamps remain unchanged throughout each selected-account root.

| Validation | Current result |
| --- | --- |
| Source shared-account cohort | Four cases pass; actual exit 0 in 106.852 seconds |
| SDK67 | Formatting, workspace check, strict Clippy and workspace tests pass |
| Default tests | 1,163 passed, zero failed; 784 fixture-dependent tests ignored |
| Physical SQL | 42 statements and 18 wrong-bind controls pass |
| Candidate49 packaged build and runtime | Build and all six package families pass |
| Combined Native eight-context cohort | All eight pass; actual exit 0 in 468.441 seconds |
| New application performance | Pending |

The three-file Rust change removes two obsolete capability-refusal queries and
their four bind controls. Every remaining statement contract is retained. Model
updates, sequence allocation, no-op suppression and cleanup SQL remain unchanged.
Series still require a provider owner; Episode relations must retain coherent
provider-account, provider-Series and global-Series foreign keys. Catalogue and
metadata bounds, absent external IDs and empty-language scope remain in force.
The removed default unit test covered only the obsolete refusal classifier.

Root independently verifies two exact Source containers, two captured host
process generations and the Source project absent. The read-only physical peer
passes Ready/Close with statement caching disabled, no business DML and unchanged
default schema. Its exact container, eight captured host process generations and
project are independently absent.

Committed source `64c35d3d6389e14fc586ae7fa05d04a0362c57e4` builds in
303.385 seconds as image
`sha256:1b0663e765891ccf930b6cc4b37b4c944002304fc91b56700739402bfc876e89`.
All 1,445 Rust inputs match SDK67; the production Main ELF differs from
Candidate48. All six package families pass, and all 55 exact package containers
are absent. The unchanged React assets and original credits remain verified.

The combined run passes all eight contexts in 468.441 seconds, with EOF and
process-group settlement. It retains the seven prior workflows and adds all four
shared-account roots on the actual packaged Main. Strict comparison validates
full SQL/support data, all eleven sequences, the declared fixture seed, task,
provider TLS, notifications, catalogue output and narrowly bound HTTP origins.
Surviving selected-account relations have causal last_seen timestamps; unrelated
owner rows stay exact. B's update and A's prune preserve the entire Episode row,
A's prune removes only its Episode relation, and B's last-owner prune removes the
global rows and Episode. Root independently verifies ten exact containers, ten
captured original host process generations and the project absent.

One binder preflight failed before application startup because it expected a
separate stdout reference rather than the existing receipt's stdout field. Only
the binder was corrected; the frozen 175-file driver, Source authority and image
were unchanged. No runtime or Source failure is inferred from that preflight.

Evidence: [Source authority](../scripts/rust-integration/shared-metadata-source1/README.md),
[SDK67](../scripts/rust-integration/milestone-batch49/SDK/summary.json), and
[physical SQL](../scripts/rust-integration/milestone-batch49/physical/README.md),
[package proofs](../scripts/rust-integration/milestone-batch49/package/inventory.json),
and [eight-context acceptance](../scripts/rust-integration/milestone-batch49/application/index.json).
Public eight-context reproduction, cross-provider transport variations, larger
catalogues, additional metadata domains, performance and all complete M0–M9
milestones remain open. Candidate48's public seven-context workflow retains its
own image and scope.


The additive public `catalogue-eight` workflow is now prepared in
[catalogue-shared-cohort](../scripts/rust-integration/catalogue-shared-cohort/README.md).
It retains the existing public seven-context and default workflows unchanged.
Its 33 Python payloads use only published, credential-safe Source projections,
ten shared SQL snapshots and Source origin evidence; no private raw50 captures
or fresh known Source business execution are needed. Four owned credentials must
remain exact initial values before token mapping, while bootstrap credentials
retain exact published digests. The exact two-row/two-sequence seed and observed
same-A-account-ID restriction remain explicit. All eleven sequences, causal
selected-owner clocks, unchanged unrelated-owner rows, independent UUIDs, task,
provider/HTTP/ordered notifications and strictly origin-bound cache URLs are
compared.

Host validation on the accepted Candidate49 captures passes all four shared
phases. Nineteen corruption controls reject credential drift, bootstrap changes,
seed row/table/sequence/script changes, copied UUIDs, unrelated-owner timestamp
changes, incorrect last-owner cleanup and altered cache/provider URLs. All four
existing launcher admission controls pass. The clean public eight-context
application run is still pending; these host checks do not qualify it.

Use the existing public launcher from a clean committed checkout whose Rust
inputs match the chosen Candidate49 build record:

```sh
python3 -B scripts/rust-integration/recording-connect-cohort/prepare.py prepare \
  --workflow catalogue-eight \
  --output /tmp/dispatcharr-public-eight \
  --build scripts/rust-integration/milestone-batch49/package/build.json \
  --source-archive /path/to/Original.tar
python3 -B scripts/rust-integration/recording-connect-cohort/prepare.py run \
  --plan /tmp/dispatcharr-public-eight/plan.json
```

The pinned Original archive SHA256 is
`f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff`.
The candidate image and pinned Source image must be available to the selected
Docker daemon. Preparation verifies image/source and helper hashes; execution
performs its actual UID65532 readability probe and owns bounded fixture cleanup.
The `--source-archive` input is the pinned Original archive, separate from the
clean candidate checkout; the candidate checkout needs no `.git` directory.
This workflow accepts no performance or complete migration milestone.
