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
| Candidate49 packaged build and runtime | Pending |
| Combined Native eight-context cohort | Pending |
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

Evidence: [Source authority](../scripts/rust-integration/shared-metadata-source1/README.md),
[SDK67](../scripts/rust-integration/milestone-batch49/SDK/summary.json), and
[physical SQL](../scripts/rust-integration/milestone-batch49/physical/README.md).
The existing seven application contexts and the new shared-account context must
run together on the packaged candidate. Cross-provider transport variations,
larger catalogues, additional metadata domains, performance and all complete
M0–M9 milestones remain open.
