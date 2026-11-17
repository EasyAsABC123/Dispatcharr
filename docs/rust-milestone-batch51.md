# Rust milestone batch51: ordinary catalogue domains

Original Dispatcharr contributors and dependency authors retain all application,
model, task, protocol and algorithm credit. React remains unchanged. This batch
covers international catalogue values, ordered identity handling and ordinary
catalogues spanning Source's 1,000-row transaction boundary.

| Validation | Current result |
| --- | --- |
| International Source lifecycle | Five refresh roots pass |
| Ordered-identity Source lifecycle | Seven refresh roots pass |
| Catalogue-scale Source lifecycle | Five refresh roots pass |
| Rust port | Prepared privately; combined compiler and runtime gates pending |
| New paired performance | Not run |

The first grouped controller exits 1 in 232.145 seconds after completing the
international and ordered-identity contexts. The scale fixture references a
missing helper before any scale business root executes. That failure remains
recorded. After repairing the helper, a separate scale-only controller exits 0
in 121.531 seconds; the twelve completed Source roots are not replayed. These are
seventeen observed roots across two controllers, not one successful seventeen-root
controller. Each run has independently verified exact container, captured host
process-generation and Compose cleanup; UID probe process-generation limitations
remain explicit.

International observations cover fresh import, repeat, metadata replacement,
sparse updates and prune without Unicode normalization. Ordered observations
cover forward/reverse cross-key provider duplicates, overlapping identity lookups onto one stored row,
secondary-ID rebinding and either kind continuing after the other's caught batch
failure. The persisted name, metadata winner, relation flags, primary keys and
sequence allocation remain part of the contract.

Scale observations cover 1,000 rows, 2,001 rows, repeat, a middle-batch uniqueness
failure and prune for both content kinds. Model counts progress through
1,000 / 2,001 / 2,001 / 1,001 / 3. Model sequences finish at 2,003 and relation
sequences at 2,001. Each parent succeeds, performs five provider requests and
queues no children. The failed middle chunk rolls back while later chunks and
final stale-owner cleanup continue. This establishes per-chunk transaction
behavior; processing the entire document in one transaction would differ.

The [safe Source authority](../scripts/rust-integration/catalogue-domains-source1/README.md)
retains all rows and all eleven sequences in fifty lossless compressed SQL
snapshots and three typed projections. Only four hundred nonnull account login
fields are redacted. Complete API pagination, provider-response digests and
large-snapshot file custody are verified. The
[controller evidence](../scripts/rust-integration/milestone-batch51/Source-runs/index.json)
keeps both actual exits and the original harness failure. Compressed evidence
avoids duplicating tens of megabytes of repeated JSON while retaining canonical
uncompressed hashes and all 405 raw capture references.

The proposed Rust join slices the owned provider array before category filtering
or deduplication, processes and settles each raw 1,000-row chunk, and performs
reconciliation and cleanup once. Scoped identity lookups replace whole-catalogue
snapshots. SQL must preserve Source's ordered conflict handling and first queued
metadata update while keeping stored-name behavior. The combined SDK, physical
SQL, actual UPDATE semantics, Docker packages and complete affected application
cohort must pass before accepting that implementation.

Language reconciliation, wider logo domains, categories beyond the captured
admission, other provider value/fault domains, public reproduction, performance
and every complete M0–M9 milestone remain open. Candidate50's accepted image and
public nine-context workflow retain their separate scope.
