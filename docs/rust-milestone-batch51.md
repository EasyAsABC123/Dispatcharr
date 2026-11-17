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
| SDK74 | All four gates pass; 1,174 tests pass, 785 ignored |
| Physical SQL / UPDATE semantics | Readonly47/30 and separate temporary rollback4 pass |
| Docker / packages | Image passes; all six package families pass with 55 CIDs absent |
| Full Native12 | Passes; actual exit 0 in 842.643 seconds |
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

The Rust join slices the owned provider array before category filtering
or deduplication, processes and settles each raw 1,000-row chunk, and performs
reconciliation and cleanup once. Scoped identity lookups replace whole-catalogue
snapshots. SQL must preserve Source's ordered conflict handling and first queued
metadata update while keeping stored-name behavior. SDK74, the separate physical checks and all six Docker package families now pass.
The complete affected Native12 cohort now passes separately.

Language reconciliation, wider logo domains, categories beyond the captured
admission, other provider value/fault domains, public reproduction, performance
and every complete M0–M9 milestone remain open. Candidate50's accepted image and
public nine-context workflow retain their separate scope.


[SDK74](../scripts/rust-integration/milestone-batch51/SDK/summary.json) passes
formatting, check, strict Clippy and workspace tests: 1,174 passed, zero failed,
785 ignored, across 180 result blocks. All 1,452 inputs, including 1,446 Rust
inputs, remain unchanged during those gates. SDK72's internal visibility
compiler failure and SDK73's strict-only lint failures remain recorded; SDK73's
passing workspace result does not promote its failed four-gate batch.

The [physical batch](../scripts/rust-integration/milestone-batch51/physical/README.md)
exits 0 in 14.054 seconds, with both EOFs and settled group. Its read-only
connection prepares 47 exact production statements and rejects 30 meaningful
wrong-bind controls with caching disabled and Ready/Close success. That gate
executes no DML. A separate compiled test executes four production Movie/Series
UPDATE controls on transaction-local shadow tables, truthfully recording DML.
The first name-only queue preserves the full stored row despite a later metadata
change; reversed order changes only description. Rows affected are respectively
zero and one for each kind. Explicit IDs and UUIDs avoid sequence allocation.
The transaction rolls back, temporary tables disappear, and public model rows
and sequence catalog remain exact. The default 62-table/59-sequence schema is
byte-identical across both invocations. Root verifies one exact container, eight
captured original host generations and the project absent.

Eight canonical Rust paths are published as
`63e6672525d6fa58d8faeb424ea14051b56552f1`, byte-identical to SDK74. The Candidate51 image and all six package families now pass separately;
Native12 also passes; public12, performance and full milestone acceptance remain
pending. Original Dispatcharr and dependency authors retain all credit.


The published source builds in 302.212 seconds as
`sha256:f74847428a5987e923bcc255865b0a3a07dd504619b8b9622c2503aa2692efe9`.
All 1,446 Rust inputs match SDK74. Actual Store and Main recompilation is
verified, and Main ELF `d1ac47cf` differs from accepted Candidate50. All six
[package families](../scripts/rust-integration/milestone-batch51/package/inventory.json)
pass in a separately consumed 24.562-second batch, with both EOFs and settled
group; 55 exact package containers and their scoped inventories are absent.
The unchanged React assets, original contributor credit and runtime dependency
closures remain verified. Fresh UID180 readability passes. The complete
Source0/Native12 cohort now passes on this image, adding the seventeen
Source-characterized roots to the unchanged old nine contexts. No Source
business root is replayed. The application result is recorded separately below;
package success alone establishes no public12, performance or full milestone
acceptance.


The [complete Native12 application run](../scripts/rust-integration/milestone-batch51/application/index.json)
exits 0 in 842.643 seconds, with both EOFs and settled group. All twelve contexts
pass. All five international, seven ordered-identity and five scale roots pass
strict comparison against retained Source observations, including complete
model/support rows, exact primary keys and all eleven sequences, causal/stable
clocks, independent UUIDs, provider bytes, ordered events, full pagination and
large-snapshot file custody. Pagination origins map only between verified actual
fixture endpoints; path/query bytes remain exact. No PK masks or Source-oracle
changes are used. Source's original controller exit 1 and separate scale exit 0
remain preserved. The older nine contexts also pass, including two viewers
sharing one upstream. Root verifies fifteen exact containers including UID,
fourteen captured original host generations and the project absent; UID's
uncaptured initial positive generation remains explicit. Clean public12
reproduction, new paired performance and every complete migration milestone
remain open.
