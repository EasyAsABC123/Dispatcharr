# Copied PostgreSQL cutover and rollback

Original Dispatcharr authors and contributors retain full application credit.

The exact Candidate110 and Candidate111 rehearsals restored a transactional
copy, reused the same front door and authenticated session, and kept the
Native channel edit in the copy. Original's untouched database matched its
complete pre-cutover fingerprint before restarting. Both Compose projects
retired all five owned containers.

Both strict controllers exited 1 after restarting Original. The independent
Original-only restart reproduced four timestamp-only writes on the same rows.
One system notification, two named periodic tasks and the Beat change marker
updated. Schema, sequence values and row counts stayed unchanged. The later
authenticated read added no changes.

The results record the passed copied-data rehearsal and the strict post-restart
fingerprint failure for each image. The integrated M9 release gate remains open.

[Exact rollback receipts](../scripts/rust-integration/milestone-batch111/copied-postgres-rollback.json)

The exact d608 replay repeated the same rollback workflow and separately measured the Rust copy's startup writes. [d608ff75 copied-data rehearsal](rust-m9-d608ff75-copied-rollback.md)

## C74 same-seed control and copied cutover

One quiescent Original donor supplied an identical transactional PostgreSQL
archive to two fresh projects. The Original-only control ran first and
retired before the C74 arm. C74 served the same authenticated session,
channel and media through the trial's unchanged loopback front door.
Its channel-name edit stayed in its own restored copy. The strict Original
fingerprint still matched the quiescent baseline immediately before
fallback restarted Original.

Both Original starts changed exactly four preexisting timestamp cells.
The backup and plugin repository schedules changed `date_changed`,
the Beat marker changed `last_update`, and the developer notification
changed `updated_at`. The complete keyed projection of all other fields,
schema, sequences and row multiplicities stayed equal. The authenticated
read after Original became ready introduced no changes. The independently
observed strict controller exit was **1** because the complete
post-restart fingerprint differed. The separate four-cell diagnostic
passed. The integrated M9 and production fallback gates remain open.

The donor, Original-only arm and C74 arm removed two, two and five
owned containers respectively, with exact absence of all seven observed
host process generations and empty project inventories. An earlier
relative path caused the donor start and cleanup commands to exit 1.
The next run completed and retired the donor, then the deferred
Original arm failed before guest startup evidence and retired cleanly.
The final run used one complete read-only verification mount. The raw
engine error from the deferred failure remains unconfirmed.
[C74 same-seed rollback receipt](../scripts/rust-integration/milestone-batch113/c74-same-seed-postgres-rollback.json)

## Original-only startup timestamp witness

A fresh pinned Original donor supplied one quiescent PostgreSQL archive to
a separate Original-only project. The restored copy matched its full
pre-start fingerprint. Each boundary paired the complete strict and keyed
observations with the four named timestamp reads in the same read-only
repeatable-read PostgreSQL transaction.

All four timestamp values advanced within the independently measured
Original startup interval. The backup and plugin repository schedules
changed `date_changed`, the Beat change marker changed `last_update`,
and the developer notification changed `updated_at`. All other keyed
fields, row multiplicities, observed schema and sequence values stayed
equal. The same authenticated channel read on the same front door added
no database changes. Both projects retired all four owned containers and
their observed host process generations, with empty project inventories.

The Source-only witness exited 0. The earlier paired controller's strict
full-fingerprint exit 1 remains recorded above. The production rollback
and full M9 gates remain open. Original Dispatcharr and dependency authors
retain full credit.

[Redacted Source-only witness](../scripts/rust-integration/milestone-batch113/original-copied-restart-timestamps.json)

## c606 copied rollback rehearsal

One quiescent pinned Original donor supplied the same transactional
PostgreSQL archive to an Original-only control and the c606 trial.
The control retired before the trial. Native served the restored
session, channel and media on the trial's loopback front door, applied
the channel edit to its own copy, and stopped normally. The edit
remained in that copy after Original restarted. Original's database
matched the complete pre-cutover baseline immediately before its
restart and returned the previously authenticated channel on the same
front door.

Four fused read-only PostgreSQL observations in each arm showed that
the same four Original timestamp cells advanced in each independently
measured startup interval. Every other observed field, row
multiplicity, schema component and sequence value matched. The
authenticated read after readiness added no SQL change. The
independent strict comparator exited **1** on the complete
pre-restart and post-readiness fingerprints. The separate
startup-aware copied rehearsal exited **0**.

All three projects recorded Compose teardown exit 0 and independently
verified retirement of nine owned containers and seven observed host
process generations. Their container, network and volume inventories
were empty. Full M9 and production rollback remain open. Original
Dispatcharr and dependency authors retain full credit.

[Redacted c606 copied rollback receipt](../scripts/rust-integration/milestone-batch113/c606-scoped-copied-rollback.json)

## 82414407 copied rollback rehearsal

One pinned Original donor supplied a 244,634-byte PostgreSQL archive to the
Original-only control and the trial. The control retired before the trial
started. Native used a separate database for its channel edit and stopped
before Original resumed on the trial's same loopback front door. Both
applications returned the restored authenticated channel. The Native copy
kept its edit, and the restored media fixture bytes matched across owners.

Four read-only PostgreSQL observations in each restored arm confirmed that
the same four Original startup timestamp cells advanced within each arm's
measured startup interval. The complete observed pre-restart state
matched the restored seed. All other observed SQL fields, row
multiplicities, schema and sequence values matched, and the authenticated
read added no SQL change after readiness.

The independent complete fingerprint comparator exited **1** after those
timestamp changes. The scoped copied rehearsal exited **0**. All three
Compose projects recorded teardown exit 0, retired their nine owned
containers and seven tracked host process generations, and had empty
container, network and volume inventories. Full M9 and production rollback
remain open. Original Dispatcharr, Django and dependency authors retain
full credit.

[Redacted 82414407 copied rollback receipt](../scripts/rust-integration/milestone-batch113/82414407-scoped-copied-rollback.json)

## 8a821e27 copied rollback rehearsal

One pinned Original donor supplied a 244,618-byte PostgreSQL archive to the
Original-only control and the trial. The control retired before the trial
started. Native used a separate database for its channel edit and stopped
before Original resumed on the trial's same loopback front door. Both
applications returned the restored authenticated channel. The Native copy
kept its edit, and the restored media fixture bytes matched across owners.

Four read-only PostgreSQL observations in each restored arm confirmed that
the same four Original startup timestamp cells advanced within each arm's
measured startup interval. The complete observed pre-restart state
matched the restored seed. All other observed SQL fields, row
multiplicities, schema and sequence values matched, and the authenticated
read added no SQL change after readiness.

The independent complete fingerprint comparator exited **1** after those
timestamp changes. The scoped copied rehearsal exited **0**. All three
Compose projects recorded teardown exit 0, retired their nine owned
containers and seven tracked host process generations, and had empty
container, network and volume inventories. Full M9 and production rollback
remain open. Original Dispatcharr, Django and dependency authors retain
full credit.

[Redacted 8a821e27 copied rollback receipt](../scripts/rust-integration/milestone-batch113/8a821e27-scoped-copied-rollback.json)
