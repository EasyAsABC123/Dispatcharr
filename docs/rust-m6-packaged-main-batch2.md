# Packaged Main M6 batch on the whole milestone candidate

Original Dispatcharr and dependency authors retain all application, algorithm and
license credit. The React UI remains the original implementation.

The actual cohort used image
`sha256:e88cc73fb054d6b285a99018e70b60b072ea0b32d96408374e2109e6692ece65`
from committed `1dc16f8da8b04a0b119afebef16649b837c6e928`. This is application
execution evidence, separate from the preceding 882-test SDK check. No new CPU,
memory or throughput gain was measured, and M6 remains incomplete.

The unchanged packaged production Main ran as UID65532 with its EPG and version
migration flags enabled. PostgreSQL, Redis and the fixed version HTTP dependency
shared one isolated network namespace. No host ports or subnet were published.
One global fixture lease covered the sequential cohort. Fixture ORM setup used
bulk inserts to avoid Original model-signal/task execution. No Original API or
task handler was replayed; retained Source outcomes were reused.

Nine consumer cases passed 94 read-only semantic assertions:

- Five TVG cases: initial parse, repeat, missing EPG, dummy source and unmapped
  EPG. Checks retain programme IDs and full timestamps, compare fresh input
  timestamps, verify repeat business fields, no-op retention, actual backend
  values and the Original configured `AsyncResult` consumer, and observe lock
  and source-counter cleanup.
- Four version200 cases: no-update, update, repeat and malformed version.
  Four actual HTTP calls used the Source headers. No-update removed the current
  notice; update produced one administrator-only high-priority notice; repeat
  preserved its ID, creation timestamp, source and expiry while advancing the
  actual update timestamp; malformed input preserved every notice field. Exactly
  one clear event and one creation event were observed.

These nine cases inject projected retained Source envelopes into the owned
broker. They establish packaged Main **consumer** behavior, not a claim that a
Native HTTP producer produced those frames.

A separate real Native `POST /api/epg/import/` returned202. Its actual Lua LPUSH
contained the positive source ID and explicit `force:false`, matching the selected
Source37 publication shape. Main completed the Refresh root, its Index child and
its configured Series child. Actual backend metadata preserved parent/root
lineage; one future recording and two artwork children were produced. Both real
artwork frames remained at the FIFO head. They were not skipped or ACKed to reach
another task. This qualifies the positive producer/parent subset; it does not
pass all37 API shapes, artwork execution or the complete default graph.

The deliberate programme-insert trigger exposed a real remaining gap. PostgreSQL
reported SQLSTATE `P0001` and the programme rows rolled back unchanged, but Native
wrote no Source-required FAILURE result during the bounded observation. That
case remains failed. The original failed expectation and absent raw metadata are
preserved; no successful result or Python traceback was fabricated.

The first attempt exited1 before any consumer/API case because the extracted
Original source root retained0700 permissions under the host umask. UID65532
could not traverse the mounted path. The harness corrected public source parents
to0755 and guide inputs to0644; the Source application and Native image were
unchanged. Both attempts remain separate evidence epochs.

The second controller exited0 for complete capture, while its explicit functional
`passed` value remains false. All ten cleanup steps passed, the full default
62-table/59-sequence/schema manifest and DB7 cache were unchanged, 53 CLI
identities and three container identities settled, both Native and Origin stopped
normally with exit0, and fresh project/CID inventories were empty.

Three observer limits remain explicit:

- Redis MONITOR was closed with an unread tail. The Native root LPUSH is present;
  later child/result commands are not all in that journal. Raw backend, SQL,
  Channels and FIFO snapshots qualify their own observed scope.
- The observer's `new_backend_ids` list also includes the earlier TVG task whose
  backend value was absent. Qualification independently identifies actual stored
  root/child metadata; it does not call that extra ID a Native API publication.
- The final two artwork frames were captured before Native shutdown. There is no
  separate post-shutdown FIFO snapshot. Normal stop and pre-stop FIFO state are
  distinct observations.

The compact [evidence manifest](../scripts/rust-job-contracts/m6-packaged-main/manifest.json)
retains447 selected files (about4.9MB), the immutable helper closure, actual raw
results/events/SQL/FIFO, the failed startup and the failed P0001 expectation.
It excludes the reproducible Original tree, compiler output and raw JWTs.
The [read-only qualifier](../scripts/rust-milestone-batches/m6/packaged/qualify.py)
can validate the published attempt2 without starting any application.

Next work is a Source-backed P0001 failure-completion adapter, a production
artwork consumer without fixture-specific paths, the remaining default scheduler
and recovery domains, and full milestone-wide functional and paired performance
acceptance on the next coherent candidate. The tested e88 image is unchanged.
