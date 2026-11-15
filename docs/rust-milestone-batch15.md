# Candidate15 natural plugin scheduling and durable ETA

Original Dispatcharr, Celery, Kombu and dependency authors retain full
application, algorithm, protocol and license credit. React remains unchanged.

Runtime `71055d14908c02588b205b564752a25f8cee9760` passes formatting, compilation,
strict Clippy and964 workspace tests (zero failures,756 ignored fixture tests,
173 result blocks). All1,237 inputs match Git:1,232 Rust inputs and five coupled
fixtures. The [SDK and physical evidence](../scripts/rust-integration/milestone-batch15/inventory.json)
retains the earlier compiler failure and failed physical test without relabelling.

Main now publishes the natural startup plugin refresh with its10-second ETA,
existing producer identity and shared Jobs publisher, respecting the existing
plugin autodiscovery opt-out and caught startup failure policy. The plugin
periodic schedule initializes independently of Beat using the existing scheduler
and SQL pool, preserving interval/delete behavior within its supported domain.

The normal dispatcher reserves typed plugin ETA frames durably into Redis
unacked storage. Waiting reservations hold the existing bounded ETA admission;
they release execution-family admission. Due execution returns through the same
normal dispatcher and six-task global budget. Exact reservation ownership gates
acknowledgement, warm restoration and bounded visibility recovery. A cursor
preserves foreign task authority while allowing bounded recovery progress.

Pinned actual Celery Request source establishes that revoked tasks are checked
before pool execution, and their backend metadata is written before acknowledgement.
The new path follows that ordering and retains the unacked reservation on backend
failure. Broader revocation, QoS, long-ETA, visibility/fairness and physical-fault
domains still require their declared application acceptance.

The additional real-Redis test passes exact FIFO reservation, stored tuple
ownership, warm restoration order, acknowledgement and honest final drain. Its
first execution reached the earlier wire assertions but failed drain because the
test still owned completed reservation guards. The corrected test releases them;
production drain is unchanged. Both disposable fixtures and their exact original
host generations retired with empty inventories. The actual passing test binary
is retained outside the shared compiler target and bound by hash.

This candidate contains the preceding provider/revocation implementation. The
prepared Candidate14 runtime batch was not executed; the next grouped cohort
combines provider, restart, idle-viewing and natural plugin scheduling on this
candidate. No Candidate14 or older image performance is inherited. Actual Main
acceptance, full M0–M9 and new paired performance measurements remain open.

The [production Docker build](../scripts/rust-integration/milestone-batch15-build/inventory.json)
is `sha256:f2f6010a20c370a4098417b0cdd65fac2f1057cbed2e393047545de259596674`.
All1,729 files match Git:1,232 Rust,495 React and two original credit files.
All114 installed original React/credit hashes and120 selected assets/notices
remain unchanged. UID65532, `/app` and the production entrypoint pass;
the ELF hash is `1cae9873215c9e4d5d92a79da2443c498d74b4b278df825891b0a90efd24815a`.
Crypto27-file/GPG2.4.8 and PostgreSQL17.11/528 component/license hash probes pass.
All probe containers retired. New grouped application and performance acceptance
remain separate pending gates.
