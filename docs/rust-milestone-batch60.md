# Candidate60: guide signals, sequence behavior and DVR producer readiness

Original Dispatcharr, Django, DRF, Celery, redis-py, redis-rs and dependency
authors retain all application, model, task, client, protocol and algorithm/code
credit. React and the original license remain unchanged.

Commit `44c885b9d0445c1a41619c5e5f22f20a27f35225` binds the final tested SDK100
Rust tree. [SDK100](../scripts/rust-integration/milestone-batch60/SDK/index.json)
passes formatting, workspace check, strict Clippy and workspace tests:
**1,216 passed, zero failed, 792 ignored**, across 180 result blocks. All 1,462
inputs, including 1,456 Rust files, remain unchanged. Seven Rust paths differ
from SDK97; one is the new production publisher control module.

The retained history records SDK98's actual failure: three explicit-auto-deref
Clippy diagnostics, with compilation and workspace tests passing. SDK99 passes
all four gates after those corrections. A subsequent Source25 sequence review
changes one SQL file; SDK100 passes all four gates with that final change.
Earlier results are not relabeled as tests of the final source.

The actual Source25 authority covers four channel creations, eight override
mutations and thirteen XML output reads. New EPG-backed channels and actual
override EPG identity changes invalidate cached output; XMLTV/no-source guide
assignments publish the existing unparented programme task with Source's
3600/3500 time limits. Dummy assignments and partial clears invalidate without
that task. Same-EPG updates and whole override deletion preserve their observed
Source cache behavior, including potentially stale XML after deletion. All
precommit SQL/cache/broker work and physical SQL close retain the request owner
through observer cancellation.

Existing custom-group association and override saves now avoid consuming extra
PostgreSQL sequence values. Source get-or-create checks for an existing relation
before insertion, and an ordinary existing override uses UPDATE rather than an
INSERT conflict path. Raw Source sequence witnesses remain intact. Concurrent
delete/save, SQL failure, rollback and broader model-hook behavior still need
qualification. Nonempty channel profiles, SD guide assignments, mixed stream/
scalar-plus-EPG updates and other unobserved compositions remain guarded.

Normal UUID and named immediate/recovery DVR producers have their own broker
manager, independent of consumer polling. Both now use the qualified read-only
PING readiness operation before the unchanged single PUBLISH Lua invocation.
Only a settled dropped PING may repeat; uncertain mutations never repeat. Queue
keys, frame bytes, capacity/error mapping, caller lease and deadlines remain.
Normal publication diagnostics use a typed UUID and fixed vocabulary; named keys
are not logged or projected into fabricated UUIDs. Two production RESP controls
cover both routes through closed readiness sockets, lost publication replies and
held PONG ownership through physical completion. This addresses a static idle
risk; it is not evidence of an observed Candidate59 due-publication failure.

The [fresh SDK100 real-Redis gate](../scripts/rust-integration/milestone-batch60/physical9/index.json)
passes all nine existing result/artwork preflight controls with zero failures.
Root consumes actual exit0 in **18.996 seconds**; the inner gate takes **18.875
seconds**. Input/source drift is empty, and independent verification finds the
exact container, original host generation and project resources absent. These
component controls do not establish built Main/DVR HTTP behavior.

The original Candidate57 CUT503 remains literal; its physical Redis error kind
was not captured. Source catches/logs recording scheduling and artwork faults,
while Rust reports503 for unsettled publication. That caught-fault HTTP parity
remains unqualified. Other cache/event/lock/producer managers are separately
listed in the [readiness audit](rust-redis-client-readiness-audit.md); hot data-plane
commands receive no blanket health probe.

The [actual Docker build](../scripts/rust-integration/milestone-batch60/build/index.json)
passes in **317.962 seconds for Docker**, **318.366 seconds for the complete Root
wrapper**, with both pipes and process group settled. Image
`sha256:fa2dc230d33eb72161fb81ebd43ba2261ae70f336ffe03c81f45f3c253d016bb`
has source tree `8f92a798b19bab4c36646cfc0bcae0b7cf91cebc8b15c7a62a0495a0bb918496`;
all 1,456 Rust build inputs match SDK100. All [six package families](../scripts/rust-integration/milestone-batch60/package/inventory.json)
pass with actual exit 0 in 26.664 seconds and 55 exact containers independently
absent. The combined 17-context Native workflow, including the new channel/guide
case, remains pending at this checkpoint. Separate SD
Source characterization has no accepted new lineup HTTP authority yet; its
pre-application manifest failure and later helper initialization failure remain
preserved. No complete M0–M9 milestone, full migration, full transport identity
or application CPU/memory improvement is accepted here. Earlier Candidate47/52
measurements are not carried forward as current gains.


A separate [offline credential component qualification](../scripts/rust-integration/milestone-batch60/credential-offline-component/index.json)
compares all nine retained observations and rejects fourteen negative controls.
The original current credential context remains failed: its Movie after snapshot
contains ten Source events versus seven Native events. Later retained snapshots
contain all ten events in both runs. Per-reader snapshots observe different
concurrent-stop phases. The scoped successor checks every intermediate row and
sequence against its actual timestamp-bounded complete lifecycle, requires all
three reader owners stopped and all six counters released, and retains all raw
IDs, timestamps and prefix differences. An explicit injective event identity
bijection checks every remaining field and observed FK; other SQL and ten
non-event sequences remain strict. Actual per-run gateway IPs are independently
bound to Docker namespace/network and published-port evidence.

Raw stop allocation order and gateway addresses differ and remain visible; full
literal SQL/event/header identity stays false. Retained complete-end evidence is
explicitly distinct from a new Source capture. A future Native successor adds a
fresh post-join event/counter SQL fence, without replaying Source business. This
offline component pass does not change the current whole17 controller or grant
application, milestone or performance acceptance.


The [restart recovery](../scripts/rust-integration/milestone-batch60/restart-recovery/summary.json) preserves Candidate60 whole17 as **interrupted**, with no observed terminal exit. Its retained report contains thirteen passed contexts, three failed contexts, and the aggregate Comskip context still running. Nested manual Comskip3 and positive4 captures passed; natural automatic MARK started without a terminal capture, and natural CUT did not start. Credential event snapshot ordering, guide broker composition and the provider multiple-fresh-logo guard remain separate failures. Prepared fixes do not relabel this run.

A host reboot retired the original command and Docker daemon. The original 3,121-input binding verifies 3,119 persistent files; two temporary helper scripts disappeared from `/tmp`, so complete input integrity cannot be claimed. The validation daemon was restored with its existing data and images. Three exact stopped containers were removed and project containers, networks and volumes were independently empty afterward. This recovery cleanup is distinct from successful original graceful teardown. All M0–M9 remain open; no current application performance gain is accepted.
