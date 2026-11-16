# Candidate32: normal restore and single-owner Main shutdown

Original Dispatcharr, Django, Celery, PostgreSQL and dependency authors retain
full application and library credit. The existing React frontend is preserved.

The [SDK42 evidence](../scripts/rust-integration/milestone-batch32/inventory.json)
records all four workspace gates passing: formatting, compilation, strict
all-target Clippy and tests. Actual totals are **1,106 passed, zero failed and
783 ignored**, across 180 test blocks. The frozen snapshot contains 1,411 inputs,
including 1,405 Rust-tree files. Sixteen changed paths were installed exactly
from that tested snapshot; unrelated working changes were excluded.

Main31 exposed a signal/server race: the graceful callback started physical
cleanup and published its task, while the completed-server branch could start
cleanup again. The first plugin shutdown succeeded, and the second encountered
closed actors. Main now joins the actual published cleanup task when the server
branch wins. Three regressions exercise simultaneous readiness, a pending owner,
actual cleanup failure and the unsignaled fallback. Closed actors are not
reinterpreted as successful shutdown. Actual Docker shutdown remains to verify.

The normal restore path now connects the admin REST request, shared broker,
registered backup worker, PostgreSQL archive tools and native current-schema
reconciliation. It fences the existing configured pool before destructive work,
uses the same Auth writer and task SQL owners, and physically retires old pool
sessions rather than creating another pool. Current pinned migration identities
are checked; missing content types and permissions follow captured Django
natural-key rules. Known task failures preserve Source's SUCCESS result carrying
a failed dictionary. Ambiguous physical or SQL outcomes remain unsettled.

This is a bounded current-schema slice. Historical migrations and enabled or
historical plugin restoration remain guarded. Restore success, partial-admission
cancellation, plugin characterization and unchanged React workflows have prepared
acceptance callbacks but have not run on this candidate. Candidate31's failed
application run remains [separate evidence](rust-milestone-batch31.md).

The immutable Docker build and six package checks below pass. The corrected
Source guide/plugin contexts and all ten Native application contexts are running.
Qualified Source transport, Redirect, file and earlier independent observations
retain their original scope. Operations and browser batches follow on the same
image, then paired performance with the unchanged measurement criteria.
No complete milestone or new performance gain is accepted here.


## Candidate32 package checks

The [package inventory](../scripts/rust-integration/milestone-batch32/package/inventory.json)
binds committed runtime `143c8638540ec3105d774a82f947289c5a72367e` to image
`sha256:8935b95d644e1e93c27255572101d4eb1064a6685c9f5f4f5429159c1afa02dc`.
The BuildKit build actually exits0 after134.010 seconds. Six sequential package
checks exit0: archive/assets, crypto, PostgreSQL, regex, full Python
compatibility host and VLC. All1,902 archived files match Git, including all1,405
SDK42 Rust inputs and495 unchanged React inputs. Installed assets preserve
Original attribution and dependency notices; the production entrypoint/probes
run as UID65532. Independent read-only inventories verify all53 exact owned
container IDs absent after all six waits complete.

These checks cover packaged bytes, dependency execution and the existing useful
Source/candidate VLC media controls. They do not verify normal restore, enabled
plugin execution, actual Main shutdown, complete application workflows or new
performance gains. Those gates remain open on this image.
