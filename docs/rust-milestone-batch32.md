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

Next validation uses one immutable Docker build and six package checks, then the
corrected Source guide/plugin contexts and all ten Native application contexts.
Qualified Source transport, Redirect, file and earlier independent observations
retain their original scope. Operations and browser batches follow on the same
image, then paired performance with the unchanged measurement criteria.
No complete milestone or new performance gain is accepted here.
