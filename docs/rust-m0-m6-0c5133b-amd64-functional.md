# AMD64 functional evidence for 0c5133b

The candidate was application revision `0c5133b62dc9f2df0900f2228e25b75baecb6754`
with the ordinary AMD64 image
`sha256:e6d40f935400adfc8ccd64d31211b556cf3bf8dffc39f7fd823fbb5688d0f2dd`
and build manifest
`72c01c185cad19e515090807006f3f98fc034fb0459a981cebc759700278ee12`.
The Original revision was `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`.
The Compose fixtures ran in disposable projects on beepboop with exact
verification that each project's containers, networks, and volumes were
removed. Each scope below passed with literal exit 0 and records functional
coverage. Formal milestone acceptance and performance measurement remain open.

## M0 routine application reads

Original and Rust each returned nine real GET responses for Version,
Timezones, permissions, and filtered and paginated synthetic system events.
Seven cases per arm returned 200. Standard users got 403 for the permissions
catalog and system events, matching Original. The proof checked typed response
bodies, established the standard and administrator identities, and verified
that scoped PostgreSQL and broker state remained unchanged after each read.
The four method-unresolved contract IDs retain their current classification.

## M1 bounded streaming core

Cargo 1.90.0 ran the locked, offline library suites after the runner matched
1,664 Rust file hashes against the frozen build manifest. Stream Core passed
17 tests and Stream Runtime passed 16. Together the tests covered shared
payload retention, MPEG-TS framing across fragment boundaries, slow-reader
independence, cancellation and eviction, and in-process sharing and
sequence delivery to 100 readers.

## M5 browser and channel control

The unchanged React UI completed first-admin setup, login, and authenticated
Channels on Rust. Chromium exited successfully, recorded 29 API responses,
passed all 16 required API paths and access and refresh storage checks,
established the WebSocket, and reported zero browser exceptions. The
candidate started with zero public database tables and used a read-only root
with an owned writable data volume at `/data`. An earlier launch exited 1
and cleaned up. The successful retry used a short private `/tmp` directory
with a 64-byte Chromium singleton socket path, then retired the browser
process group and removed its scratch directory.

A second Compose fixture used authenticated admin requests to rename one
owned channel through Channel Batch and then restore its name. Fresh
Original and Rust requests each returned 200 for both PATCH operations and
their follow-up GETs. The matching response digests, intermediate database
change and restoration to the initial database digest were verified in both
arms.

## M6 natural linked XMLTV run

The ordinary Rust all-in-one deployment released one saved UTC cron
occurrence from its scheduler. It fetched the owned XMLTV fixture once,
wrote the expected PostgreSQL programme and index, and naturally dispatched
`build_programme_index_task` and `evaluate_series_rules`. The reducer matched
the fresh Native terminal, child lineage, stored outcomes, and drained queue
to the retained Original authority for this scope. The Original authority
was collected separately. The ARM64 paired proof is recorded in its own
report.

Full credit for Dispatcharr and the retained React application belongs to
the original authors and contributors. See [attribution](../ATTRIBUTION.md)
and the [license](../LICENSE).

[Machine-readable evidence](../scripts/rust-integration/m0-m6-0c5133b-amd64-functional.json)
