# Original aggregate M3U refresh reference

Original Dispatcharr, Django, Celery and dependency authors retain full application,
algorithm and protocol credit. React and application licensing remain unchanged.

[Source55](../scripts/rust-integration/milestone-batch38/Source55/qualification.json)
passes four fresh phases through the actual collection refresh HTTP action:
zero active accounts, two local STD accounts, XC missing credentials, and mixed
active custom/XC accounts with inactive local accounts. All four requests return
202. Four real parent tasks publish five direct children in total; inactive IDs
are excluded and every active ID is published without an added type/locked filter.
The default custom account remains active, locked and without a URL or file.

All actual parent and child Celery backends report SUCCESS. This includes caught
logical import failures: SUCCESS alone does not mean that ingestion succeeded.
The custom child returns the original failure string, changes status to error
with `No M3U source available (missing URL and file)`, sends exactly one
`downloading`/progress-100 update containing both message and error, and inserts
one `m3u_error` audit with the account name/message and null channel fields.
The [captured contract](../scripts/rust-integration/milestone-batch38/Source55/default-custom-contract.json)
preserves these effects; Source task bodies were not called by the test helper.
The original Beat was stopped only after generic startup qualification to keep
unrelated periodic work outside the characterization.

The full Source batch exited 0 in 99.514 seconds with both pipe EOFs and settled
process group. Independent cleanup verified two fixture containers, two recorded
original positive host PIDs, empty project inventories and the separate UID
probe container absent. The UID probe host generation was not independently
captured. These are functional reference timings, not performance measurements.

This exposed an important Native acceptance dependency: simply guarding or
filtering the custom account leaves aggregate refresh unsupported on fresh
installations. The private followup reuses the existing caught-transfer
status/audit/events/result path to implement the missing-input child case and
keeps preclaim checks read-only. Its new source requires SDK, schema, image,
package and Native four-phase qualification before acceptance. Cached/nonempty
and other child domains retain their own explicit guards and open criteria.
Source55 establishes no Native, complete M6 or application performance pass.
