# M3U, XMLTV, and backup Compose comparisons on 9cf6407

Original Dispatcharr authors and contributors retain full application credit. See [upstream attribution](../ATTRIBUTION.md).

The checks used ordinary Rust revision `9cf640701dacea07ea5f394ce7b18a6fa7c8c088` and the measured Original fixtures. Exact image, workload and teardown hashes are in the [machine-readable evidence](../scripts/rust-integration/m7-m8-9cf6407-ordinary-compose.json).

The [portable M3U and XMLTV packet](../scripts/rust-integration/ordinary-output-9cf6407/README.md) rechecks all 32 request and response pairs offline.

- **M3U export.** Both backends matched 15 ordinary GET cases plus four named-profile GET and POST cases. Exact playlist digests, selected headers, channel counts and download-event deltas passed. The first Original harness stopped after it tried to reopen a single-use fixture snapshot. An independent inventory verified that all resources it started had retired. The corrected fixture uses one snapshot per backend and separates the two event-delta checks inside it.
- **XMLTV export.** Both backends matched 13 cases in the shared fixture window. The checked forms include cold and warm GET and bodyless HEAD, bare and named paths, programme windows, and TVG and Gracenote ID selectors. Selected headers, expected channel IDs and XML body digests matched. Exact owned cleanup passed.
- **Backup inventory and export.** Both backends returned the expected empty and populated inventories and the correct archive bytes and download headers. A genuine PostgreSQL backup created with Original management seeded the two inventory entries in each fixture. Repeated reads preserved file and database state. All two Original and four Rust roles retired. Backup creation through the Rust worker and restore require separate acceptance.

These checks cover the listed output and inventory workflows. Wider protocol, backup jobs, restore and resource gates remain open.
