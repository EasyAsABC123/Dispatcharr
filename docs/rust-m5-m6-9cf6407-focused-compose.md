# Channel Groups and canonical scanner Compose checks

Original Dispatcharr authors and contributors retain full application credit. See [upstream attribution](../ATTRIBUTION.md).

The main comparisons used ordinary Rust revision `9cf640701dacea07ea5f394ce7b18a6fa7c8c088`. Exact image and proof hashes are recorded in the [machine-readable evidence](../scripts/rust-integration/m5-m6-9cf6407-focused-compose.json).

## M5 Channel Groups across restart

The 12-request sequence matched both backends for normal login, create, list and detail, PATCH and PUT, fresh login and read after restart, claim and unclaim, delete, and the final 404. Typed response bodies, literal `Allow` headers, current PostgreSQL process identity, group ownership and unrelated rows matched. Both serving restarts and retirement of the five owned container generations passed.

## M5 two-request guide edit

A separate paired check on `ad6babd` replayed the two HTTP requests sent by the React editor. Set EPG linked the second guide and published one programme job. The detail PATCH changed the channel and reordered its existing streams while preserving the selected profile and the existing link rows. It published no duplicate programme job. The next XMLTV read matched Original, and both fixture arms retired.

## M6 canonical `scan-files`

Both backends ran one naturally due occurrence of the saved stock 20-second scanner schedule. The scanner and its linked M3U group task reached `SUCCESS`. The pair matched result contents, parent and child lineage, scanner account state, the cache marker derived from file mtime, and a persisted run count of one. Both isolated fixtures retired. The Rust run used explicit Selected/Core ownership.

An earlier Original fixture could not format its generated arming probe and stopped before it enabled the schedule. It retired cleanly. The corrected fixture exercised and compiled the generated probe in an offline control before repeating the real run.

Additional account, channel and media workflows, the default worker graph and comparative resource gates remain open.
