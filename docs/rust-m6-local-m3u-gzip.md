# Local gzip Refresh All parity

The original Dispatcharr authors and contributors retain full application and protocol credit. Celery authors retain dependency credit.

The C112 replay used the retained image and the selected worker configuration. On both backends, an admin submitted `POST /api/m3u/refresh/` and received HTTP 202 with the same JSON response. The durable broker observation recorded one aggregate task and two account children. All three finished successfully. The two local standard accounts imported one stream each with matching stream projections. Both preserved their default group memberships and gained one enabled imported group each. The default properties and legacy empty properties were preserved. Fixture A was a concatenated two-member gzip playlist. Fixture B was the plain control. The encoded and decoded input hashes matched across backends.

Both actual child controllers exited 0. All five owned container and host generations retired across the two independent phases. The first attempted pair exited 1 before Original reached the request. Its guest startup observer exited 2 because it could not traverse a copied fixture mount. Original cleanup passed and Native never started. The retry repaired directory traversal and reused the same built image and frozen file content.

The accompanying two-file change accepts local `.m3u.gz` through the existing bounded M3U ingestion path. It bounds compressed and decompressed bytes separately and handles concatenated gzip members. The scoped result covers the selected local standard worker path. Ordinary Main, remote or XC refresh, full M6 acceptance, and resource performance stay open.

[Redacted paired receipt](../scripts/rust-integration/milestone-batch112/local-m3u-gzip.json)

## Integrated a785 selected worker comparison

Pinned Original and the exact a785a2b7 Rust package each returned the stock
Refresh All HTTP 202 response. Each ran one aggregate and two successful
account children. Both backends imported one stream from a two-member gzip
playlist and one from the plain control. Encoded and decoded file hashes,
stream projections, account properties and enabled group memberships matched.

The Rust container used the selected Jobs graph with Beat disabled. Both
the manual-backup and Original M3U owner flags were absent. The two
controllers exited 0 and all five owned container and host generations
retired. The ordinary Main comparison with the retained Original worker
is documented separately. [Ordinary Main scope](rust-m6-refresh-all-original-worker.md)

[Redacted a785 selected worker receipt](../scripts/rust-integration/m6-selected-local-gzip-a785.json)
