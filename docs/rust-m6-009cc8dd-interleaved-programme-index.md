# M6 ordinary interleaved programme-index persistence

Pinned Original and the exact Rust `009cc8dd` AMD64 package each imported the same local synthetic XMLTV file through one signed request. Both returned 202 and published the real Refresh job and its Index and Series children. The Index result in Redis DB2 was SUCCESS with a null result, and its UUID and parent joined the actual DB1 job and owned source.

Each fixture started with two mapped EPGData rows and no index or programme rows for that source. At the first observed Index terminal, a single read-only SQL transaction found the exact offsets, both original parser IDs with updated channel names, and all five ProgramData IDs linked to the expected channel rows. A second read-only transaction after the application and workers exited returned the identical typed data. This observation is bounded by the polling and SQL reads.

| Verified observation | Original | Rust |
|---|---:|---:|
| Ordinary signed import status | 202 | 202 |
| Real Refresh, Index and Series jobs | 3 | 3 |
| Owned EPGData rows | 2 | 2 |
| Owned ProgramData rows | 5 | 5 |
| Channel A byte offsets | 186, 620 | 186, 620 |
| Channel B byte offsets | 476, 765 | 476, 765 |
| Retired containers and host generations | 2 | 3 |

The five Programme tags appeared in A/A/B/A/B order. Both database rows retained each channel’s block starts and an empty over-cap interleaving list. Both scopes retired their owned Redis locks and broker observer. All five owner phases returned zero, and the exact Docker container, network, and volume inventories were empty.

This qualifies one ordinary positive programme-index case. The formal programme-index failure cases, whole M6 batch, and performance measurements remain open.

Original Dispatcharr, Django, Celery, Redis, React, and dependency authors retain full credit.
