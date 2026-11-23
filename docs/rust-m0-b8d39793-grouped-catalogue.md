# Grouped catalogue Source and b8d package differential

The corrected Original run completed with preflight/run/cleanup/proof/campaign
exit 0. All 102 signed requests returned HTTP 200 and preserved the scoped SQL
and broker state. This includes the selected persisted entities in all 68 GETs
across 34 declared operations. The 17 HEAD and 17 OPTIONS results remain
observations attached to the existing route identities. Original ran as pinned
`1241b3f7` on the internal direct uWSGI listener from the existing isolated
fixture. The image, original source archive, signed roles, exact route roster,
guest bytes, database state, proof, and cleanup were independently verified.

Exact Linux AMD64 Native `b8d39793` used the same privately seeded catalogue
and the retained Original receipt. No Original business requests were replayed.
Native preflight and cleanup exited 0. Its run, proof and campaign exited 1
after recording all 102 requests. All four Native fixture container and process
generations were absent after Compose down, and no project containers, networks
or volumes remained. The same scoped SQL and broker state held throughout.

| Observed method | Original | Native | Full compared cells matching |
| --- | --- | --- | ---: |
| GET | 68 × 200 | 54 × 200 and 14 × 404 | 28 |
| HEAD | 17 × 200 | 17 × 200 | 8 |
| OPTIONS | 17 × 200 | 7 × 200 and 10 × 405 | 7 |

Both roles received Native 404 on those same seven declared aliases. The
canonical reads all returned 200 and showed the selected SQL entities. No
successful JSON response differed on typed content.

- The 404 aliases are Channel Group detail, M3U Server Group list/detail, EPG
  Programme list/detail, and EPG Data list/detail.
- The ten 405 responses are administrator OPTIONS on canonical list/detail
  routes for Output Profile, Channel Profile, M3U Server Group, EPG Programme
  and EPG Data.
- All 59 compared differences included Native `Vary: origin` versus
  `Vary: Accept, origin`. Allow differed in 57 cases, and content length in 24.
  Counts across axes overlap.
- All 17 Native HEAD responses were bodyless. Original direct uWSGI sent
  the corresponding administrator GET entity bytes in all 17. That listener
  diagnostic is separate from the comparison of matched status, representation
  metadata and announced length.

The machine audit retains exact per-request role, route, alias, statuses,
headers and typed digests. It gives no coordinator acceptance or performance
credit. The failed first Original attempt remains separately preserved.

Original Dispatcharr, Django, DRF and dependency authors retain full credit.

[Per-request comparison receipt](../scripts/rust-integration/m0-b8d39793-grouped-catalogue.json)
