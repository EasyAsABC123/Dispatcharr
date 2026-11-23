# M5 signed channel profile duplication and membership

Pinned Original and the exact Rust `48181ab0` AMD64 package matched the ordinary API workflow used by the unchanged React channel-profile editor. Each copied fixture used signed Administrator and Standard accounts. The fixture assigned the Standard account to the original profile before its signed requests.

The Administrator created an empty profile. The Standard account enabled the first owned channel through its assigned profile, and the Administrator explicitly disabled the second. The third had no membership row in the original. The duplicate created a distinct profile with an enabled first channel and disabled rows for both the source-disabled channel and the source-absent channel. The Standard account’s signed read of the unassigned copy returned 404 and preserved both profiles.

The Administrator bulk-edited three existing rows on the copy. Their IDs stayed the same, and the original profile’s membership IDs and states stayed intact. Final signed reads verified the original’s first channel and the copy’s second and third channels. The same SQL projection was present in a read-only transaction after application exit.

| Observation | Original | Rust |
|---|---:|---:|
| Exact signed API calls | 9 | 9 |
| Exact read-only SQL snapshots | 6 | 6 |
| Existing copy memberships changed | 3 | 3 |
| New memberships during bulk edit | 0 | 0 |
| Retired containers and host generations | 2 | 3 |

Both arms returned the status sequence 201, 200, 200, 200, 201, 404, 200, 200, 200. All five owner phases returned zero. Exact scoped container, network and volume inventories were empty.

This qualifies the signed API and copied-data workflow. React browser interaction, broader role coverage, performance, and full M5 acceptance remain open.

Original Dispatcharr, Django, Celery, Redis, React, and dependency authors retain full credit.
