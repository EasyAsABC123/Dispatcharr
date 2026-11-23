# M6 missing XMLTV programme-index worker

Pinned Original and the exact Rust `48181ab0` AMD64 package each exercised two missing local XMLTV file cases. The first source began without an index row. The second began with a real SQL-null index row and an existing timestamp. Both kept their copied source and parser identities intact. The signed current-programme lookup returned an empty 200 for the two missing paths.

In each arm, an ordinary scheduled `scan-files` task published both Index jobs. The DB1 journal bound each exact source argument and worker UUID to that scanner parent. Both matching DB2 results were SUCCESS with a null value. After each first observed Index terminal, a separate read-only PostgreSQL transaction found both source and index states identical to their baseline. The absent row stayed absent, and the existing SQL-null row retained its original timestamp. A fourth read after application and worker exit returned the same projection.

| Observation | Original | Rust |
|---|---:|---:|
| Signed current-programme response | Empty 200 | Empty 200 |
| Actual scanner parents and scoped Index children | 1 + 2 | 1 + 2 |
| Matching null Index successes | 2 | 2 |
| Exact SQL snapshots | 4 | 4 |
| Retired containers and host generations | 2 | 3 |

All five owner phases returned zero. The two source file leases and the broker observer retired in each arm. Exact scoped container, network and volume inventories were empty.

This qualifies ordinary MissingFile behavior for an absent index row and an existing SQL-null row. The already-deleted source and non-null existing index branches remain separate. Formal programme-index and whole-M6 acceptance, including performance, remain open.

Original Dispatcharr, Django, Celery, Redis, React, and dependency authors retain full credit.
