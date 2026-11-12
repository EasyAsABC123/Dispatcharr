# Original notification mutation observations

The pinned Original backend completed 48 notification PUT, PATCH and DELETE framework requests. All 1,449 predeclared checks passed, and both the Source shell and controller exited 0. [The portable evidence packet](../scripts/rust-contracts/notification-mutations-source/README.md) retains exact responses, SQL/cache snapshots, request clocks, inputs, comparison code and cleanup receipts. [Independent review](../scripts/rust-contracts/notification-mutations-source-review.json) recomputes the checks and verifies the input closure.

The capture uses Original revision `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`, its pinned image, real route resolution and persisted active actors forced through Django APIClient. It establishes framework behavior; credential acceptance and deployed TCP behavior need their own checks. Original Dispatcharr authors and contributors retain full application, algorithm and license credit.

| Observed behavior | Result |
| --- | --- |
| Anonymous / standard or network-denied administrator | 401 / 403 before malformed body parsing |
| Missing, inactive, expired or filtered detail target | 404 before body validation |
| Invalid required, unique, null, choice, boolean, date or length values | 400 |
| PUT with omitted optional fields; empty or read-only PATCH | 200, optional values retained and update clock recorded |
| PATCH making the saved row inactive or expired | 200 with saved-row response; subsequent lookup 404 |
| DELETE of a visible target | 204; only its dismissal children cascade |
| Truthy nonobject developer condition data | Actual final framework 500 with complete AttributeError metadata |

Across the fixed sequence, status counts were 401×3, 403×6, 404×9, 200×12, 400×15, 204×2 and 500×1. The checks also bind exact seed aliases, actors, dismissal triples, target identities, serialized bodies, unchanged sequences, other named tables and per-request update clocks. Both default-database audits cover the same eleven named tables and eleven sequences. Owned containers, networks, volumes, the exact former container and known host processes were independently absent after teardown.

The first attempt stopped before seed or requests because fixture receiver metadata contained a non-serializable weak reference. Its exit 1, raw traceback, original input seal and cleanup evidence remain published. The separate corrected executor changed receiver reporting only; it preserved the 48-case plan, application code and budgets. Publication required no Source replay.

The portable validator and 17 corruption controls pass; a fresh local reconstruction reproduces all 44 executed inputs, including the exact Original archive from local Git. These observations support the next Rust update/delete implementation. They add no Native parity, operation-register assignments or performance results. M0 and M5 remain in progress; image53 remains immutable while its independent package/Main gates run.
