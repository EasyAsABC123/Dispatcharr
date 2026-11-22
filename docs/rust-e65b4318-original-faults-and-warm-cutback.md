# Original fault references and e65b4318 warm cutback

| Scope | Observed result |
| --- | --- |
| M6 Original broker reply loss | Redis committed the first provider rehash frame before the proxy withheld the acknowledgement. Original retried inside the first HTTP request. Both frames used the same task ID. The request returned HTTP 200, and the separate same-value request added no frame. The worker produced one successful result, merged the expected data, emptied the queue, and released the lock. The receipt records the observed refresh events separately. |
| M7 Original Xtream info recovery | Sixteen requests covered stale and missing movie and series metadata. All eight upstream 503 or invalid JSON cases returned fallback HTTP 200 and preserved cache SQL, clocks, queues, and results. All four cases recovered on the next request, and the following open read cached data without calling upstream. |
| M8 Original copied SQLite dismissal | Five requests verified personal notification dismissal, repeat and update behavior, and an admin-only visibility denial. The writes produced two actor-scoped rows. Repeat and update preserved the existing row ID and initial timestamp. |
| M9 e65b4318 copied PostgreSQL warm cutback | The Native writer retired before reconciliation. Original used the same process generation and signed session. One acknowledged fallback rename changed one channel row, while unrelated rows and schema matched. Both the workload and exact cleanup passed. |

The first reply-loss attempt stopped before the provider PATCH when the warm
web replacement left an extra task in the held queue. The reviewed retry
limited plugin discovery suppression to that replacement process. Its first
ordinary startup still verified all three expected job roots. Both attempts
verified exact cleanup.

The three Original scopes provide runtime references for further Rust
comparisons. The warm PostgreSQL scope applies to this exact e65b4318
package. The separate strict cold cutback remains open because ordinary
Original startup changed four persisted clocks in the retained test.

[Machine-readable evidence](../scripts/rust-integration/e65b4318-original-faults-and-warm-cutback.json)

Original Dispatcharr and dependency authors retain full credit for the
application, models, algorithms, and protocols.
