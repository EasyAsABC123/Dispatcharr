# AMD64 Core multipart writes and renderer rollback on d2c5e611

The pinned Original and exact packaged Rust candidate each accepted the two signed administrator form submissions below. The fixture verified fresh CSRF cookies and their matching form or script masks, then compared the normalized HTML bytes after accounting for each arm's SQL-verified generated User-Agent key and UTC timestamps.

| Route | Method and status | Body bytes per arm | Verified response masks |
| --- | --- | ---: | --- |
| `/api/core/useragents/` | POST 201 | 15,292 | Hidden form and script |
| `/api/core/streamprofiles/6/` | PUT 200 | 17,896 | Script |

Both arms matched `Content-Type: text/html; charset=utf-8`, `Vary: Accept, origin, Cookie`, and the route-specific Allow values. PostgreSQL confirmed the exact new User-Agent and updated profile, including selection of the generated key. Each arm restored the same owned baseline. Four signed canonical and alias JSON reads returned HTTP 200 and the saved values without issuing a CSRF cookie. Four signed Standard write attempts across canonical and alias routes returned HTTP 403 and preserved the SQL and broker state.

Native also returned the expected HTTP 501 capacity response for each of two signed multipart PUTs. One used a 240,469-byte request whose escaped response exceeded the HTML limit. The other was a 482-byte request with 1,001 selectable User-Agent rows. Independent SQL and broker snapshots matched before and after each rejection. The fixture then removed the overflow rows and verified the original baseline.

The ordinary Linux AMD64 build verified all 2,170 source inputs and 497 unchanged protected inputs. The 17 focused Core tests and the Main check passed. Both live request processes, the comparison, and exact cleanup exited 0. Cleanup verified all four owned container and process generations had retired and inventories showed zero containers, volumes, and networks. Formal M0 acceptance and performance remain open. Original Dispatcharr, Django, Django REST Framework, React, and dependency authors and contributors retain full credit. [Evidence](../scripts/rust-integration/m0-d2c5e611-core-multipart-atomic.json) · [Attribution](../ATTRIBUTION.md)
