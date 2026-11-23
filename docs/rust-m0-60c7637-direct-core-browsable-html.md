# AMD64 direct Core browsable HTML on 60c7637

The pinned Original and the exact packaged Rust candidate returned HTTP 200 with matching normalized HTML for two administrator GETs with `Accept: text/html`. The paired Compose fixture seeded equivalent SQL and broker state, checked administrator and Standard roles, and issued real HTTP requests to both applications.

| Administrator route | Body bytes | Allow | Cookie-linked masks |
| --- | ---: | --- | --- |
| `/api/core/useragents/` | 16,470 | `GET, POST, HEAD, OPTIONS` | POST form and script |
| `/api/core/streamprofiles/6/` | 17,715 | `GET, PUT, PATCH, DELETE, HEAD, OPTIONS` | Script |

Both routes matched on `Content-Type: text/html; charset=utf-8`, `Vary: Accept, origin, Cookie`, and CSRF cookie attributes. The fixture reconstructed each raw response using the verified mask slots, then compared normalized response hashes. It retained the slot names and hashes with the credential and token values removed. In both applications, explicit JSON requests from a Standard account returned matching JSON with HTTP 200. Native also returned the same HTTP status, JSON bytes, and relevant headers for the Standard account's explicit HTML requests, with no CSRF cookie issued.

The Linux AMD64 package used candidate `60c7637c807671955fcb789cbc94a5763ae799a2`, parent `329730439bf3d9dd9ccdfb4c4fdce44ad9f76def`, and Original `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`. The package audit verified 2,167 source inputs, including 497 unchanged protected inputs. All 10 focused host tests and the Main check passed. Compose preflight, both request processes, the comparison, and teardown exited 0. Each request preserved the SQL and broker state. Cleanup verified that all four owned container and process generations were absent and that no owned containers, networks, or volumes remained.

This check covers two administrator GETs and the Native Standard JSON fallback. HTML form submissions, Original Standard HTML, HTML HEAD and OPTIONS, format aliases, and broader browser coverage remain open. M0 and its performance gate remain open. The original Dispatcharr, Django, Django REST Framework, React, and dependency authors and contributors retain full credit. [Evidence](../scripts/rust-integration/m0-60c7637-direct-core-browsable-html.json) · [Attribution](../ATTRIBUTION.md)
