# Signed channel inventory differential on 8a547cf0

The same isolated Channel/Stream/Logo inventory completed in Original and
Native. Original matched all 32 signed GET observations across 16 existing
declared route IDs. This includes 13 JSON aliases and one request per route
from each signed Standard and administrator role. Original was pinned to
`1241b3f7` and ran through its private direct uWSGI listener. Each response
passed its owned-entity and provider-URL checks. The scoped SQL and broker
state remained unchanged.

The ordinary Linux AMD64 Native `8a547cf00cd33a94fe4ccb982272be1861d52e73`
package reused the qualified Original receipt with a fresh private seed.
It completed all 32 observations. The Native run, proof and campaign exited 1.
Preflight and exact cleanup exited 0.

| Physical responses | Result |
| --- | --- |
| Canonical Stream list, Stream detail, and Logo list | Six Native HTTP 200 responses. Typed JSON and the owned SQL projection matched for both roles. These six differed only in Allow and Vary. |
| Thirteen declared JSON alias routes | All 26 Native responses returned HTTP 404 for the same aliases across both roles. Original returned HTTP 200 in every corresponding case. |
| Compared headers | All 32 Native replies omitted Allow and reported `Vary: origin`. Original included the route-specific Allow and `Vary: Accept, origin`. |
| Overall | Zero exact compared matches. There was no typed payload difference among responses with matching HTTP status. |

Both arms verified physical retirement. Original retired its baseline and
origin generations. Native also retired its data initializer and application
generations. Both owned Compose inventories were empty. No scoped SQL or
broker mutations occurred during the reads. Raw bodies, credentials and
provider URLs were excluded from the retained projection.

The [scoped machine evidence](../scripts/rust-integration/m0-m5-8a547cf0-channel-inventory-differential.json)
records the actual receipts, phase exits, typed hashes, headers and route IDs.
The earlier 102-case catalogue pass used the same package and covers different
routes. These results add scoped Original authority and a current Native
differential. Formal M0 and M5 and performance gates remain open.

Original Dispatcharr, Django, Django REST Framework and dependency authors
retain full credit.
