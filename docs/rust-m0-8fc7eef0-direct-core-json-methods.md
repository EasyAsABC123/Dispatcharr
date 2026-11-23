# M0 direct Core JSON GET, HEAD and OPTIONS

Original Dispatcharr, React and dependency authors and contributors retain full credit.

The exact `8fc7eef0a702b57d3c6b20dac171478b38e0196e` AMD64 package passed a fresh Docker Compose comparison of 16 requests per backend on the direct User-Agent and Stream Profile list and detail routes. The signed Standard and Administrator callers were verified independently, and the owned Stream Profile referenced the owned User-Agent.

| Direct route | Standard GET / HEAD status | GET bytes and HEAD Content-Length | Administrator OPTIONS status / bytes | Standard OPTIONS status / bytes |
| --- | --- | ---: | --- | --- |
| `/api/core/useragents/` | 200 / 200 | 851 | 200 / 1,146 | 403 / 63 |
| `/api/core/useragents/{id}/` | 200 / 200 | 206 | 200 / 1,149 | 403 / 63 |
| `/api/core/streamprofiles/` | 200 / 200 | 1,064 | 200 / 1,283 | 403 / 63 |
| `/api/core/streamprofiles/{id}/` | 200 / 200 | 118 | 200 / 1,286 | 403 / 63 |

Each selected response had `Content-Type: application/json` and `Vary: Accept, origin`. List routes returned `Allow: GET, POST, HEAD, OPTIONS`. Detail routes returned `Allow: GET, PUT, PATCH, DELETE, HEAD, OPTIONS`. The GET bodies, Administrator OPTIONS metadata and signed Standard OPTIONS denial matched byte for byte. The Standard denial body was `{"detail":"You do not have permission to perform this action."}`.

Rust sent zero entity bytes for all four HEADs and retained the matching GET length. The direct Original uWSGI listener sent an entity on each HEAD, so the comparison uses status and headers for HEAD. The separate stock AIO ingress witness recorded nginx suppressing the HEAD entity while retaining the same headers as direct uWSGI. [Stock AIO ingress HEAD witness](rust-m0-stock-aio-core-head-ingress.md)

The verified ordinary Native package contains 2,162 application inputs and preserves all 497 protected React, license and attribution inputs. The pinned Original and exact Native images used an owned internal network with no published ports. Every case preserved the scoped SQL and broker observation. All five phase exit files contained literal 0, and cleanup verified the exact baseline, candidate, data-init and origin container and process generations absent with zero project containers, networks and volumes remaining. [Sanitized evidence and hashes](../scripts/rust-integration/m0-8fc7eef0-direct-core-json-methods.json)

This pair covers the specified JSON requests and actors. The previously observed browsable HTML negotiation differences, wider executable coverage, formal M0 acceptance and performance remain open. [Earlier methods and negotiation comparison](rust-m0-390ad8aa-direct-core-methods-negotiation.md)
