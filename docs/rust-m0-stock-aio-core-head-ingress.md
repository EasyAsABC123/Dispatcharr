# Stock AIO HEAD through nginx and uWSGI

The original Dispatcharr, React, nginx, Django and dependency authors and contributors retain full credit. See [Attribution](../ATTRIBUTION.md).

The exact Stock Original AIO image served the authenticated User-Agent collection from both its nginx and direct uWSGI loopback listeners. All four GET and HEAD requests returned HTTP 200, JSON media type, `Allow: GET, POST, HEAD, OPTIONS`, `Vary: Accept, origin`, and `Content-Length: 644`.

| Listener | GET body bytes | HEAD body bytes |
| --- | ---: | ---: |
| Stock nginx | 644 | 0 |
| Direct uWSGI | 644 | 644 |

The direct HEAD body matched the same listener's GET fingerprint. The stock nginx HEAD kept the GET length header and sent an empty body. This single Source route establishes the listener distinction seen in the separate [390ad8aa direct comparison](rust-m0-390ad8aa-direct-core-methods-negotiation.md). That comparison found actionable Rust gaps in HEAD headers, OPTIONS metadata, and HTML negotiation. Its direct HEAD body observations are diagnostic.

An isolated fixture administrator used the normal setup and login requests. All owner and physical phases exited 0. The owned container and process generation retired and the project inventories were empty. The [machine-readable receipt](../scripts/rust-integration/m0-stock-aio-core-head-ingress.json) retains the exact image, wire fingerprints and cleanup. This Stock-only witness covers one route.
