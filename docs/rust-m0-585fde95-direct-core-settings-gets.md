# Direct Core settings reads on 585fde95

The original Dispatcharr and React authors and contributors retain full credit. See [Attribution](../ATTRIBUTION.md).

Pinned Original and ordinary Rust AMD64 `585fde95` matched HTTP status, JSON media type, complete typed content, and the observed list order on eight positive request pairs. The pair covered the unsuffixed User-Agent and Stream Profile list and detail GET routes. Both a signed Standard user and a signed Administrator made each request after an authenticated identity check. Settings on the existing React frontend use these collection routes.

| Direct GET route | Matched typed pairs | Observed content |
| --- | ---: | --- |
| `/api/core/useragents/` | 2 | Four records including the synthetic User-Agent |
| `/api/core/useragents/{id}/` | 2 | Seven fields including the exact fixture timestamps |
| `/api/core/streamprofiles/` | 2 | Six records including the synthetic inactive profile |
| `/api/core/streamprofiles/{id}/` | 2 | Seven fields including the linked User-Agent ID and empty command and parameters |

All eight wire pairs differed. The response body lengths matched, and the SHA-256 fingerprints differed. Original returned `Allow` and `Vary: Accept, origin`. Rust omitted `Allow` and returned `Vary: origin`. The recordings retain the original wire lengths and fingerprints, so this comparison leaves byte and header parity open.

Every read preserved the scoped actor, User-Agent, and Stream Profile SQL and the broker fingerprint. Original and Rust observed the same disposable database after Original serving retired. Preflight, run, cleanup, proof, and campaign all exited 0. Compose teardown left zero owned containers, volumes, and networks, and all four recorded container and process generations were absent.

The [machine-readable receipt](../scripts/rust-integration/m0-585fde95-direct-core-settings-gets.json) includes exact image custody, route IDs, sanitized typed and wire fingerprints, and teardown results. This scope leaves other methods, full operation coverage, milestone acceptance, and performance unmeasured.
