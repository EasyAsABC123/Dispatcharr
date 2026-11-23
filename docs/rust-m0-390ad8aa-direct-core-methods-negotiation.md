# Direct Core methods and content negotiation on 390ad8aa

The original Dispatcharr and React authors and contributors retain full credit. See [Attribution](../ATTRIBUTION.md).

Pinned Original and ordinary Rust AMD64 `390ad8aa` each returned 12 responses on the direct User-Agent and Stream Profile list and detail routes. A signed Standard user sent the four HEAD requests. A signed Administrator sent the OPTIONS and GET requests. Each request preserved the scoped account, User-Agent and Stream Profile SQL and broker state.

| Request | Pairs | Verified result |
| --- | ---: | --- |
| HEAD with JSON Accept | 4 | Both returned 200, JSON media type and matching declared lengths. Original sent `Allow` and `Vary: Accept, origin`. Rust omitted `Allow` and sent `Vary: origin`. |
| OPTIONS with JSON Accept | 4 | Original returned 200 with DRF renderer, parser and serializer metadata. Rust returned 405 with an empty body. |
| Explicit HTML Accept | 2 | Original returned 200 browsable HTML with `Vary: Accept, origin, Cookie`. Rust returned 200 JSON with `Vary: Accept, origin`. |
| JSON-preferred Accept with HTML alternative | 2 | Both returned 200 with identical JSON body fingerprints and selected headers. |

The raw direct HEAD observer read 851, 206, 1,064, and 118 entity bytes after the header terminator on Original uWSGI. Rust's direct listener sent zero bytes for all four. These observations describe those listeners. The comparison excludes HEAD entity framing from its parity requirement, and the packaged Stock nginx ingress has a separate test.

The comparison and campaign exited 1 for the ten selected differences. Independent proof and cleanup exited 0. All four owned container and process generation checks passed and the project inventories were empty. The first attempt stopped before Native because its fixture expected every Original HEAD body to be empty and omitted the HTML Cookie variation. Its literal exits and verified Source teardown remain separate.

The [machine-readable receipt](../scripts/rust-integration/m0-390ad8aa-direct-core-methods-negotiation.json) records both arms, selected wire fields, and exact custody. Complete method and representation parity and full M0 acceptance remain open.
