# Direct M3U provider selection on f960875

The original Dispatcharr authors and contributors retain full credit for the application. See [Attribution](../ATTRIBUTION.md).

Original and ordinary Native served the same four authenticated direct M3U phases from a copied PostgreSQL fixture with two synthetic STD accounts and two stream links. Only the second account was allowed in the first phase. Both were allowed in the second and third phases, with the stream order reversed in the third. The fourth phase left the first stream URL empty and selected the second.

Both arms returned HTTP 200 and identical selected accounts and playlist body hashes across all four phases. Each playlist contained one channel, matched content type, disposition and length, and omitted XC catch-up attributes for the selected STD stream. Each phase waited three seconds for the two-second cache TTL. The scoped SQL rows were preserved across reads and had the same initial and reset hashes.

The Compose controller, independent reducer and teardown each returned 0. All four owned container and process generations retired, and the project contained zero containers, networks or volumes at the final census. The [machine-readable receipt](../scripts/rust-integration/m7-f960875-direct-provider-m3u.json) covers these four ordinary restricted-selection phases. The unrestricted fallback, XC credential reconstruction, denied requests, full M7, and performance remain open.
