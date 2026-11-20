# Ordinary anonymous M3U exports on c74

Original Dispatcharr authors and contributors retain full application credit.

Pinned Original and the exact c74 Rust image each returned HTTP 200 for all
15 positive M3U requests in internal Docker Compose. The comparison covered
bare and trailing paths, named profiles with Unicode and escaped characters,
TVG, logo, and EPG options, and output option precedence.

All 15 live playlist pairs matched byte for byte and each digest also matched
its retained Original capture. The runner compared Content-Type,
Content-Length, Content-Disposition and X-Content-Type-Options. Each phase
recorded 15 download events and zero blocked events. The metadata requests
did not follow the advertised playback URLs.

The comparison exited 0. All four owned resource generations retired, and
the Compose inventory had zero containers, networks or volumes remaining.
Large-catalog delivery, XC actor paths, broader M7 coverage and performance
remain separate acceptance gates.

[Exact scoped receipt](../scripts/rust-integration/milestone-batch113/m3u-positive-output-c74.json)
