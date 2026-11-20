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

## Integrated a785 package

A fresh comparison repeated the same 15 anonymous M3U requests per backend
against pinned Original and packaged Rust a785a2b7. All 30 requests returned
HTTP 200. Every live pair matched the complete playlist length and digest,
the retained Original digest, the entry count and the four selected HTTP
headers. Each backend again recorded 15 download events and zero blocked
events. The candidate ran in ordinary mode with the manual-backup owner
absent.

The comparison exited 0. All four container and host generations retired,
and the project inventory was empty. [Redacted a785 receipt](../scripts/rust-integration/m7-ordinary-m3u-positive-output-a785.json)
