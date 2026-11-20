# Original DRF directory HTML for administrators

Pinned Original served HTML for four authenticated administrator GET
cases on `/api/channels/` and `/api/m3u/`. The literal HTML and Chrome
mixed Accept profiles matched the same per-route sanitized HTML digest.
Both directories showed the `Api Root` heading and embedded the same
ordered links as their same-actor JSON controls.

All cases returned HTTP 200 with `text/html; charset=utf-8`,
`Allow: GET, HEAD, OPTIONS` and `Vary: Accept, origin, Cookie`. The
responses set a CSRF cookie and used independent masks on repeat.
Channels returned 11,055 bytes and M3U returned 10,271 bytes.
The isolated Compose pair of services retired all owned resources.
[Redacted Original receipt](../scripts/rust-contracts/verified-original-drf-directory-html.json)

The fixture used bearer authentication and requested only the two
directory pages. The SQL rows stayed unchanged. A Native HTML comparison
and broader directory negotiation remain open.

Original Dispatcharr and Django REST Framework contributors retain full
application and behavior credit. [Dispatcharr upstream](https://github.com/Dispatcharr/Dispatcharr)
