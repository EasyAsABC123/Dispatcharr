# HDHomeRun browser authentication on e0016d42

The fixed status GET returned HTTP 401 on both backends for eight forced
browsable requests using synthetic inactive credentials or an expired
bearer token. Original and Rust returned the same `Allow` and bearer
challenge. Original selected HTML and included `Cookie` in `Vary`.
Rust returned JSON with a different content length and `Vary` header.
Both physical observations and independent cleanup passed. The response
format comparison remains open.

The fixture kept the existing throttle and access policy in place across
both policy states. It retained response lengths, body hashes, and selected
headers without copying the credential values or raw HTML. That evidence
leaves the exact safe browsable shell, embedded error details, and dynamic
CSRF and cookie attributes to a targeted Original structural observation.

[Machine-readable comparison](../scripts/rust-integration/m0-e0016d42-hdhr-browser-auth.json)

Original Dispatcharr, Django REST Framework, and dependency authors
retain full credit for the application, models, algorithms and protocols.
