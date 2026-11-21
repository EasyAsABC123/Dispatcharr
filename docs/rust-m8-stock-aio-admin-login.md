# Stock and Rust AIO public admin login redirect

The untouched Stock AIO nginx front door returned 301 for
`GET /admin/login/`, with an absolute same-origin `/login` Location and
no cookie. A separate GET to that landing returned 200 with the exact
858-byte packaged React shell and no cookie. The fixture sent no
credentials or cookies. Its controller and independent reducer exited
zero, and the exact Stock role and its generation retired.

The separately qualified Rust default AIO Main control returned 301 for
the same ordinary path, with relative `/login` and no session cookie.
Both redirects resolve to the same-origin login route. The literal
Location formats differ. The Rust control did not capture the redirect
body or follow the landing. Stock's full HTML redirect measured 178 bytes.
[Qualified Rust AIO control](rust-m8-2581-private-admin-settings-restart.md)

The first Stock attempt recorded guest and physical phases exit 0 and
exact cleanup, then overall qualification exit 1. Its expected
169-byte HTML came from the separate pinned nginx fixture. Adding the
Ubuntu suffix to that fixture's footer reproduced the exact complete
178-byte stock body and hash. The same-image retry verified that stock
representation and the unchanged landing.

This scope covers the ordinary public redirect and Stock's packaged React
shell. Direct Django admin authentication, full M8 and performance have
separate evidence.
[Redacted ingress receipt](../scripts/rust-integration/m8-stock-aio-admin-login.json)

The original Dispatcharr, nginx, Django and dependency authors and
contributors retain full credit. [Attribution](../ATTRIBUTION.md)
