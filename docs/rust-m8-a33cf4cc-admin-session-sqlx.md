# PostgreSQL administrator session interoperability on a33cf4cc

Pinned Original performed an ordinary same-origin administrator login,
read the permitted settings page, and logged out over HTTP. The responses
were 302, 200, and 200. A private Rust Store test module read that same
Django-signed session from the copied PostgreSQL database using SQLx and
resolved the same staff account and permitted setting.

For a second session, the test used the stock login form and performed the
credential and CSRF checks through the Rust Store owner. SQLx committed the
signed session and the fixture account's updated last-login value. Django
decoded the new session from PostgreSQL, and stock Django accepted it on the
same settings HTTP page after the test browser supplied the issued session
cookie. The Rust Store owner then consumed the real stock logout form's CSRF
token and deleted that session. The fixture verified session expiry, CSRF
rotation, and preservation of unrelated sessions, settings, and admin logs.

The isolated run exited 0. Both owned container and process generations
were gone, and the project had zero containers, networks, and volumes.
The [stock browser witness](rust-m8-a33-original-admin-session.md) documents
the separate Original HTTP authority. Native HTTP login, settings, logout,
and session cookie headers still require an owned listener and physical
comparison. Full M8 acceptance and performance remain open.

The original Dispatcharr and Django authors and contributors retain full
application and framework credit. [Redacted receipt](../scripts/rust-integration/m8-a33cf4cc-admin-session-sqlx.json)
