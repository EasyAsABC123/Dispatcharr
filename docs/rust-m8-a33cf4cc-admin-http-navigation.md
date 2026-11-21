# Administrator HTTP loopback and stock navigation on a33cf4cc

## Private Rust HTTP loopback

Pinned Original and a private Rust router each completed an ordinary
administrator login form GET, login POST, read-only settings GET and
logout POST. Both returned 200, 302, 200 and 200. Each workflow used the
issued cookies and same-origin masked CSRF forms through a real HTTP
cookie jar. Django decoded the SQLx-issued session from PostgreSQL.
The private Rust settings page returned the expected stored value
and logout expired the browser cookie and deleted the same database
session. Only the owned account's last-login value remained changed.

The Rust router was part of a test executable in the pinned Original
guest and bound an unpublished loopback port. Its exact listener
process exited 0, the port retired, and the Compose resource inventory
was empty after both owned generations retired. The first physical
attempt exited 1 at the fixture's listener readiness reader and
completed independent cleanup. The corrected reader consumed the
ordinary test preamble and readiness record from the same executable.

The [PostgreSQL session interoperability evidence](rust-m8-a33cf4cc-admin-session-sqlx.md)
covers the separately qualified SQLx owner. Packaged Rust Main and
public ingress remain outside this HTTP loopback scope. [Redacted loopback receipt](../scripts/rust-integration/m8-a33cf4cc-admin-http-loopback.json)

## Original direct administrator navigation

A separate pinned Original fixture opened /admin, followed the canonical
slash and login redirects, submitted the actual same-origin login form,
and followed the visible admin index and Core Settings changelist links
to the owned settings form. It read the form and logged out. The eight
responses were 301, 302, 200, 302, 200, 200, 200 and 200. SQL verified
the one signed session, unchanged settings and admin logs, deletion
of the same session, and preservation of the unrelated session rows.
The copied Original owners retired and the project inventories were empty.

This workflow used the direct stock uWSGI HTTP endpoint. Stock AIO and
modular Compose publish nginx on 9191 by default, where /admin redirects
to /login. Stock dev and debug Compose publish the direct HTTP port
5656, which serves the complete Django application. The source-backed
route authority and the earlier [direct settings deep-link witness](rust-m8-a33-original-admin-session.md)
are distinct from Native navigation and packaged direct ingress, which
remain open. [Redacted stock navigation receipt](../scripts/rust-integration/m8-a33-original-admin-navigation.json)

Full M8 acceptance and performance remain open. The original Dispatcharr
and Django authors and contributors retain full application and
framework credit.
