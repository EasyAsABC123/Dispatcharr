# Original packaged React root and assets

The original Dispatcharr authors and contributors retain full application,
React and deployment credit. [Attribution](../ATTRIBUTION.md)

The local Linux amd64 all-in-one image of Original
`1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` served the anonymous `GET /`
through its ordinary stock entrypoint. The Compose project had no network
access or application and asset mounts. Its fresh data directory used tmpfs.
The build used readable archive directories. The harness copied the disposable
container's `/etc/environment`, added only `LANG=C.UTF-8`, and verified the
exact bytes round-trip before startup. This explicit OS login precondition
is part of the qualified fixture.

| Observation | Actual response |
| --- | --- |
| Anonymous root | HTTP 200, `text/html`, 858 bytes equal to the baked `index.html`, zero redirect |
| Linked JavaScript module | HTTP 200, `application/javascript`, 2,654,224 bytes equal to the baked asset |
| Linked stylesheet | HTTP 200, `text/css`, 227,833 bytes equal to the baked asset |
| Full deployed Django 6.0.9 resolver | Ordinal 589, unnamed URL, `django.views.generic.base.TemplateView`, empty keyword arguments, `index.html` |

The observed GET identifies the existing declaration
`route-a21d90b60c8d5e9c908b:METHOD_UNRESOLVED`. Django derived its runtime
view name from the callable. The original route's declaration name and
method metadata stay unchanged. The exact build, five Compose phases and
three locale copy and verification phases exited zero. The root ran in the
same Original process generation observed at readiness. Both command pipes
reached EOF and the exact Original container and generation retired. The
independent inventory found zero owned containers, networks and volumes.
[Redacted build, execution and teardown receipt](../scripts/rust-contracts/original-aio-root7.json)

The first image's probe exited 137. A later diagnostic reproduced readiness
exit 137 while the Original container exited 1. Its login shell initialized
PostgreSQL with locale C and a SQL_ASCII template, and the application request
to create a UTF-8 database failed. The next attempt reached HTTP readiness
using the explicit locale but exited 1 at an incorrect resolver assertion
before the root request. The succeeding attempt corrected that assertion and
recorded the root and assets first.

The network-disabled SDK now includes one partial Original assignment for
the anonymous root. It verified 797 of 798 HTTP declarations with partial
Original coverage. The remaining route, Native coverage, default-locale
boot, performance and full M0 remain open.
[SDK receipt](../scripts/rust-contracts/sdk-live-root7-797.json)
