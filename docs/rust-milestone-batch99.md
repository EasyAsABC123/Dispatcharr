# Programme detail mutations and public admin redirect

The original Dispatcharr authors and contributors retain full application and
workflow credit. Django, Django REST Framework, and nginx authors retain full
credit for their respective implementations.

Candidate99 adds administrator PATCH, PUT, and DELETE on the canonical EPG
Programme detail route. The mutation path keeps the stored guide relationship
and provider metadata, and it preserves omitted fields. The new admin router
uses the measured Original 301 response and accepts an optional trusted
`DISPATCHARR_PUBLIC_ORIGIN`. With that setting it uses the configured origin
for absolute redirects. The default sends relative `/login`. The literal
`admin` XC alias still uses the streaming path. A slashed two-tail request
keeps the reserved Native response, while the Original downstream admin UI
remains an open scope.

The exact lint-corrected revision is
`19284a9cc1c04234c0aa3b339e606e43933a280a`. Formatting, 548 Store tests,
40 Main tests and strict all-target lint passed. Store and Main reported 79
and one external-fixture tests ignored, respectively. The first strict lint
run identified two imports left behind by the routing change. The exact
retry removed those imports and reran all four commands. The paired package
checks and any new performance comparison are pending.

A separate exact Original management capture completed 16 canonical
operations in Compose. It verified the roles, route resolution and scoped
SQL and files. Original's EPG Programme collection POST returned HTTP 500
with the owned database state preserved. That observation remains separate
from the detail mutations. Both owned Source fixture resources retired.
[Original management receipt](../scripts/rust-integration/canonical-management-original/receipt.json)
