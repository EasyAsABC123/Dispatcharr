# Authenticated Channel detail GET

The exact `a33cf4cc` package matched Original on successful Channel
detail GETs for one managed and one manual channel. Both backends used
the same scoped PostgreSQL state. All four requests returned HTTP 200
with matching typed channel values and the literal
`Allow: GET, PUT, PATCH, DELETE, HEAD, OPTIONS` header. SQL stayed intact
and all four owned fixture roles retired.

The prior 11dd visibility comparisons recorded ten successful GETs per
backend and the missing Rust header. The new receipt qualifies this
successful GET scope. Wider M0 and M5 acceptance and performance remain
open. The original Dispatcharr authors and contributors retain full credit.
[Redacted receipt](../scripts/rust-integration/m0-channel-detail-get-allow-a33cf4cc.json)
