# Numeric AppData method comparison on 63ee09fc

The Original and ordinary AMD64 Rust backends each served 18 signed
requests across a collection route, a key route, and three roles.
GET matched on all six cases. Each HEAD status and client-exposed
body matched, with differences in the Allow and Vary fields. Original
returned HTTP 200 with the framework metadata in all six OPTIONS
cases. Rust returned HTTP 405.

SQL fingerprints stayed unchanged across the full scope. Both
projects passed exact teardown. This comparison establishes the
observed difference for the next repair.

Original Dispatcharr and its upstream contributors retain full credit.
[Machine-readable request comparison](../scripts/rust-integration/m0-63ee09fc-numeric-appdata-method-differential.json)
