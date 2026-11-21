# Group Manager OPTIONS parity on 2581d75b

Five authenticated Admin requests to the ordinary Group Manager OPTIONS routes
returned identical compact JSON and selected response headers on Original and Rust.
The cases cover the collection, a provisioned group, the claim-custom action, an
unallocated group, and the claim action. Eight scoped SQL tables remained identical
before and after the requests on both backends. The exact Compose roles and owned
resources were independently verified as retired.

The scoped receipt records the candidate image, the pinned stock authority, and the
five response digests. This test covers JSON metadata for these requests.

[Redacted paired receipt](../scripts/rust-integration/m5-2581d75b-group-options.json)

The original Dispatcharr authors and contributors retain full credit.
