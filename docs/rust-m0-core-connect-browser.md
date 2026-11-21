# Core and Connect browsable API directories

Pinned Original and the exact Rust package at f22f2260dae5ae67293c785a54e558ea24103918 returned HTTP 200 over the authenticated bare Core and Connect API directories for both explicit HTML and the captured browser Accept profile. The four cases matched body length, Allow, Vary, content type, and normalized whole HTML. Each case independently generated fresh masked CSRF values. The paired ordered JSON controls matched, and the isolated database rows stayed unchanged. [Exact receipt](../scripts/rust-integration/m0-core-connect-browser.json)

The earlier exact 1423 diagnostic returned canonical JSON on Native for these four browser requests. That diagnostic established the Original response and the discrepancy. The fresh qualification independently ran both arms and verified cleanup and the Native Main exit.

This evidence covers these two bare authenticated directory GET routes. Other directory negotiation profiles, the full M0 matrix, and comparative performance remain open. Full credit belongs to the original Dispatcharr and Django REST Framework authors and contributors. [Attribution](../ATTRIBUTION.md)
