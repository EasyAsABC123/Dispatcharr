# Trusted local plugin actions with selected Rust workers

Original Dispatcharr authors and contributors retain all application credit.

Packaged Rust `8b0c76ca` returned HTTP 200 for the trusted fixture plugin's
`echo` and `scalar` admin requests. Both decoded JSON results matched the
retained pinned Original HTTP responses. The original fixture template ran
from an already-enabled registry row with the same saved settings. Native
recorded one import and exactly one Python action marker per request.
The registry state stayed unchanged. Rust ran with
`RUST_EXPERIMENTAL_SELECTED_JOBS=1` and `RUST_EXPERIMENTAL_SELECTED_BEAT=0`.

Content-Type and Allow matched on both responses, and each Native
Content-Length matched its returned bytes. Original sent
`Vary: Accept, origin` for both actions, and Native sent `Vary: origin`.
Selected action-header parity remains open.

The same compatibility-host process was observed before and after both
actions. Its tracked generation retired, Main exited 0, all three tracked
service generations retired, and the separate UID helper was removed.
The Compose inventory was empty and the controller exited 0.

This result covers the two trusted actions using the selected worker
configuration and its limited configured-plugin startup scope. Default
runtime attachment, enabling through the API, broader M8 acceptance and
performance remain open.

[Redacted scoped receipt](../scripts/rust-integration/milestone-batch113/selected-trusted-plugin-actions-8b0c.json)
