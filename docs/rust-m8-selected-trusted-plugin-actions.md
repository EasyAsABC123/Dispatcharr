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
The successor below verified the scoped header correction.

The same compatibility-host process was observed before and after both
actions. Its tracked generation retired, Main exited 0, all three tracked
service generations retired, and the separate UID helper was removed.
The Compose inventory was empty and the controller exited 0.

This result covers the two trusted actions using the selected worker
configuration and its limited configured-plugin startup scope. Default
runtime attachment, enabling through the API, broader M8 acceptance and
performance remain open.

[Redacted scoped receipt](../scripts/rust-integration/milestone-batch113/selected-trusted-plugin-actions-8b0c.json)

## Exact trusted action Vary header

The successor package at `e852e760` returned `Vary: Accept, origin` for
both actions, matching the two retained Original HTTP responses. Both
Native calls returned HTTP 200 with matching decoded JSON, Content-Type
and Allow. Each Native Content-Length matched its own response bytes.
The runner also verified the unchanged registry, one action marker per
request and retirement of the same compatibility host and all owned
service generations. The selected worker configuration stayed the same.

All 13 focused Rust tests and strict Clippy passed after recompiling the
changed store. The package source map differed from 8b0c only in the
plugin action response header handling. All 497 protected React and
attribution inputs stayed identical. The historical scoped header
difference remains recorded above. Default compatibility-host ownership,
enablement through the API, broader M8 acceptance and performance remain open.

[Exact header receipt](../scripts/rust-integration/milestone-batch113/selected-trusted-plugin-run-vary-e852.json)

## Trusted enabled plugin on 976f

A fresh `976f2867` run used the same installed, already-enabled trusted
fixture on Selected Main. Selected Beat and both standalone backup owners
were disabled. Both authenticated `echo` and `scalar` requests returned
HTTP 200 and matched the decoded JSON, Content-Type, Allow and literal
`Vary: Accept, origin` of the retained pinned Original responses. The
response lengths matched the returned bytes.

The echo used the same effective saved settings and plugin defaults.
One import and one action marker per request were observed. The plugin
registry stayed unchanged. The same Main and the same compatibility
host served both actions. Main exited 0 and both tracked process
generations retired. Three owned Compose service generations and the
separate UID helper retired, and the inventories were empty.

This scope covers an already-enabled reviewed local module under the
Selected runtime. Default runtime attachment, enabling through the API,
enabled-plugin restore, full M8 and comparative performance remain open.

[Redacted 976f Selected plugin receipt](../scripts/rust-integration/milestone-batch113/trusted-selected-plugin-actions-976f.json)
