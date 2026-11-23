# M0 current AppData method evidence

Ordinary Rust application `0c5133b` and the pinned Original completed
fresh paired Compose runs for collection GET and per-key GET, PUT, and
DELETE. The runs used signed synthetic owner, administrator, and
unrelated Standard accounts. Both backends matched the statuses and
response bodies for twelve primary requests, including the ordinary
403 denials. Two additional reads verified persisted writes. Reads and
denials left scoped state unchanged. The write fixture restored its
scoped rows while preserving unrelated state.

The two successful DELETE responses have empty bodies. Original sends
`Content-Length: 0`, and Rust omits the field. The fixture records that
existing framing difference. Both scopes passed preflight, physical
execution, independent teardown, and strict comparison.

These method checks add runtime evidence to two tracked discovery
patterns. The canonical register still carries their unresolved method
placeholders. Full M0 coverage remains open.

Original Dispatcharr, its React frontend, and its dependencies retain
their existing attribution and licenses.

[Machine-readable receipt](../scripts/rust-integration/m0-0c5133b-appdata-methods.json)
