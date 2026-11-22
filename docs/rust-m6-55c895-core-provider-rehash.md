# Scoped Core provider rehash

Pinned Original Dispatcharr and the ordinary Rust image from
`55c895bfe5637541788fe0445cd968c5f8f52ffc` both served HTTP 200 for the same
authenticated hash-key-only PATCH on an owned inactive STD provider. The
Rust image used the explicit Core scheduling and provider rehash options.
Its shared worker processed the publication and linked it to the typed
result `Successfully rehashed 2 streams`.

Both arms merged two streams into one, retained the existing winning
channel link and retargeted the other link while preserving its ID and order.
The unrelated provider, stream and channel stayed unchanged. Both emitted
starting, processing and completed events, released the scoped account lock
and finished with no pending or reserved jobs. The controller and reducer
exited 0. Exact inventory and process-generation checks verified retirement
of the two Original and three Native roles.

The first controller exited 1 when its Original child rejected the hidden
launcher mode with exit 2 before Compose. Independent checks found no owned
roles, empty fixture inventories and retired process groups. The fresh
same-image retry fixed the child parser and reran both backends.

This run covers the successful explicit Core rehash path. The Native 501
refusal and 503 unsettled-publication responses remain source-reviewed and
unmeasured. Default rollout, the broader M6 graph and performance remain
outside this receipt. Original Dispatcharr, Celery and Django authors
retain full credit.

[Redacted receipt](../scripts/rust-integration/m6-55c895-core-provider-rehash.json)
