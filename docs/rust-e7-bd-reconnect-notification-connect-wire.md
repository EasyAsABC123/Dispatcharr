# Fresh reconnect, notification subscriptions, and Connect JSON wire evidence

## Fresh fMP4 reconnect after abrupt owner loss

After each 75-second abrupt-loss observation, a fresh viewer requested the same profile once from the surviving route. Both stock Original and Rust `e7dda7f` returned HTTP 200 with `video/mp4` and opened one additional upstream connection. Original delivered a prefix with 74 decoded video frames and AAC audio. Rust sent 295,596 bytes and then timed out before the fixture obtained a qualified decode. The paired observer passed and both exact cleanups passed. The playback discrepancy remains open. [Machine-readable evidence](../scripts/rust-integration/m3-e7dda7f-abrupt-fresh-reconnect.json)

## Ordinary WebSocket notifications

Stock Original and Rust `e7dda7f` accepted real administrator and Standard-user subscriptions. The same stock publisher sent three persisted notifications in public, administrator-only, and public order. Both backends delivered the same ordered events to each eligible role. They closed both sockets normally and retired the scoped subscriptions and projects. This verifies receiving and role filtering with the shared stock publisher. [Machine-readable evidence](../scripts/rust-integration/m7-e7dda7f-notification-subscription.json)

## Successful Connect JSON responses

Both `.json` spellings on ordinary Rust `bd07920` returned the same fixed timestamp, selected values, field order, and normalized compact JSON as stock Original. Original sent 327 bytes and Rust sent 373 bytes. The verifier reproduced the full 46-byte difference from PostgreSQL JSON spacing. The literal Content-Length matched the actual body in each response. Original returned `Allow: GET, HEAD, OPTIONS`, and Rust omitted the header. Both exact cleanups passed. [Machine-readable evidence](../scripts/rust-integration/m5-bd07920-connect-json-success-wire.json)
