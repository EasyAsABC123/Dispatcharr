# Xtream refresh and copied SQLite bootstrap on 4558a85b

The same source revision produced two verified AMD64 images from one
normalized build archive. The ordinary workspace passed 1,567 tests
with 814 ignored. The feature-enabled Store and App passed 770 tests
with 130 ignored. Formatting and strict Clippy passed in both configurations.

## Xtream request-owned refresh

The ordinary image matched the four retained Original stale and missing
movie and series requests. Each returned HTTP 200 after one provider
authentication GET and one metadata GET. Both preserved the selected
relation and the queue and task-result census. The independently
compared response shapes preserved the same field order, lengths and
typed values after checking generated IDs and clock fields against each
database. The stale series kept its stored episode. The missing series
created one episode and one corresponding provider relation.
Both arms verified exact cleanup.

## Copied SQLite bootstrap

The feature image served 72 scoped observations before restart and 72
after it. All ten simultaneous Standard bootstrap API calls returned
HTTP 200 in each generation. The separate administrator Users check
matched the copied account and profile census. Original performed
fixture login before its observation, so timestamp-bearing Users
entities and HEAD lengths are recorded separately.

The strict comparison exited 1 because the configured setup-status
response omitted Original's `Vary: origin` in both generations. The
remaining measured stable fields matched and exact teardown passed.
The same-image browser startup and package-level comparison of the
header correction are still open.

## Provider rehash broker refusal

The owned Native fixture observed one Lua LPUSH denial, no successful
publication, and unchanged stream hashes. It returned HTTP 503 with
a 62-byte JSON body and retained the expiring lock. Original returned
HTTP 500 with a 145-byte HTML body and cleared its lock. Both had
already saved the override, and both returned HTTP 200 without
publishing on the same-value retry. The Native diagnostic exited 0,
and the response and lock parity gate remains open.

This checkpoint measures functional behavior. Comparative application
performance remains open. Original Dispatcharr and dependency authors
retain full credit.

[Redacted evidence](../scripts/rust-integration/m6-m7-m8-4558a85b-compose-checkpoint.json)
