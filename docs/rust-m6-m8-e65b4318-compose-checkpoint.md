# Definite broker refusal and copied SQLite on e65b4318

Both AMD64 packages used the same verified application archive for
`e65b43181f235f7fc50c6563aae00bf04bd4002d`. The ordinary image kept copied
SQLite support disabled. The second image explicitly enabled it. Formatting,
strict Clippy, and both test configurations completed successfully.

The two Rust release compilation steps took 352.8 and 363.3 seconds on the
build host. BuildKit reused the ordinary compiler layer in the opted-in build,
along with the frontend and packaged helper layers.

## M0 HDHomeRun route contracts

The ordinary image matched Original for authenticated administrator HEAD and
OPTIONS on both `/api/hdhr/` and `/hdhr/`. All four independent observations
matched status, headers and body hashes. Both fixtures verified exact teardown.

## M6 provider rehash

A real broker ACL denial produced the same HTTP 500 response as Original,
including the 145-byte HTML body. Both backends retained the saved provider
override, released the owned rehash lease, and returned HTTP 200 for the
same-value retry. The fixture observed zero successful scoped publications
and no worker result or stream merge. All three owned Native roles retired.

An acknowledged publication with a lost broker reply remains a separate case.

## M8 copied SQLite

The paired HTTP cohort checked 88 requests in each of two generations and
all ten concurrent Standard startup reads per generation. Stable wire fields
matched Original, including `Vary: origin` on the configured setup status
response. Notification filters and actor-specific dismissal reads matched.
The receipt records the administrator Users login-clock fields separately,
with SQL and typed user census verification.

A real Standard user signed in through the unchanged React UI, reached the
channel table, and rendered its owned channel. All ten startup GETs and the
deferred Logos GET succeeded. The raw Standard and anonymous Logos GET and
HEAD responses matched Original's status and headers. The comparison checked
HEAD and anonymous body hashes and independently verified the Standard body,
including the fixture-specific cache URL. The WebSocket connected
with the real login token, delivered the permitted guide update, filtered the
administrator telemetry, and returned close code 1001 during normal Main
shutdown. Browser processes, TCP listener, IPC socket, and owned containers
retired.

The first browser attempt stopped because the synthetic tools bind directory
was `0700` and the nonroot probe could not read it. The second used `0755` on
the bind with a private parent, then reached Chromium after real token, Logos,
and WebSocket checks. Its host controller left the Node command pipe open and
timed out awaiting process exit. A separate real Chromium control verified the
command-pipe fix. The successful replay used the same application image and
assertions. Both prior attempts verified exact container and IPC cleanup.

[Machine-readable evidence](../scripts/rust-integration/m6-m8-e65b4318-compose-checkpoint.json)

The original Dispatcharr and dependency authors retain full credit for the
application, models, algorithms, and protocols. Comparative application
performance and broader milestone acceptance remain open.
