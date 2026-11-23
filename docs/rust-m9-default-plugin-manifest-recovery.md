# Ordinary packages and ARM64 default plugin startup recovery

Application 0596767d65b889424d529670cc7301b02564071b built and verified in ordinary Docker packages on Linux AMD64 and ARM64. An independent ARM64 Compose check then observed the enabled default official repository's startup task settle, followed by two signed XMLTV refreshes that each completed their job tree. The source map is 0e83fbb98ab3c549a1763decdb9ae7c0de97fe8faa376e944a184e2656b35743 and covers 2,187 files, including 1,690 Rust-tree files and 497 unchanged React, license, and attribution files. The exact host workspace recorded 1,747 passed tests, zero failures, and 821 ignored tests across 181 test groups. Formatting, strict Clippy, and three focused manifest transport tests passed. [Machine-readable evidence](../scripts/rust-integration/m9-default-plugin-manifest-recovery-0596767d.json)

| Architecture | Image digest | Scoped physical result |
| --- | --- | --- |
| Linux AMD64 | `sha256:e34bcd9b550bb4feb6a20636e4bf4436ed7ddee3ea7f56515f67d80e08fd2cbe` | Ordinary package build and verification exited 0. The independently hashed source archive matched the exact source map. |
| Linux ARM64 | `sha256:5d2856ca77a5b4e4d7b1725407e58e8f35659d14c984181f5244be9462327acd` | Ordinary package, separate signed-auth fixture, and evidence audit exited 0. Readiness and the React HTML returned HTTP 200. First admin setup, JWT login, and signed channel-group read passed. The auth scope retired five container IDs, one network checked by ID and name, and one volume. |

## Default startup and signed guide work

The earlier ARM64 default-AIO check accepted a signed guide import with HTTP 202 and then recorded readiness HTTP 503. The default plugin ETA failed after acknowledgement, and the guide task remained queued. A private reproduction identified the Rust manifest response-framing guard. The ordinary patch uses the existing HTTP/1 chunk parser and gzip/deflate decoder. It retains separate limits of 65,536 encoded and 65,536 decoded entity bytes, the original inactivity and lifetime limits, and the existing error-status check before content decoding.

The new exact ARM64 image ran the ordinary AIO entrypoint with startup discovery enabled and instrumentation disabled. The observed startup ETA matched its stored `SUCCESS` result with a null value. The enabled official repository's fetch timestamp advanced and the queues drained. The runtime receipt establishes that this startup reached a terminal and released the worker to subsequent work. The provider HTTP status and content coding, and the repository's last-fetch status, were outside the retained projection.

Both signed guide imports returned the expected HTTP 202 response and each produced a distinct successful root and two successful children. The source programme and index matched the disposable fixture, queue and lock counts drained, and recordings stayed unchanged. One request used a freshly exchanged access token and the other used the ordinary signed admin token. The application and observer exited normally. The fixture independently verified exact absence of four container IDs and the network by both ID and name. Both project inventories were empty, with zero named volumes.

The ARM64 guide check used zero viewers. Paired performance, long soak, full M9 and release approval remain open. Original Dispatcharr and dependency authors retain full credit.
