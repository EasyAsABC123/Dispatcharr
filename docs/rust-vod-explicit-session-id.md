# Explicit VOD session identity

An explicit fresh VOD session ID now gets its own session when admitted. An exact existing ID still reuses its own session. The original Dispatcharr authors retain full credit for the application and its session behavior.

The combined Rust image `sha256:90bc93409ae1d7eb04985c6f208fc8e4c3f5d214be91e990f47611ad51eafd3f` at revision `7adb0c72dae360cc5c3a4b5e4d8f7b100e8f200b` passed the four event-relative Compose cohorts with controller exit 0. The comparison used pinned Original revision `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`.

| Playback request | Original | Rust |
| --- | --- | --- |
| Same explicit ID, two repeats | HTTP 200, same session, request count 3 | HTTP 200, same session, request count 3 |
| Fresh explicit ID, two repeats | HTTP 503, `No available stream`, no fresh session | HTTP 200, fresh session and its own request count of 1 |
| Stable nested `/proxy/vod/stats/` after fresh-ID dispatch | One unrelated connection, no fresh row | One unrelated connection and one fresh row |
| Provider slots at +100, +250 and +400 ms after last close | 2, 2, 2 | 1, 1, 1 |

Both same-ID responses and the unrelated playback prefixes matched across backends. All eight guests exited 0. Fourteen complete prefix decodes passed with audio and 75 video frames each. Every cohort retired its four recorded Compose container generations and left no container, network or volume in its project. Both named Redis/HTTP regression tests passed in the combined package and the exact Redis container and host process generation retired.

The earlier event-relative run observed Native incorrectly adopting the prior idle ID. The first C124 isolated test run exited 1. Its helper kept stdout but lost the panic stderr. The corrected test assertions preserve the outward compatibility range header, distinct response bytes and separate request counters. Both corrected component tests passed on the next attempt, whose controller still exited 1 because the cleanup helper rejected Docker's missing-container wording. The latest helper verifies removal with the exact-ID inventory and independently observes the absent host process generation. A separate diagnostic attempt stopped at socket access before launching a container. The failed receipts remain intact.

Provider-capacity release timing remains open. The Original 503 versus Rust 200 is still reproducible. This result establishes the explicit session identity and its visible stats behavior in the named fixture. It provides no performance, full VOD or full M4 acceptance. [Redacted physical evidence](rust-vod-explicit-session-id.json)
