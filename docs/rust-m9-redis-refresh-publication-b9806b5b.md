# M9 Redis refresh publication recovery

Rust `b9806b5bcbe65c87a7df4083029fa7599e698628` reuses the existing read-only Redis connection check before publishing a refresh. The transport may repeat PING after a settled connection drop. It submits the original queue mutation once after a successful connection check and preserves ambiguous mutation outcomes.

A real Redis test closed only the Rust refresh publisher’s owned broker socket. The prior implementation returned `AmbiguousPublication` and left the queue empty. The updated implementation queued one frame with the exact refresh request fields. Three existing transport tests verified recovery before publication, a single mutation when its reply was lost, and retention of admission and ownership while the read-only check was pending. Exact fixture retirement passed.

A Lappy ARM64 diagnostic on the frozen `3d8f9672` source observed the signed five-minute import return HTTP 500 from `publish_ambiguous_backend`. The corresponding diagnostic with the connection check returned the expected HTTP 202 JSON and the application recorded `publish_accepted`. Both runs captured all 73 timed observations and retired all six owned resources. The second run reached the 28-second terminal readback deadline before verifying the refresh and its two children. The long-running worker and soak gates remain open.

The ordinary AMD64 package for `b9806b5b` then passed a signed XMLTV import in Docker Compose. The fixture observed the expected 202, one durable successful refresh root, both actual Index and Series children, an empty broker, ordinary application shutdown, and exact teardown. Execution, cleanup and evaluation each exited zero. The package manifest SHA-256 is `a15bce71a7889b41c3e3dc6dfb0aa0f990a7c93b1191bed46dd6be549a84e0dd` and the image is `sha256:62e265becbada0a2403eb038be9340d960536d85214f6f17ee4a691a1160bc3b`. The scoped proof SHA-256 is `d11dad846eef97a9a594895d410aa7e391c94609c66d737eab5ea8429a38011e`.

Formatting and strict workspace Clippy passed. The workspace suite covered 194 targets with 1,984 tests passing, 840 ignored, and zero failures. The prior `976c7800` application retains its [15 verified physical rows and three authorities](rust-current-postgresql-release-acceptance-976c7800.md). The timed terminal workflow and successor release acceptance require their own physical evidence.

Original Dispatcharr and all upstream contributors retain full credit for their work. React, license, and attribution files match the preceding application.
