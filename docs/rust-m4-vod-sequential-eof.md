# M4 VOD completion and sequential admission

Original Dispatcharr contributors retain full application and behavior credit.

Pinned Original and packaged Rust revision `34b5198793164aee8077665d73bff9f697904014` each played the same 4,299,455-byte MP4 through a disposable provider with one eligible slot. An authenticated administrator finished the first HTTP 200 response. A read-only census then observed zero provider slots, no active readers, and removal of the first idle session metadata. A different authenticated viewer began a fresh explicit session only after those observations. The second request also returned HTTP 200 and completed. Both responses on each backend matched the entire fixture hash and decoded audio and video. The origin accounted for exactly two completed playback responses per backend.

The Rust successor retains provider ownership through normal response-body completion. The focused package-source VOD library run passed 37 tests with 21 existing ignored tests. The paired Compose controller exited 0 and all four owned role generations retired, with zero project containers, volumes and networks remaining.

The first two paired controllers each exited 1 with verified cleanup. The second attempt observed the first 8 KiB of the second Original response and one provider slot, with no active reader or expected profile on the requested second metadata key. Rust qualified that same earlier observer. Pinned Original scores a same-content, same-timeshift idle session as eligible for reuse under a new request ID. Its normal one-second idle cleanup and the unchanged Rust timer remove idle session metadata. The final fixture waits for that observed removal and uses the existing eight-second watchdog from the accepted Range plans. The earlier requested-ID ownership difference remains open.

The comparison covers a finite sequential workflow after observed retirement. The timing between final HTTP body framing, TCP delivery, and release was outside the observation. The earlier provider-capacity timing question, full M4 parity and performance verification remain open.

[Redacted receipt](../scripts/rust-integration/m4-vod-sequential-eof-34b519879316.json)
