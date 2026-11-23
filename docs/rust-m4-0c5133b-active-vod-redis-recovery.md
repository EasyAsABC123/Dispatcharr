# Authenticated VOD playback after Redis replacement on 0c5133b

The ordinary AMD64 Rust image matched a fresh Original arm after an isolated Redis restart without saved state. A single signed viewer with a one-stream limit requested a synthetic MP4. After replacement, the fixture reopened the same playback ID with fresh valid authentication while the first response remained open.

| Observed behavior | Original | Rust |
| --- | ---: | ---: |
| First response | 200 | 200 |
| Origin-matching prefix from the first response | 278,528 bytes | 278,528 bytes |
| Same-ID reopen | 200 | 200 |
| Origin-matching prefix from the reopened response | 8,192 bytes | 8,192 bytes |
| Next bytes from the first response | 8,192 at expected offset | 8,192 at expected offset |

The new and old response bytes matched the origin independently. The ordinary run, exact teardown and strict reducer each exited 0. The earlier [paired failure](rust-m4-9f29dbe-active-vod-redis-restart.md) ended the first Rust response at 65,536 bytes and returned HTTP 503 for the reopen.

Ordinary recovery binds this request to the viewer authenticated by the signed-header verifier, along with the same content, provider and owned playback generation. Reopen uses the existing atomic stop and capacity checks. The body and compensating cleanup refer to the generation selected during its serialized admission. Existing extended recovery stays behind its existing configuration. Two standalone Redis controls passed, covering an attachment that captured an older origin and compensation after a settled ENTER reply was lost. The [package and playback receipt](../scripts/rust-integration/m3-m4-0c5133b-qualified-comparison.json) records the verified source, image and cleanup.

The paired check covers one Redis replacement and one reopen during an active response. Full media decode, repeated replacement with multiple concurrent reopens, post-reopen event history, broader close ordering, and comparative performance remain separate acceptance requirements.

Original Dispatcharr, FFmpeg and dependency contributors retain full credit.
