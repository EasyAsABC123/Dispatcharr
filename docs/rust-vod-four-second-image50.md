# Image50: four-second Redis reply exposes an open parity gap

Original Dispatcharr and dependency authors retain full code, algorithm and license credit.

The once-only physical-reply observation is valid, and parity fails. With the actual provider-reservation reply held for four seconds, Original returns 200 in the success case and 429 in the busy case. Image50 returns 503 (`Redis unavailable`) at approximately three seconds in both cases. This does not invalidate the previously accepted selected 2.4-second cases; it establishes a separate unhandled boundary. Full M4 and the backend rewrite remain incomplete.

| Case | Original status / request-to-headers | Rust status / request-to-headers |
| --- | --- | --- |
| Success | 200 / 6.012497 seconds | 503 / 3.005392 seconds |
| Busy | 429 / 4.010429 seconds | 503 / 3.004334 seconds |

The measured physical holds were 4.000619–4.000791 seconds, within the unchanged 4.35-second capture limit. Original's additional success delay is consistent with the fixture pacing its first 65,536-byte write at 32,768 bytes per second; this is not a newly inferred six-second Redis timeout. Original synchronously waits for its actual Redis reservation. Rust's outer recovery observer expires at three seconds even though its retained physical operation remains owned.

All four phases retain valid timing, the early follower's 200 response and exact 8,192-byte prefix, and the administrative stop witness. Busy native observations retain the other stream's private owner/fence/grant and global count of one. They do not establish Original/native public-counter parity. Original request-start/finished signals were checked again after the native phase: `request_finished` witnesses response-close resource-closer attempts, not successful generator finalization, timer completion, or upstream socket EOF.

Both exact Compose projects were independently absent after execution. Nine named default tables and sequences were unchanged. This selected audit does not claim every database object was inspected. No runtime setting, acceptance budget or retry was changed. No performance sampling took place.

The exact tools and sanitized observations are under `scripts/rust-vod-contracts/four-second-main/`. `blocked11` is the frozen blocked controller; `execution12` records the historical cue-guarded execution. Its cue is expired and grants no authority for another run. Historical artifact paths are retained as provenance, not advertised as a portable one-command replay. Raw reports and authentication material remain private. The raw summary's inherited `UNEXECUTED` docstring is preserved; the sanitized summary explicitly identifies the actual executed observation.

Next: qualify a reservation-specific observation policy against the Original first-read behavior, preserving physical-operation ownership, cancellation, busy results and ordinary coordinator deadlines. Then validate a new immutable executable and packaged image. Image50's observed failure must remain in the evidence history.
