# M4 signed VOD and M9 six-minute soak findings

## Signed VOD

The pinned Original `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` and ordinary AMD64 Rust `2daeb067563627338ff767a9d63df105aea8c734` were exercised with three fresh signed VOD sessions on each backend. The non-admin viewers had verified bearer-to-session mappings. The explicit out-of-bounds request returned identical HTTP 416 responses and the full requests returned identical 4,299,455-byte MP4s. Both full captures passed strict decoding, and the fixture verified exact Compose teardown.

For the suffix request, Original and Rust both returned HTTP 206, advertised a length of 257, and reported `Content-Range: bytes 0-256/4299455`. The bodies differed. Original delivered the last 256 bytes of the actual MP4, and Rust delivered the first 257. Independent SHA-256 checks against the full MP4 established the offsets. The signed comparison failed on suffix size and digest. The [seven-case direct-wire receipt](rust-m4-fresh-vod-range-2daeb067.md) remains valid for its recorded Original request sequence.

Original's upstream request forwards the literal suffix while its connection has no saved media length. With a saved length, Original applies its prefix interpretation before forwarding the request. Its downstream response builds the range headers from the original request and discovered size. The captured signed request ran first on Original, while the retained direct-wire sequence ran HEAD, full playback, and explicit ranges before the suffix case. The next replay captures the physical Origin request for fresh and previously initialized sessions to establish which state explains the difference. Signed VOD and the broader M4 gates remain open.

The one-line follow-up `b0123bcaa0684d12d69b647b331863dec058b2c0` uses the equivalent Clippy-recommended range guard. Formatting, strict workspace Clippy, and all 194 workspace test targets passed. There were 1,984 passing tests, 839 ignored tests, and zero failures. The physical receipts above remain associated with the package built before the lint follow-up.

## Six-minute ARM64 soak

The exact ordinary ARM64 `3d8f96725ed90f5d37e406492e264e7c4d37f89a` package maintained 73 scheduled observations across ten readers, and the owned Native candidate remained ready. At the five-minute XMLTV refresh, token exchange returned HTTP 200 JSON, and the signed import returned a 145-byte HTTP 500 HTML response in about 6.76 ms. Readiness returned HTTP 200 immediately afterward. The run exited 1. Exact fixture teardown and Compose down reported zero, and the engine census showed zero active containers. The independent acceptance proof did not run.

The exact 500 body is common to multiple Rust branches. A UI account policy error, SQL failure during source lookup, or publication error remains compatible with the retained evidence. The next diagnostic observes the signed UI guard, source SQL, and broker queue state at the timed import. The timed 202 requirement, two-hour follow-on, and whole-service performance gates remain open.

Original Dispatcharr and all upstream contributors retain full credit for their work.
