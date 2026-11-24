# M4 cold and initialized VOD Range parity

Docker Compose verified ordinary AMD64 Rust `976c7800395286a03af2d858c7a82f5ee87e63cd` against pinned Original `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`. The tested Rust image was `sha256:aa6971ddf1e6c40bde918a0f9d910dbeffb7b8d652534f4572b80a9a61e9db9d`. Its build manifest SHA-256 was `5e02d018656546f88f7dedb217506f3c73e21d008ed87d9482d1f9fb49c7169e`.

| Case | Result on both packages |
| --- | --- |
| Six cold suffix requests per backend, three signed and three unsigned | The provider received `bytes=-256`. The client received the exact 256-byte tail with the Original 257-byte declaration and the same incomplete-read behavior. Each request used a separate content ID and verified absence of prior provider metadata. |
| Two same-session repeats per backend | After Redis held the discovered media length, the provider received `bytes=0-256`. The client received the complete 257-byte prefix. |
| Signed invalid bounds and full GET | Equal selected headers and bodies, including the exact 416 response and full MP4. Both full captures decoded with strict audio and video checks. |

Rust forwards the suffix unchanged when the media length is unknown. For the verified provider tail response, the HTTP/1 body marks its actual final frame so the server flushes the tail before closing the connection. This preserves Original's response framing without adding a media buffer or changing the ordinary initialized-session path. The four focused HTTP/Redis tests passed. Formatting and strict workspace Clippy passed. The full Rust test suite returned 1,984 passes, 839 ignored and zero failures across 194 targets.

The package build, physical fixture, normal Main stop, and independent exact Compose cleanup succeeded. The private raw comparison SHA-256 is `256d66e706b9eedc9693a4bd78f31a71ee51da6f8fff21551440f9e1d963ff27`.

Original also adopted idle provider state for a distinct authenticated viewer with a new SID. The fixture verified the old state's request counter advanced and the new SID acquired no provider metadata. That observation establishes the Original behavior for a separate Rust adoption gate. The signed 416 case sent no provider request on Original and one on Rust. Both remaining comparisons, the formal M4 gate, and current-package performance stay open.

Original Dispatcharr and all upstream contributors retain full credit for their work. React, license and attribution files match the frozen application.
