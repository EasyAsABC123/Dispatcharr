# ARM64 signed VOD reconnect resource comparison on D6

In one matched ARM64 VOD reconnect pair, Rust used **5.84% more CPU**
and **38.37% less peak working memory** than Original. Both arms used one
signed client and one session for two complete 4,299,461-byte HTTP 200 MP4
reads. Both reads passed strict H.264 and AAC decode. The proof verified
two upstream requests per backend, provider slot release after each read,
and session retirement after the second read.

| Measurement | Original | Rust | Change |
| --- | ---: | ---: | ---: |
| CPU core-seconds | 2.1657 | 2.2923 | 5.84% higher |
| Peak concurrent working memory | 356.0 MiB | 219.4 MiB | 38.37% lower |
| First body byte, read 1 / read 2 | 261.62 / 270.60 ms | 259.32 / 274.41 ms | −2.30 / +3.81 ms |
| Goodput, read 1 / read 2 | 266.23 / 266.11 kB/s | 266.12 / 266.28 kB/s | −0.04% / +0.06% |

The measurement includes the backend, PostgreSQL, Redis, Origin, and the
signed probe with its decoder. Each arm used the same limits and one
decoder thread. The independent observer recorded 36 samples per arm and
reports its own CPU separately. The proof passed the unchanged cgroup
limits, zero-new-throttling, no-swap, no-OOM and exact cleanup checks.

The first launch exited before opening the resource owner because the
independently verified HLS proof had yet to be placed in its expected role
directory. The retry bound the exact proof bytes. Its cold check,
admission, run and separate proof all exited zero, and the running engine
was empty afterward.

The result covers one two-read VOD reconnect pair. Sustained VOD,
transformed catch-up, Timeshift and full M4 remain separate gates.

Original Dispatcharr, React, FFmpeg, PostgreSQL, Redis and upstream authors
retain full authorship and license credit. [Machine-readable evidence](rust-m4-arm64-vod-reconnect-resource-d6ebd813.json)
