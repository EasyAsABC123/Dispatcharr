# ARM64 VOD reconnect resource comparison

The frozen `3d8f9672` Rust package and the pinned Original ran one
matched pair on Lappy. Each backend used one signed client and the same
session across two complete 4,299,461-byte HTTP 200 MP4 reads. Each read
reached exact EOF and passed a strict H.264 and AAC decode. Both sides
retired the provider slot and the Redis session. The Origin served two
requests and 8,598,922 bytes per backend.

| Measurement | Original | Rust | Change |
| --- | ---: | ---: | ---: |
| CPU core-seconds | 2.271 | 2.407 | 6.0% higher |
| Peak concurrent working memory | 365.3 MiB | 215.7 MiB | 40.9% lower |
| First-body-byte latency, read 1 / read 2 | 269.4 / 274.8 ms | 264.1 / 274.9 ms | −5.3 / +0.1 ms |
| Delivered goodput, read 1 / read 2 | 266.28 / 266.13 kB/s | 266.20 / 266.24 kB/s | −0.03% / +0.04% |

The measurement includes the backend, PostgreSQL, Redis, Origin and the
signed probe with its decoder. The independent observer took 36 samples
per backend and recorded its own CPU separately. Both arms used the
same CPU and memory limits and one codec thread per decode. The proof
verified exact lifecycle and cleanup and passed the zero-new-throttling
gate. Two earlier matched attempts reached exact cleanup but exceeded
their declared throttling gate. The archived proof covers this one
ARM64 workload. The AMD64 sustained and transformed timeshift scopes
and complete M4 acceptance are separate.

Dispatcharr and React remain the work of the original authors and
contributors. FFmpeg, PostgreSQL, Redis and the other dependencies retain
full authorship and license credit.
[Machine-readable proof](rust-m4-arm64-vod-reconnect-resource-3d8f9672.json)
