# Ten shared viewers during a live XMLTV import

Original Dispatcharr, React, and media dependency contributors retain full credit. See [upstream attribution](../ATTRIBUTION.md).

One quiet Original/Rust Compose pair measured 60 seconds per arm with ten authenticated H.264/AAC MPEG-TS viewers on one shared VLC input. Each arm used one upstream connection and one provider slot. The same local guide import started five seconds into measurement. Refresh, Index, and Series all succeeded during the window while all ten readers advanced. Both arms passed equivalent decoded audio and video work, the zero-swap guard, and exact cleanup.

| Measurement | Original | Rust | Change |
| --- | ---: | ---: | ---: |
| Backend and infrastructure CPU, core-seconds | 3.187 | 1.501 | 52.9% lower |
| Backend and infrastructure peak working set, MiB | 868.332 | 176.684 | 79.7% lower |
| Whole fixture CPU, core-seconds | 4.809 | 2.604 | 45.8% lower |
| Whole fixture peak working set, MiB | 1164.383 | 477.414 | 59.0% lower |

Backend and infrastructure includes the app, resident job workers, PostgreSQL and Redis. The whole fixture also includes the origin and measurement observers. The Rust arm used `9cf640701dacea07ea5f394ce7b18a6fa7c8c088` with the explicit Selected worker profile. The result describes this one measured workload. Repeat trials, the default worker graph and release-wide performance remain open.

The first physical attempt stopped before measurement because the Original fixture lacked its `epgs` directory. Its exact cleanup passed. The corrected fixture creates the directory before writing the owned guide. [Measured values and raw receipt digests](../scripts/rust-integration/m2-9cf6407-live-xmltv-performance.json)
