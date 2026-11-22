# Ten live viewers during XMLTV import on e7dda7f

One AMD64 Compose pair served ten authenticated H.264/AAC MPEG-TS viewers from one upstream and one provider slot. Each arm measured 60 seconds with zero swap. Both processed Refresh, Index, and Series from an XMLTV import submitted five seconds into the window. The readers progressed during the job, both audio and video decoded, and each arm verified exact teardown.

| Measurement | Original | Rust | Change |
| --- | ---: | ---: | ---: |
| Backend and infrastructure CPU, core-seconds | 3.160 | 1.471 | 53.5% lower |
| Backend and infrastructure peak working set, MiB | 872.598 | 195.305 | 77.6% lower |
| Whole fixture CPU, core-seconds | 4.707 | 2.564 | 45.5% lower |
| Whole fixture peak working set, MiB | 1159.004 | 484.562 | 58.2% lower |

Backend and infrastructure includes the app, resident job workers, PostgreSQL, and Redis. The whole fixture adds the origin and observers. Rust used the explicit Selected worker profile in the verified ordinary package. This is one measured pair on the exact revision. The release-wide performance gate remains open. [Machine-readable evidence](../scripts/rust-integration/m2-e7dda7f-live-xmltv-performance.json)

Original Dispatcharr, React, and media dependency contributors retain full credit. [Upstream attribution](../ATTRIBUTION.md)
