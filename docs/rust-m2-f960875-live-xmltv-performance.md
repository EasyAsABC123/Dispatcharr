# Ten live viewers during XMLTV import on f960875

Original Dispatcharr, React, and media dependency contributors retain full credit. See [upstream attribution](../ATTRIBUTION.md).

One fresh Original/Rust Compose pair served ten authenticated H.264/AAC MPEG-TS viewers from one upstream and one provider slot. Each arm measured 60 seconds. The same XMLTV import was submitted five seconds into each measurement window, and Refresh, Index, and Series passed while all ten readers advanced. Both arms passed real audio and video decode, confirmed equivalent useful work, used zero swap, and verified exact cleanup.

| Measurement | Original | Rust | Change |
| --- | ---: | ---: | ---: |
| Backend and infrastructure CPU, core-seconds | 3.139 | 1.430 | 54.4% lower |
| Backend and infrastructure peak working set, MiB | 872.711 | 190.887 | 78.1% lower |
| Whole fixture CPU, core-seconds | 4.961 | 2.526 | 49.1% lower |
| Whole fixture peak working set, MiB | 1163.734 | 483.559 | 58.4% lower |

Backend and infrastructure includes the app, resident job workers, PostgreSQL, and Redis. The whole fixture also includes the origin and observers. Rust used its explicit Selected worker profile in the packaged application. The pair measures this workload once. It leaves repeat trials and release-wide performance open. [Machine-readable evidence](../scripts/rust-integration/m2-f960875-live-xmltv-performance.json)
