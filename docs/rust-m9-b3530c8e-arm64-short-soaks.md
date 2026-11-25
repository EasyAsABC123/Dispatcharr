# B353 ARM64 short-soak evidence

The exact ordinary B353 ARM64 package ran both short soaks. Workloads and owned cleanup exited zero, and the Docker engine was empty after each. The 120-second proof passed. The 360-second proof verified the exact passing same-build predecessor and then failed at the descriptor limit. [Typed evidence](rust-m9-b3530c8e-arm64-short-soaks.json)

| Idle median growth | 120 seconds | 360 seconds | Existing limit |
| --- | ---: | ---: | ---: |
| Working memory | +18,157,568 B (17.3 MiB) | +33,611,776 B (32.1 MiB) | +64 MiB |
| File descriptors | +2 | +229 | +16 |

The 360-second run had 73 active observations, and descriptor medians climbed from 268 to 497. The two retained 11-sample idle panes independently reproduce that increase. Socket descriptors fell by 90. The earlier F38 diagnostic attributed extra regular-file descriptors to the PostgreSQL checkpointer. The exact B353 formal run establishes its own aggregate metric and does not identify the worker subtype independently.

The 360-second gate and the two-hour qualification remain open. Original Dispatcharr, PostgreSQL, and upstream dependency authors retain full credit.
