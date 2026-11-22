# SQLx idle maintenance on the ordinary f960875 image

Original Dispatcharr authors and contributors retain full application, algorithm and UI credit. PostgreSQL, SQLx, FFmpeg and other dependency authors retain full credit for their work. See [upstream attribution](../ATTRIBUTION.md).

The old ordinary 65db short run failed its post-idle resource gate. A separate instrumented run with the same workload retained a physical exit 1. Its clock and process join exited 0 and associated all seven persistent PostgreSQL backends with the same Native Main generation across 11 idle samples. All 77 observations identified an idle timestamp reset during the maintenance sweep.

The ordinary `f960875` package passed a fresh 120-second run and independent reducer with exact five-role Compose cleanup. Ten viewers received H.264/AAC through one upstream encoder. Both scheduled XMLTV jobs completed, and the fixture verified 20 decode clips. The package combines the AIO role selection and idle maintenance corrections. The Store retains the 60-second idle timeout with a five-second maintenance interval, and the reaper preserves each already idle connection’s timestamp.

| Run | Processes | Threads | File descriptors | Socket descriptors | Median working-set change, MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| Ordinary 65db | +6 | +5 | +143 | +14 | +17.70 |
| Private diagnostic 65db | +6 | +5 | +168 | +14 | +19.30 |
| Ordinary f960875 | -2 | -3 | -158 | -2 | +0.86 |

Each row shows post-idle growth within its own run using the unchanged census and working-set budgets. The ordinary package’s result applies to this bounded workload. The 360-second prefix, two-hour soak, final release resource comparison, and remaining M9 cells have separate gates. [Machine-readable evidence](../scripts/rust-integration/m9-f960875-sqlx-idle-correction.json)
