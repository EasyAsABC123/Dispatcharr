# d6be AMD64 Core8, Chromium Live Stats, and scaled M3U

The exact d6be Core8 fixture passed 33 host tests and the fresh physical held-reader and Source/Native cohorts at one, ten, and one hundred readers. These runs measured functional behavior. Equivalent-work Core performance and full M1 acceptance remain open.

Both Chromium arms played advancing video, showed the active channel and client, and displayed an empty Live Stats view after a normal player close. The paired test verified the 112 packaged static assets and read back the HTML, JavaScript, and stylesheet entrypoints over HTTP. The remaining M5 browser and workflow rows stay open.

The scaled M3U fixture returned the same 3,492,158-byte playlist with 6,000 named profiles on each cold, warm, and expired request. The separate quiet comparison measured three requests in each arm and passed every scoped threshold.

| Scaled M3U metric | Original | Rust | Change |
| --- | ---: | ---: | ---: |
| CPU core-seconds | 1.783 | 0.841 | −52.8% |
| Peak charged memory | 466.0 MiB | 328.7 MiB | −29.5% |
| Peak summed process RSS | 579.0 MiB | 410.6 MiB | −29.1% |
| First-byte p95 | 216.35 ms | 43.24 ms | −80.0% |
| Completion p95 | 226.16 ms | 45.83 ms | −79.7% |
| Goodput | 2,694,006 B/s | 3,572,537 B/s | +32.6% |

The full engine inventory matched after each phase. The [typed evidence](rust-d6be0944-amd64-core-live-stats-and-scaled-m3u.json) binds the exact manifest and fixture digests. Full M7 output and Realtime acceptance remain open.

Original Dispatcharr, React, and all upstream contributors retain full authorship and license credit.
