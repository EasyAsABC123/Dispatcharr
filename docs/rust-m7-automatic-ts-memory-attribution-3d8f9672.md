# M7 automatic TS Connect memory measurements

In the paired test on Rust application `3d8f9672`, both backends completed six signed TS cycles, delivered 451,200 media bytes and acknowledged 12 callbacks per arm. The paired performance gate failed on charged memory. [Machine-readable evidence](rust-m7-automatic-ts-memory-attribution-3d8f9672.json)

| Metric | Original | Rust | Change |
| --- | ---: | ---: | ---: |
| CPU core-seconds | 3.916987 | 2.417005 | −38.3% |
| Peak summed process RSS | 559,665,152 B | 388,980,736 B | −30.5% |
| Peak summed cgroup charge | 406,896,640 B | 541,777,920 B | +33.1% |

Before the first TS offer, the synthetic Origin container carried 27,205,632 B in the Original arm and 102,580,224 B in the Rust arm. Both Origin containers had 10,919,936 B of anonymous memory at that point. Origin differed by 73,867,264 B at the two charged-memory peaks, about 54.8% of the arithmetic gap. The roles reached these peak rounds at different elapsed times. The observation measures which cgroup carried the charge. Its underlying allocation remains unconfirmed.

The retained samples kept only the anonymous entry from `memory.stat`. The next paired owner retains the full per-role kernel memory map and matched Origin media and mount observations. It preserves the existing equivalent-work and scoring thresholds.

Original Dispatcharr and upstream contributors retain full authorship and license credit.
