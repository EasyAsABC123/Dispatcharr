# Owned six-minute soak prefix on d608

The original Dispatcharr authors and contributors retain full credit.
[Attribution](../ATTRIBUTION.md)

The six-minute diagnostic on Native
`d608ff758c4a937961439f4389d01040f560122d` exited 1. The ten viewers and a
single upstream encoder remained active through the 360-second timeline. The
scheduled XMLTV refresh at +300 seconds reached its terminal. All ten reader
transports closed, the viewer-owned Redis state retired, and independent
cleanup confirmed all five Compose role generations were absent.

The resource verifier stopped the run after the complete post-idle measurement
and before it qualified the captured clips. Aggregate cgroup working memory,
measured as `memory.current` less `inactive_file`, increased by 126,754,816 bytes
from its pre-idle median of 200,331,264 bytes. The ceiling was 67,108,864 bytes.
Independent replay of the retained counter samples reproduced the summary. The
process, thread, descriptor, socket, TCP TIME_WAIT and OOM limits all passed.

| Measured role | Pre-idle median bytes | Post-idle median bytes | Difference bytes |
| --- | ---: | ---: | ---: |
| Native Main | 10,170,368 | 87,318,528 | +77,148,160 |
| PostgreSQL and Redis fixture | 120,639,488 | 165,355,520 | +44,716,032 |
| Origin and encoder | 59,457,536 | 61,460,480 | +2,002,944 |
| Unix relay | 9,613,312 | 12,431,360 | +2,818,048 |

The aggregate takes the median of the summed roles at each sample. These
cgroup counters measure resident working memory across the container
workload. The retained samples lack the allocation breakdown needed to
attribute the growth to particular process allocations or cache classes.
The 120-second qualification is a separate result. The six-minute prefix,
the two-hour soak, and comparative CPU and memory performance remain open.
[Short soak](rust-m9-d608-short-soak.md), [Redacted failure receipt](../scripts/rust-integration/m9-d608-six-minute-soak-prefix-failure.json)
