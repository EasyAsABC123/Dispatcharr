# fdede54c canonical playback soak short and prefix

The exact fdede54c Rust package passed 120 seconds of short playback and
a separate 360-second canonical prefix in fresh isolated Compose projects.
Both used ten simultaneous H.264/AAC viewers and one shared upstream
encoder generation. Twenty media clips decoded successfully in each run.
The short run settled two owned XMLTV refreshes scheduled at +5 and +60
seconds. The prefix settled one at +300 seconds while viewing continued,
using an ordinary Native refresh-token exchange. The prefix recorded 73
active progress samples. The viewer and provider state retired and all
five owned container and host process generations retired per project.

| Phase | Pre-idle median | Post-idle median | Change | Ceiling |
| --- | ---: | ---: | ---: | ---: |
| 120-second short | 202,485,760 B | 201,465,856 B | −1,019,904 B | 67,108,864 B |
| 360-second prefix | 198,987,776 B | 221,036,544 B | +22,048,768 B | 67,108,864 B |

Each figure is aggregate working memory from the owned service cgroups.
Each 20-second idle window had 11 complete censuses. Both phases stayed
within the unchanged growth ceiling of the larger of 64 MiB or 10% of the
pre-idle median. Native Main's cgroup limits were two CPU cores, 2 GiB of
memory and 512 PIDs. The canonical observer and limits were unchanged
from the prior recipe. The separate d608 diagnostic remains evidence for
that image only.

These passes admit the same image to the separately run 7,200-second
workload. The two-hour workload, integrated M9, and comparative
performance remain pending.
[Redacted receipt](../scripts/rust-integration/m9-fdede54c-canonical-soak-short-prefix.json)

The original Dispatcharr and dependency authors retain full credit.
[Attribution](../ATTRIBUTION.md)
