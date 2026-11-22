# Post-playback resource gate on the 65dbbbd short soak

Original Dispatcharr authors and contributors retain full application credit. See [upstream attribution](../ATTRIBUTION.md).

The 120-second qualification on `65dbbbd` recorded 26 passing workload checks, including two completed XMLTV refreshes and retirement of the viewers’ owned Redis state. The post-playback resource gate then exited 1. The independent Compose cleanup passed. The 360-second prefix and two-hour run await a passing short qualification.

| Post-playback change | Observed | Allowed |
| --- | ---: | ---: |
| Processes | +6 | +0 |
| Threads | +5 | +4 |
| Open file descriptors | +143 | +16 |
| Socket descriptors | +14 | +16 |
| Working memory | +17.7 MiB | +64 MiB |

The infrastructure group accounts for the process increase. The recorded census lacks process role names, so it does not identify the component that retained them. The passive repeat below kept the workload and limits. [Machine-readable evidence](../scripts/rust-integration/m9-65dbbbd-short-soak-resource-stop.json)

## Passive repeat on the same image

The repeat completed all 26 workload checks and exact Compose cleanup, and failed the same idle resource gate. Seven new PostgreSQL process generations in the shared infrastructure appeared after the final playback sample and remained in all 11 idle samples. One initial process was absent, for a net increase of six. The Native, origin, and relay roles gained no persistent process generation in that interval. Across the owned roles, open file descriptors rose by 171, threads by five, and socket descriptors by 14.

The process names identify PostgreSQL as the location of the growth. The evidence leaves connection state, socket peers, and the reason for persistence unresolved. The prefix and two-hour gates remain open. [Machine-readable attribution](../scripts/rust-integration/m9-65dbbbd-short-soak-process-attribution.json)
