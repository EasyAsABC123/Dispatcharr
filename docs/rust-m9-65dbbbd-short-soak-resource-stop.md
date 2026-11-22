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

The infrastructure group accounts for the process increase. The recorded census lacks process role names, so it does not identify the component that retained them. The next diagnostic will record process roles with the same workload and resource limits. [Machine-readable evidence](../scripts/rust-integration/m9-65dbbbd-short-soak-resource-stop.json)
