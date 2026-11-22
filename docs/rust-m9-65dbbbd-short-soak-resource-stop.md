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

## PostgreSQL socket counterparts

A further repeat paired all seven persistent PostgreSQL generations with the same Native Main process in all 11 idle samples. The observer matched both ends of each owned established TCP socket and opened zero database sessions. The result identifies which process retains the sockets. It leaves PostgreSQL session activity and the cause of retention unresolved.

The observer kept the same 120-second workload, sample schedule, and limits. The run completed all 26 workload checks and exact cleanup. The idle resource gate again exited 1, with six additional processes, 167 open file descriptors, five threads, and 14 socket descriptors. [Machine-readable counterpart evidence](../scripts/rust-integration/m9-65dbbbd-postgres-counterpart.json)

## SQLx pool-state diagnostic

The private instrumented image retained the 120-second workload, 11 post-idle samples, and original resource budgets. It matched all seven persistent PostgreSQL processes to the corresponding SQLx pool events in Rust Main. All 77 process/sample classifications observed a maintenance requeue that reset an idle timestamp. The live trace established that these connections had returned to the idle queue. The observer opened zero database sessions.

All 26 workload checks and exact cleanup passed. The short resource gate again exited 1. The independent trace join exited 0. A fresh ordinary-image run is required to qualify any correction before advancing the soak. [Machine-readable pool-state evidence](../scripts/rust-integration/m9-65dbbbd-sqlx-idle-reset.json)
