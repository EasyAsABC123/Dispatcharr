# F38 ARM64 existing-VM S360 descriptor audit failure

The exact ordinary ARM64 F38 package completed the signed 360-second workload on the existing VM. It recorded 73 active samples, 20 useful audio/video captures and one signed guide terminal at the scheduled +300-second refresh. The physical owner and cleanup exited zero and the engine was empty. The independent acceptance audit exited one. File-descriptor growth was the sole rejected idle gate.

| Measured role | Descriptors before | Descriptors after | Descriptor growth | Socket growth |
| --- | ---: | ---: | ---: | ---: |
| Full fixture | 268 | 497 | +229 | −90 |
| Stock Original baseline | 171 | 445 | +274 | −45 |
| Rust candidate | 93 | 48 | −45 | −45 |
| Origin | 4 | 4 | 0 | 0 |

The whole fixture used the unchanged idle limit of +16 descriptors. Rust's descriptor decrease matched its socket decrease. The Stock Original baseline supplied the measured increase, with growing non-socket descriptor counts. The independent projection replayed all 22 retained idle observations. All nine baseline process generations were present throughout. One stable non-init baseline process contributed the +319 non-socket descriptors and another lost 45 descriptors. The baseline namespace init was flat. The exact Rust application remained at 18 non-socket descriptors. The retained census did not record executable classes for those two baseline processes or distinguish file, pipe and anonymous-inode targets.

The full fixture's idle working-set growth was +30,908,416 B against the unchanged 67,108,864 B limit. Threads grew by one and live process count stayed at 11. The original exact S120 memory failure remains on record. The existing-VM S120 repeat passed its scoped check, and this S360 audit failure prevents S7200 admission.

The retained S360 observation SHA-256 is `e841bd35cb934f252a24194e21c0d3d28f78361d476aea4ecdeb55b3112f0b56`. The independent per-process projection SHA-256 is `6c9fa96729693b880d88cd1729c10413440a71863031864daae7683737a2e64e`. The original Dispatcharr authors and contributors retain full credit.
