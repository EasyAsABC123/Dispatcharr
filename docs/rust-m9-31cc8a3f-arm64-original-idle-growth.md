# ARM64 Original idle memory growth in the paired workload

The Original completed the ten-viewer workload, two guide trees,
and 20 independently decoded captures. Its checked service working
memory rose 147.9 MiB between idle-window medians, exceeding the
predeclared 64 MiB limit. Even the lowest after sample exceeded the
highest before sample by 88.0 MiB. The independent audit reproduced
the rejection from the retained measurements. The application and
fixture processes retired cleanly.

The growth was in the Original application container. The aggregate
excludes the origin and VM observer. The retained data can identify
the container, while process and memory-type attribution still need
a separate read-only diagnostic. Native was not started, so the run
yields no CPU or memory comparison. The separate full-scan benchmark
remains deferred.

Original Dispatcharr and its upstream contributors retain full credit.
[Machine-readable memory observations](../scripts/rust-integration/m9-31cc8a3f-arm64-original-idle-growth.json)
