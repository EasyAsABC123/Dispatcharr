# Ten live viewers plus XMLTV on 9f29dbe

In one 60-second paired AMD64 test with ten authenticated viewers, Rust used 56.0% less backend and infrastructure CPU and 78.0% less peak working memory than Original. Both arms used exactly one active upstream connection and one provider slot. Each viewer made progress during a local XMLTV import and produced independently decoded H.264 video and AAC audio. Both arms completed the same Refresh, Programme Index and Series Evaluation jobs.

| Measure | Original 1241b3f7 | Rust 9f29dbe | Reduction |
| --- | ---: | ---: | ---: |
| Backend and infrastructure CPU, core-seconds | 3.175 | 1.398 | 56.0% |
| Backend and infrastructure peak working set, MiB | 870.6 | 191.4 | 78.0% |
| Whole fixture CPU, core-seconds | 4.822 | 2.490 | 48.4% |
| Whole fixture peak working set, MiB | 1,143.6 | 452.6 | 60.4% |

The fixed measurement took 61 cgroup samples per arm, after warmup, on the same quiet host. CPU is the cumulative usage delta. Working set is the peak of the same-tick sum of each cgroup's charged memory less inactive file cache. The whole-fixture figures include the origin and observers. Both arms completed with zero swap or OOM and passed exact teardown. The [aggregate receipt](../scripts/rust-integration/m2-9f29dbe-live-xmltv-performance.json) records raw artifact digests, per-arm totals and clock bounds.

This is one descriptive comparison of the exact ordinary package under its explicit Selected/Legacy Beat workload profile. The earlier `e7dda7f` figures remain tied to that revision. Claiming release-wide performance still requires repeat coverage and the other workload gates.

Original Dispatcharr, FFmpeg and dependency contributors retain full credit.
