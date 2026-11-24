# Signed live stream resource comparison on AMD64

Beepboop compared the exact ordinary Rust package
`3d8f96725ed90f5d37e406492e264e7c4d37f89a` with pinned Original
`1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` in isolated Docker Compose.
The full 1, 10 and 100 viewer comparison used three alternating pairs
per cohort, 15 seconds of warmup and 60 seconds of measurement per arm.
The resource scope includes the application backend, common PostgreSQL
and Redis infrastructure, and the shared Origin service.

| Concurrent viewers | Median CPU reduction | Median peak working memory reduction | Lowest Rust goodput vs Original | Largest gated first-byte p95 ratio |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 39.9% lower | 50.4% lower | 98.31% | 1.000× |
| 10 | 60.2% lower | 45.1% lower | 97.97% | 0.389× |
| 100 | 88.1% lower | 47.3% lower | 98.34% | 0.078× |

Every pair passed the CPU, working memory, goodput and first-byte gates.
The latency gate uses the same 50 ms floor for both backends. The preceding
six functional windows, three Original qualification windows and all 18
timed windows observed at most one concurrently open upstream connection.
All readers performed the signed workload and strict media checks. The
independent verifier reconstructed the numerical result and confirmed
exact project teardown. This evidence qualifies the named live workload.
The remaining whole-milestone and release-matrix gates are tracked separately.
[Redacted measured proof](rust-m1-amd64-signed-live-resource-3d8f9672.json)

The original Dispatcharr, Django, PostgreSQL, Redis, FFmpeg and dependency authors retain full credit.
