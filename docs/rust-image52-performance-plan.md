# Image52 performance measurement plan

Status: planned. Image52 has no measured CPU or memory gain yet. Original
Dispatcharr authors and contributors retain full application and algorithm
credit and their licenses.

The candidate is Git `98e447186af099dd17496539aec46312f6cf4f62`, Docker image
`sha256:983dc2ba0b740d97abf45cd3a51bf96633b75fbce8cbe9ea678bad36647d9b44`.
Its 788 Rust inputs and unchanged Original React inputs are bound by the retained
build manifest. Image48's [published measurements](../scripts/rust-live-perf/image48/README.md)
apply only to its measured image; they are not image52 results.

After the image52 package review, repeat the established authenticated live
experiment with the same workload and budgets. Freeze support Git
`28122faf28a0acf1c136ca0d0831961e786cb686` and the exact seven workload/controller
files from image48. Bind the independent verifier and publication to the new
image52 digest; the historical verifier deliberately accepts its own measured
image only. Keep historical controllers and reports unchanged.

1. Verify the frozen support, controller, application input hashes and local
   validation controls. Run fresh functional Source/native phases at 1, 10 and
   100 viewers, with no resource sampling.
2. Pause competing builds, containers and validation work. Run Source-only
   qualification at those three counts and independently check the retained
   headroom, cadence, work and cleanup evidence before measuring Rust.
3. Use a fresh quiet cue to run all nine fixed pairs: Source/native,
   native/Source, Source/native for each viewer count. Preserve every failure;
   do not adjust budgets or select passing pairs.
4. Independently recompute whole-window CPU deltas, simultaneous working-memory
   sums, per-pair ratios and cohort medians. Verify every retained media prefix,
   decoder transcript, image binding and owned Compose resource cleanup before
   publishing results and standalone plots.

Each measured phase has 15 seconds of warmup and 60 seconds of observation.
The complete matrix has 666 authenticated continuous readers. CPU ratio must
be at most 0.90, peak working-memory ratio at most 0.95, throughput ratio at
least 0.95, and p95 first-byte ratio at most 1.10 with the existing 50 ms floor.
The unchanged protocol also requires per-reader pacing, at least 50 resource
samples per phase, no errors or OOM events, and bounded cleanup. Each reader's
2 MiB prefix must pass the existing bounded audio/video decode; this does not
establish byte equality between timed streams.

The comparison covers the active HTTP backend plus common PostgreSQL, Redis and
the deterministic local origin. Background workers, browser, driver, Docker
daemon and the complete application graph remain outside these numbers. Passing
this comparison alone will not complete M2 or the rewrite. Other milestone
workloads, including import during playback and full-service soak, still need
their own evidence.
