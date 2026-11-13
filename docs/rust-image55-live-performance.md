# Image55 measured live performance

All nine fixed paired Docker experiments passed at 1, 10 and 100 authenticated viewers. Root recomputed the complete resource data, verified 1,998 media/transcript hashes and 30 raw corruption controls, and independently checked owned cleanup.

| Viewers | Median CPU reduction | Median peak working-memory reduction | Minimum throughput ratio |
| --- | ---: | ---: | ---: |
| 1 | 69.37% | 51.76% | 1.0015 |
| 10 | 78.89% | 46.54% | 0.9845 |
| 100 | 91.14% | 49.72% | 0.9903 |

The [portable report, plots and reproduction instructions](../scripts/rust-live-perf/image55/README.md) contain 1,080 raw samples (1,062 in-window), 666 measured readers and separate functional/Original qualification evidence. Original Dispatcharr authors retain all application and algorithm credit; the React frontend and original attribution remain unchanged.

These results bind image55/aca6/794 Rust inputs and the live HTTP backend plus common PostgreSQL/Redis/local origin. Workers, browser/driver, Docker daemon and the whole application graph are excluded. The later 806-input recording source is separately qualified. Full rewrite, default graph and whole-milestone acceptance remain open.
