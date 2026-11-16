# Candidate43: verified loader isolation and reusable Docker compilation

Original Dispatcharr, Comskip, FFmpeg and dependency authors retain all original
application, algorithm, configuration and library credit. React is unchanged.

The [Candidate42 package failure](rust-milestone-batch42.md) is preserved.
Its actual loader diagnostic establishes the correct empty-list override:
`--inhibit-rpath ""`. The wrapper now prevents Comskip's embedded system RPATH
from overriding its exact captured library bundle. The original binary,
configuration and algorithms remain unchanged. Unknown corresponding-source
revision and broader release provenance limits remain explicit.

The Docker build copies Cargo files, crates, vendored dependencies and the one
runtime JSON used by a compile-time include before compilation. Mutable runtime
collectors are excluded from that build layer. One locked BuildKit target cache
and registry cache support two build jobs. Both application executables are
copied out of the cache mount to `/out` before the final image copies them;
the XMLTV test target uses the same cache. This first build seeds the cache.
No build speedup or application CPU/memory improvement is inferred yet.

[SDK60](../scripts/rust-integration/milestone-batch43/checks.json) passes all four
gates: 1,145 tests passed, zero failed and 784 fixture tests ignored across
180 result blocks. There are 1,448 inputs, including 1,442 Rust inputs. Only
the collector and Dockerfile differ from SDK59; Rust source and Cargo files
remain identical to the SDK58 physical 19-statement/six-negative proof.

Docker build, all six package families, and the combined Native cohort remain
pending. The cohort covers five VOD update phases with retained Source capture,
three manual Comskip cases using the exact retained MKV, and shared VLC viewing.
The qualified Source branch is actual exit-one/no-commercials; nonempty marks/
FFprobe, automatic Comskip, cut, Series, broader metadata/roles/concurrency,
protocol faults, full matrices and matched-work performance remain open.
