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

The committed source `566d94474d156000f04c6ad7e65548b5e1136c93` builds in
471.805 seconds, producing image
`sha256:7d41646ca5db1847d7367a0926504d8db4ff2ba2902d2c69277b931c084a2f64`.
The [six package families](../scripts/rust-integration/milestone-batch43/package/inventory.json)
all exit zero. The existing archive probe verifies 139 Comskip libraries and
272 total executable/library/configuration/notice files, then observes the
original unsupported-version exit 2 and banner through the actual wrapper.
Independent cleanup confirms all 55 owned container IDs are absent.
The [combined Native cohort](../scripts/rust-integration/milestone-batch43/application/FAILED.json)
exits 1 in 71.030 seconds. All three contexts were attempted; no business
workflow qualified. Main panics on the new Comskip route conflicting with the
existing recording catchall. Comskip fixture setup independently fails an
out-of-range rootless input-copy UID. Four exact containers and project resources
are independently absent, as are three captured positive host PIDs. One fast
panicked Main had PID zero at its first inspection; that original generation
was never captured, and the raw cleanup false remains unchanged.
The successor routes Comskip through the existing recording dispatcher and
uses a byte-identical host-owned transfer file.

The changed [XMLTV Docker target](../scripts/rust-integration/milestone-batch43/XMLTV/qualification.json)
passes in 54.259 seconds: 14 tests, zero failures, all 1,144 retained Source32
parser comparisons and actual parity CLI exit zero. The application compilation
step is an actual Docker cache hit. The target and same-container parity ELF
are captured, with exact container/host-generation/project cleanup. This proves
that changed build path; it does not qualify the failed Main workflow or an
application CPU/memory gain. The failed cohort was intended to cover five VOD update phases with retained Source capture,
three manual Comskip cases using the exact retained MKV, and shared VLC viewing.
The qualified Source branch is actual exit-one/no-commercials; nonempty marks/
FFprobe, automatic Comskip, cut, Series, broader metadata/roles/concurrency,
protocol faults, full matrices and matched-work performance remain open.
