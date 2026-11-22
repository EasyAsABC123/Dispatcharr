# Isolated Docker Cargo rebuild comparison

The corrected BuildKit pilot compared two Cargo rebuild policies on the same
`424880a9` child and separate cold caches of
`9ace9f66`. The compiler ran offline
with the same arguments and identical absolute paths in each arm. The dependency
warm-up and cache-mount control ran separately.

| Arm | Cargo wall time | Outer Docker build | Local crates compiled |
| --- | ---: | ---: | ---: |
| Cold all-touch | 441.5 s | 444.3 s | 29 |
| Cold content-aware | 440.0 s | 442.7 s | 29 |
| Warm all-touch | 341.1 s | 345.8 s | 29 |
| Warm content-aware | 145.3 s | 148.3 s | 3 |

The content-aware arm changed timestamps only on changed inputs. Warm Cargo
time fell 57.4% (195.8 seconds). Outer Docker build time
fell 57.1% (197.4 seconds).
The Main and operator executables matched byte for byte and by file mode
across policies at both revisions.

The earlier pilot embedded two different generated workspace paths in its
executables and stopped before its warm comparison. Both corrected arms used
the same fixed generation name inside independent cache mounts. The cold
binary check passed before either warm build began.

These measurements cover the isolated compiler target. The production
build path, complete packaged image, and application CPU and memory have
separate gates.
[Machine-readable evidence](../scripts/rust-integration/m10-docker-build-cache-ab.json)

The next package comparison uses the original `/workspace/rust` compiler
source and `/tmp/dispatcharr-target` target paths. It compares both
production executables and modes and runs an exact-image, non-root Compose
smoke on the complete package. The ordinary build remains the fallback
when cache state is missing or fails verification.

Original Dispatcharr and dependency authors retain full credit.
[Attribution](../ATTRIBUTION.md)
