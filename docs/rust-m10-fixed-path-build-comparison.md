# Fixed-path Cargo cache experiment

Original Dispatcharr authors and contributors retain full application credit. See [upstream attribution](../ATTRIBUTION.md).

The two private Docker BuildKit recipes used the same production source, target, and output paths. The cold arms used exact `9ace9f6` source. The warm arms applied the same nine changed Rust inputs for `424880a`. The existing recipe refreshed all 1,573 compile inputs. The content-aware recipe refreshed nine.

| Measurement | All-touch | Content-aware |
| --- | ---: | ---: |
| Cold Cargo | 439.982 s | 440.709 s |
| Warm Cargo | 339.796 s | 145.585 s |
| Warm compile step | 340.349 s | 146.418 s |
| Warm full image build | 342.785 s | 148.099 s |

This pair measured a 57.2% reduction in warm Cargo time and a 56.8% reduction for the warm full-image build. Both Main and operator executables matched byte for byte and by mode in both phases. Both final private images passed their non-root operator and exact cleanup checks.

The comparison measures build performance for these exact inputs. The ordinary release recipe and application runtime performance gates retain separate evidence. [Machine-readable evidence](../scripts/rust-integration/m10-fixed-path-build-comparison.json)
