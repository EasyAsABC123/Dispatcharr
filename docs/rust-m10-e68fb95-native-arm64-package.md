# Ordinary e68fb95 package on native ARM64 Docker Desktop

The original Dispatcharr authors and contributors retain full credit for the application. See [Attribution](../ATTRIBUTION.md).

Lappy built the ordinary e68fb95 Rust package natively for Linux ARM64. The input archive matched 2,159 files, including 497 protected inputs. Both optional SQLite build arguments were 0. The final image inherited the six pinned runtime layers and added two layers containing the exact compiled Rust binaries. The verifier confirmed the ARM64 index and child identities, default user and entrypoint, installed hashes and dynamic linkage.

The first assembly copied parent directory modes that blocked the packaged user. A second assembly copied only the two executables and preserved the pinned runtime directory modes. Its first verifier still expected one new layer. The final verifier proved both file-copy layers and used that same assembled image.

With the packaged user, ordinary Compose startup returned health and React HTML 200. It created one initial administrator, checked normal authenticated routes, preserved all 184 applied migrations, and verified retirement of five containers plus the exact network and volume. The build and [typed verification receipt](../scripts/rust-integration/m10-e68fb95-native-arm64-package.json) cover this package and startup scope. Comparative ARM64 performance and controlled rollout remain open.
