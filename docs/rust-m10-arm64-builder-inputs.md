# Pinned ARM64 builder inputs

The Dockerfile in `f2db61e5` and `d608ff75` selects the same official Rust,
Debian, and Node Linux ARM64 image inputs. The audit verified each child
manifest and its config against the pinned parent OCI digest, including the
architecture and layer inventory.

Debian's signed Bookworm package index supplies all five ARM64 equivalents
of the existing AMD64 fallback packages. The audit checked the archive
signature using the keyring from the pinned Debian ARM64 base layer. It
verified the index digest, each package's exact digest and size, and the
package, architecture, and version in the Debian control records.

The two newest verified layers of the exact Rust builder establish its
effective package inventory. The builder already contains ARM64 m4
`1.4.19-3` and liblz4-1 `1.9.4-1`. The fallback candidates for building the
guarded PostgreSQL helper are Bison, Flex, and liblz4-dev.

| Required fallback candidate | Bookworm version |
| --- | --- |
| Bison | `2:3.8.2+dfsg-1+b1` |
| Flex | `2.6.4-8.2` |
| liblz4-dev | `1.9.4-1` |

The ARM64 Docker build still needs to consume the authenticated inputs and
verify the resulting helper ELF, loader, private libraries, non-root restore,
and copied-data replay on a native ARM64 host. Package distribution also
requires the exact corresponding Comskip source and dependency notices.
[Earlier platform audit](rust-m10-arm64-package-inputs.md) ·
[Verified input receipt](../scripts/rust-integration/m10-arm64-builder-inputs.json)

The original Dispatcharr, Rust, Debian, Node.js, Comskip, PostgreSQL, and
dependency authors and contributors retain full credit. [Attribution](../ATTRIBUTION.md)
