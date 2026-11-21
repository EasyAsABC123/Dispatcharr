# Native ARM64 Rust release binaries

The native ARM64 GitHub-hosted job built the `Dockerfile.app` `build` target for Dispatcharr Rust application `fdede54c54ccb19300c01031537803a0b553f581`. It used the same 1,469 reviewed compiler, helper, license, and attribution entries at docs revision `a7ccdd1f28a2af744a48c1958624535779e68a91`. The isolated validation commit `f4001b8a2b654816a43ea94922bb55f27bc4cd0b` preserved the original Dispatcharr authors' full credit. [GitHub Actions run 44871602843](https://github.com/EasyAsABC123/Dispatcharr/actions/runs/44871602843)

The official ARM64 Rust 1.90.0 Bookworm builder matched its pinned OCI manifest, config, and filesystem layers. The job admitted all **328 locked crates** by Cargo.lock archive hash and checked **15,050** expanded source files against Cargo's checksums. The two tracked SQLx patches and the guarded PostgreSQL helper remained part of the reviewed build inputs. The compile ran with Docker build-step networking disabled and Cargo in locked, offline mode.

| Executable | SHA-256 |
| --- | --- |
| `dispatcharr-rs` | `3b7c2e375c4e1b2565e4fb980c70420b0437f6e9041b4359431c0202f0bc5efe` |
| `dispatcharr-operator` | `d0851f07b3262094f245e974543fba4defbb7cf5fa17588a72f99f4a82341a7a` |

The read-only non-root check identified both as little-endian AArch64 ELF64 executables with the GNU ARM64 loader. It verified the packaged helper digest embedded in Main and matched the operator's expected `--help` output byte for byte. Main was inspected as a binary because invoking it starts the application. The controller exited 0 and verified retirement of the owned containers, build tag, and work directory.

The first local validation-ref publication attempt exited 2 before creating a hosted job. The retained hosted proof is from the separately verified ref. This build qualifies the Rust release executables. Full ARM64 runtime assembly, Compose acceptance, and distribution remain open. The hosted job retained a text-only receipt, whose SHA-256 is `e6d21e727bdcf39a918456ed4509c30620094d52f9455bd335e6ca8d3da0f9ce`. [Machine-readable evidence](../scripts/rust-integration/m10-native-arm64-rust-build.json)
