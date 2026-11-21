# Native ARM64 Rust release binaries

The native ARM64 GitHub-hosted job built the `Dockerfile.app` `build` target for Dispatcharr Rust application `d426d322b27a197aaaaa057ef73dfb53c516c452`. It used the same 1,470 reviewed compiler, helper, license, and attribution entries at docs revision `29a02d10e0c4de683fd0021b8a665637cec55202`. The isolated validation commit `12829d297a79e1b7409190987e79bb82e4d4dafa` preserved the original Dispatcharr authors' full credit. [GitHub Actions run 44879696241](https://github.com/EasyAsABC123/Dispatcharr/actions/runs/44879696241)

The official ARM64 Rust 1.90.0 Bookworm builder matched its pinned OCI manifest, config, and filesystem layers. The job admitted all **328 locked crates** by Cargo.lock archive hash and checked **15,050** expanded source files against Cargo's checksums. The two tracked SQLx patches and the guarded PostgreSQL helper remained part of the reviewed build inputs. The compile ran with Docker build-step networking disabled and Cargo in locked, offline mode.

| Executable | SHA-256 |
| --- | --- |
| `dispatcharr-rs` | `d39f4255f88dda4d60248061cdfbcf512ed09502a260b2564ff7351846aac1ac` |
| `dispatcharr-operator` | `83677f8369461221a157a7041d454be0b434811a241e4cb645bd9de4557f62ac` |

The read-only non-root check identified both as little-endian AArch64 ELF64 executables with the GNU ARM64 loader. It verified the packaged helper digest embedded in Main and matched the operator's expected `--help` output byte for byte. Main was inspected as a binary because invoking it starts the application. The controller exited 0 and verified retirement of the owned containers, build tag, and work directory.

This build qualifies the Rust release executables. Full ARM64 runtime assembly, Compose acceptance, and distribution remain open. The hosted job retained a text-only receipt, whose SHA-256 is `0581183020a7d40edb2f5fac1ea134c5781f8851e91abcf96b5076568e18ccdb`. [Machine-readable evidence](../scripts/rust-integration/m10-native-arm64-rust-build-d426.json)
