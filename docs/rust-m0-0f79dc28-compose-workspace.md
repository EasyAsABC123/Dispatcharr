# M0 Rust workspace Compose verification on 0f79dc28

The isolated Compose `verify` service built and ran the Rust workspace for `0f79dc28f8718a05a6b28dc554be958e3e295b47` with Rust 1.90.0. Configuration, image build, formatting, strict Clippy, tests and cleanup all exited 0.

- `cargo fmt --all -- --check` and locked, offline workspace Clippy with `--all-targets -D warnings` passed.
- Locked, offline Cargo tests ran 193 test binaries. They reported **1,491 passed, 0 failed and 814 ignored**.
- The verifier ran with Compose networking disabled. Cleanup removed the owned verifier image and verified empty container, network and volume inventories for the exact project.

The verification run applied a six-file tooling patch over the frozen source archive. It changed the contract register and its Python producer and test, together with the verifier Dockerfile and its ignore file. Production Rust sources, Cargo manifests and lockfile, the production Dockerfile, React, and credit files matched the package inputs. The verifier results cover the workspace commands above. Formal workflow acceptance and comparative performance retain separate receipts.

[Machine-readable receipt](../scripts/rust-integration/m0-0f79dc28-compose-workspace.json)

Original Dispatcharr and dependency authors and contributors retain full credit.
